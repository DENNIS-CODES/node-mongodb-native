import type { Document, Long, Timestamp } from '../bson';
import {
  type ChangeStreamDocument,
  type ChangeStreamEvents,
  type OperationTime,
  type ResumeToken,
  ChangeStream
} from '../change_stream';
import { INIT, RESPONSE } from '../constants';
import { MongoRuntimeError } from '../error';
import type { MongoClient } from '../mongo_client';
import type { TODO_NODE_3286 } from '../mongo_types';
import { AggregateOperation } from '../operations/aggregate';
import type { CollationOptions } from '../operations/command';
import { type ExecutionResult, executeOperation } from '../operations/execute_operation';
import { GetMoreOperation } from '../operations/get_more';
import type { ClientSession } from '../sessions';
import { type Callback, type MongoDBNamespace, maxWireVersion } from '../utils';
import { type AbstractCursorOptions, AbstractCursor } from './abstract_cursor';

/** @internal */
export interface ChangeStreamCursorOptions extends AbstractCursorOptions {
  startAtOperationTime?: OperationTime;
  resumeAfter?: ResumeToken;
  startAfter?: ResumeToken;
  maxAwaitTimeMS?: number;
  collation?: CollationOptions;
  fullDocument?: string;
}

/** @internal */
export type ChangeStreamAggregateRawResult<TChange> = {
  $clusterTime: { clusterTime: Timestamp };
  cursor: {
    postBatchResumeToken: ResumeToken;
    ns: string;
    id: number | Long;
  } & ({ firstBatch: TChange[] } | { nextBatch: TChange[] });
  ok: 1;
  operationTime: Timestamp;
};

/** @internal */
export class ChangeStreamCursor<
  TSchema extends Document = Document,
  TChange extends Document = ChangeStreamDocument<TSchema>
> extends AbstractCursor<TChange, ChangeStreamEvents> {
  _resumeToken: ResumeToken;
  startAtOperationTime?: OperationTime;
  hasReceived?: boolean;
  resumeAfter: ResumeToken;
  startAfter: ResumeToken;
  options: ChangeStreamCursorOptions;

  postBatchResumeToken?: ResumeToken;
  pipeline: Document[];

  /**
   * @internal
   *
   * used to determine change stream resumability
   */
  maxWireVersion: number | undefined;

  constructor(
    client: MongoClient,
    namespace: MongoDBNamespace,
    pipeline: Document[] = [],
    options: ChangeStreamCursorOptions = {}
  ) {
    super(client, namespace, options);

    this.pipeline = pipeline;
    this.options = options;
    this._resumeToken = null;
    this.startAtOperationTime = options.startAtOperationTime;

    if (options.startAfter) {
      this.resumeToken = options.startAfter;
    } else if (options.resumeAfter) {
      this.resumeToken = options.resumeAfter;
    }
  }

  set resumeToken(token: ResumeToken) {
    this._resumeToken = token;
    this.emit(ChangeStream.RESUME_TOKEN_CHANGED, token);
  }

  get resumeToken(): ResumeToken {
    return this._resumeToken;
  }

  get resumeOptions(): ChangeStreamCursorOptions {
    const options: ChangeStreamCursorOptions = {
      ...this.options
    };

    for (const key of ['resumeAfter', 'startAfter', 'startAtOperationTime'] as const) {
      delete options[key];
    }

    if (this.resumeToken != null) {
      if (this.options.startAfter && !this.hasReceived) {
        options.startAfter = this.resumeToken;
      } else {
        options.resumeAfter = this.resumeToken;
      }
    } else if (this.startAtOperationTime != null && maxWireVersion(this.server) >= 7) {
      options.startAtOperationTime = this.startAtOperationTime;
    }

    return options;
  }

  cacheResumeToken(resumeToken: ResumeToken): void {
    if (this.bufferedCount() === 0 && this.postBatchResumeToken) {
      this.resumeToken = this.postBatchResumeToken;
    } else {
      this.resumeToken = resumeToken;
    }
    this.hasReceived = true;
  }

  _processBatch(response: ChangeStreamAggregateRawResult<TChange>): void {
    const cursor = response.cursor;
    if (cursor.postBatchResumeToken) {
      this.postBatchResumeToken = response.cursor.postBatchResumeToken;

      const batch =
        'firstBatch' in response.cursor ? response.cursor.firstBatch : response.cursor.nextBatch;
      if (batch.length === 0) {
        this.resumeToken = cursor.postBatchResumeToken;
      }
    }
  }

  clone(): AbstractCursor<TChange> {
    return new ChangeStreamCursor(this.client, this.namespace, this.pipeline, this.cursorOptions);
  }

  _initialize(session: ClientSession, callback: Callback<ExecutionResult>): void {
    const aggregateOperation = new AggregateOperation(this.namespace, this.pipeline, {
      ...this.cursorOptions,
      ...this.options,
      session
    });

    executeOperation<TODO_NODE_3286, ChangeStreamAggregateRawResult<TChange>>(
      session.client,
      aggregateOperation,
      (err, response) => {
        if (err || response == null) {
          return callback(err);
        }

        const server = aggregateOperation.server;
        this.maxWireVersion = maxWireVersion(server);

        if (
          this.startAtOperationTime == null &&
          this.resumeAfter == null &&
          this.startAfter == null &&
          this.maxWireVersion >= 7
        ) {
          this.startAtOperationTime = response.operationTime;
        }

        this._processBatch(response);

        this.emit(INIT, response);
        this.emit(RESPONSE);

        // TODO: NODE-2882
        callback(undefined, { server, session, response });
      }
    );
  }

  _getMore(callback: Callback<Document>) {
    const cursorId = this.id;
    const cursorNs = this.namespace;
    const server = this.server;

    if (cursorId == null) {
      return callback(new MongoRuntimeError('Unable to iterate cursor with no id'));
    }

    if (server == null) {
      return callback(new MongoRuntimeError('Unable to iterate cursor without selected server'));
    }

    executeOperation(
      this.client,
      new GetMoreOperation(cursorNs, cursorId, server, this.cursorOptions),
      (error, response) => {
        if (error) {
          return callback(error);
        }

        this.maxWireVersion = maxWireVersion(this.server);
        this._processBatch(response as ChangeStreamAggregateRawResult<TChange>);

        this.emit(ChangeStream.MORE, response);
        this.emit(ChangeStream.RESPONSE);
        callback(error, response);
      }
    );
  }
}
