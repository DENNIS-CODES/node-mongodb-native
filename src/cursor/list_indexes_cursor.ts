import type { Document } from '../bson';
import type { Collection } from '../collection';
import { MongoRuntimeError } from '../error';
import { executeOperation, ExecutionResult } from '../operations/execute_operation';
import { GetMoreOperation } from '../operations/get_more';
import { ListIndexesOperation, ListIndexesOptions } from '../operations/indexes';
import type { ClientSession } from '../sessions';
import type { Callback } from '../utils';
import { AbstractCursor } from './abstract_cursor';

/** @public */
export class ListIndexesCursor extends AbstractCursor {
  parent: Collection;
  options?: ListIndexesOptions;

  constructor(collection: Collection, options?: ListIndexesOptions) {
    super(collection.s.db.s.client, collection.s.namespace, options);
    this.parent = collection;
    this.options = options;
  }

  clone(): ListIndexesCursor {
    return new ListIndexesCursor(this.parent, {
      ...this.options,
      ...this.cursorOptions
    });
  }

  /** @internal */
  _initialize(session: ClientSession | undefined, callback: Callback<ExecutionResult>): void {
    const operation = new ListIndexesOperation(this.parent, {
      ...this.cursorOptions,
      ...this.options,
      session
    });

    executeOperation(this.parent.s.db.s.client, operation, (err, response) => {
      if (err || response == null) return callback(err);

      // TODO: NODE-2882
      callback(undefined, { server: operation.server, session, response });
    });
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
      callback
    );
  }
}
