import { expect } from 'chai';

import {
  ReadPreference,
  ReadPreferenceMode,
  ReadPreferenceOptions
} from '../../../src/read_preference';
import { ServerType, TopologyType } from '../../../src/sdam/common';
import { ServerDescription, TagSet } from '../../../src/sdam/server_description';
import * as ServerSelectors from '../../../src/sdam/server_selection';
import { TopologyDescription } from '../../../src/sdam/topology_description';
import { serverDescriptionFromDefinition } from './server_selection_spec_helper';

interface ServerSelectionLogicTestServer {
  address: string;
  avg_rtt_ms: number;
  type: ServerType;
  tags?: TagSet;
}
interface Test {
  topology_description: {
    type: TopologyType;
    servers: ServerSelectionLogicTestServer[];
  };
  operation: 'read' | 'write';
  read_preference: {
    mode: ReadPreferenceMode;
    tag_sets?: TagSet[];
  };
  /**
   * The spec says we should confirm the list of suitable servers in addition to the list of
   * servers in the latency window, if possible.  We apply the latency window inside the
   * selector so for Node this is not possible.
   * https://github.com/mongodb/specifications/tree/master/source/server-selection/tests#server-selection-logic-tests
   */
  suitable_servers: never;
  in_latency_window: ServerSelectionLogicTestServer[];
}

function readPreferenceFromDefinition(definition) {
  const mode = definition.mode
    ? definition.mode.charAt(0).toLowerCase() + definition.mode.slice(1)
    : 'primary';

  const options: ReadPreferenceOptions = {};
  if (typeof definition.maxStalenessSeconds !== 'undefined')
    options.maxStalenessSeconds = definition.maxStalenessSeconds;
  const tags = definition.tag_sets || [];

  return new ReadPreference(mode, tags, options);
}

/**
 * Compares two server descriptions and compares all fields that are present
 * in the yaml spec tests.
 */
function compareServerDescriptions(s1: ServerDescription, s2: ServerDescription) {
  expect(s1.address).to.equal(s2.address);
  expect(s1.roundTripTime).to.equal(s2.roundTripTime);
  expect(s1.type).to.equal(s2.type);
  expect(s1.tags).to.deep.equal(s2.tags);
}

function serverDescriptionsToMap(
  descriptions: ServerDescription[]
): Map<string, ServerDescription> {
  const descriptionMap = new Map<string, ServerDescription>();

  for (const description of descriptions) {
    descriptionMap.set(description.address, description);
  }

  return descriptionMap;
}

/**
 * Executes a server selection logic test
 * @see https://github.com/mongodb/specifications/tree/master/source/server-selection/tests#server-selection-logic-tests
 */
export function runServerSelectionLogicTest(testDefinition: Test) {
  const allHosts = testDefinition.topology_description.servers.map(({ address }) => address);
  const serversInTopology = testDefinition.topology_description.servers.map(s =>
    serverDescriptionFromDefinition(s, allHosts)
  );
  const serverDescriptions = serverDescriptionsToMap(serversInTopology);
  const topologyDescription = new TopologyDescription(
    testDefinition.topology_description.type,
    serverDescriptions
  );
  const expectedServers = serverDescriptionsToMap(
    testDefinition.in_latency_window.map(s => serverDescriptionFromDefinition(s))
  );

  let selector;
  if (testDefinition.operation === 'write') {
    selector = ServerSelectors.writableServerSelector();
  } else if (testDefinition.operation === 'read' || testDefinition.read_preference) {
    try {
      const readPreference = readPreferenceFromDefinition(testDefinition.read_preference);
      selector = ServerSelectors.readPreferenceServerSelector(readPreference);
    } catch (e) {
      expect(e, ejson`Invalid readPreference: ${testDefinition.read_preference}`).not.to.exist;
    }
  }

  const result = selector(topologyDescription, serversInTopology);

  expect(result.length).to.equal(expectedServers.size);

  for (const server of result) {
    const expectedServer = expectedServers.get(server.address);
    expect(expectedServer).to.exist;
    compareServerDescriptions(server, expectedServer);
    expectedServers.delete(server.address);
  }

  expect(expectedServers.size).to.equal(0);
}