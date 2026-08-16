import { LogStoreMemory } from '../src/layer-memory.js';
import { LogStoreContract } from '../src/log-store-contract.js';

// The memory backend can fake a dead notification channel, so it runs the
// whole suite including the change-feed failure case. A backend that cannot
// skips that one and the suite says so in the test name.
LogStoreContract.logStoreContract('memory', {
  layer: LogStoreMemory.layer,
  layerWithFailingChanges: LogStoreMemory.layerFailingChanges,
});
