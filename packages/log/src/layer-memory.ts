import type { Crypto, Layer } from 'effect';

import { build } from './internal/memory-store.js';
import type { LogStore } from './log-store.js';

/** In-process LogStore Adapter with fresh storage for every layer build. */
export const layer: Layer.Layer<LogStore.Service, never, Crypto.Crypto> =
  build(undefined);

export * as LogStoreMemory from './layer-memory.js';
