import { Effect, Layer, MutableHashMap, type Crypto } from 'effect';

import { AttachmentStore } from './attachment-store.js';
import { service } from './internal/memory-store.js';

/**
 * In-process attachment storage.
 *
 * Every layer build receives fresh storage. Bytes are copied on both sides of
 * the Interface, addressed by their digest, and verified again on read.
 */
export const layer: Layer.Layer<AttachmentStore.Service, never, Crypto.Crypto> =
  Layer.effect(
    AttachmentStore.Service,
    Effect.suspend(() => service(MutableHashMap.empty<string, Uint8Array>())),
  );

export * as AttachmentStoreMemory from './layer-memory.js';
