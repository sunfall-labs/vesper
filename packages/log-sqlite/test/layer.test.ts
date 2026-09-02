import { describe, expect, it } from '@effect/vitest';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import * as ReactivityModule from 'effect/unstable/reactivity/Reactivity';
import { SqlClient } from 'effect/unstable/sql';

import { LogStoreSqlite } from '../src/layer.js';
import { SqliteNative } from '../src/layer-native.js';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogStoreConformance } from '@sunfall/vesper-log/testing';

const infrastructure = Layer.mergeAll(
  SqliteNative.layer(':memory:'),
  NodeServices.layer,
  ReactivityModule.layer,
);

// The shared contract needs a plain `Layer<LogStore.Service, E>`, but this
// backend's `LogStore.Service` is only usable once its tables exist. Folding
// `migrate()` into the layer itself — rather than calling it inside each
// case, which the contract does not know to do — gives every generated
// `it.effect` a fresh, already-migrated `:memory:` database.
const storeLayer = Layer.effect(
  LogStore.Service,
  Effect.gen(function* () {
    yield* LogStoreSqlite.migrate();
    const client = yield* SqlClient.SqlClient;
    return yield* LogStoreSqlite.make(client);
  }),
).pipe(Layer.provide(infrastructure));

// A single-file `:memory:` connection cannot stand up two independent
// connections to the same database, so this backend skips any case that
// would need one.
LogStoreConformance.register('sqlite', storeLayer, { concurrent: false });

describe('LogStore SQLite backend', () => {
  it.effect('supports positional value rows through the native client', () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe('CREATE TABLE values_test (value TEXT)').unprepared;
      yield* sql.unsafe('INSERT INTO values_test VALUES (?)', ['ok'])
        .unprepared;
      const rows = yield* sql.unsafe('SELECT value FROM values_test').values;
      expect(rows).toEqual([['ok']]);
    }).pipe(Effect.provide(SqliteNative.layer(':memory:'))),
  );
});
