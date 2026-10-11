import * as NodeServices from '@effect/platform-node/NodeServices';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { Effect, Layer } from 'effect';
import { Reactivity as ReactivityModule } from 'effect/reactivity';
import { SqlClient } from 'effect/sql';

import { chainTamperContract } from '../../log/test/chain-tamper-contract.js';
import { logStoreContract } from '../../log/test/log-store-contract.js';
import { LogStoreSqlite } from '../src/layer.js';
import { SqliteNative } from '../src/layer-native.js';

// A fresh in-memory database, migrated, per build. The shared contract runs
// against it so SQLite is held to the same behaviour as memory and Postgres.
const backing = Layer.mergeAll(
  SqliteNative.layer(':memory:'),
  NodeServices.layer,
  ReactivityModule.layer,
);

const migratedLayer = Layer.effect(
  LogStore.Service,
  Effect.gen(function* () {
    yield* LogStoreSqlite.migrate();
    return yield* LogStoreSqlite.make(yield* SqlClient.SqlClient);
  }),
).pipe(Layer.provide(backing));

logStoreContract('sqlite', { layer: migratedLayer });

const execute = (statement: string) =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql.unsafe(statement).unprepared,
  );

chainTamperContract('sqlite', {
  layer: Layer.effectDiscard(LogStoreSqlite.migrate()).pipe(
    Layer.provideMerge(LogStoreSqlite.layer()),
    Layer.provideMerge(backing),
  ),
  editText: (path, seq) =>
    execute(
      `UPDATE vesper_log_records SET record = json_set(record, '$.text', 'x')
       WHERE path = '${path}' AND record_offset = '${LogOffset.fromSeq(BigInt(seq))}'`,
    ).pipe(Effect.asVoid),
  deleteRecord: (path, seq) =>
    execute(
      `DELETE FROM vesper_log_records WHERE path = '${path}' AND seq = '${String(seq)}'`,
    ).pipe(Effect.asVoid),
});
