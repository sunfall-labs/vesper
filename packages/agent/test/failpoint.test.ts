import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect } from 'effect';
import * as FileSystem from 'effect/FileSystem';

import { locations } from '../src/internal/failpoint.js';

// A static assertion that `Failpoint.locations` and the `Failpoint.hit(...)`
// call sites instrumented across the package cannot drift apart: every named
// location must be reachable from at least one real call site, and every
// call site's string literal must name a location the union actually
// declares — a typo'd location string would otherwise silently never fire in
// `Chaos.converge` without failing anything.

const INSTRUMENTED_FILES = [
  '../src/dispatch.ts',
  '../src/internal/session-open.ts',
  '../src/recording-sink.ts',
  '../src/conversation.ts',
] as const;

// Most call sites pass a literal directly: `Failpoint.hit('tool:before-started')`.
// `recording-sink.ts` instead looks the location up from a small table keyed
// by record tag (`ToolOutcome: 'tool:before-outcome'`) and calls
// `Failpoint.hit(variable)`, so a location string can appear either as a
// `Failpoint.hit(...)` argument or as a table value feeding one. Matching the
// location-shaped literal itself, rather than only the `hit(...)` call
// syntax, catches both — and a typo in either place still fails this check,
// since it would produce a string this pattern finds but `locations` does not
// declare, or vice versa.
const LOCATION_LITERAL_PATTERN = /'([a-z]+(?:-[a-z]+)*:[a-z]+(?:-[a-z]+)*)'/g;

const locationLiteralsIn = (source: string): ReadonlyArray<string> =>
  Array.from(
    source.matchAll(LOCATION_LITERAL_PATTERN),
    (match) =>
      // oxlint's type-aware check narrows this capture group to always-defined
      // (the group is mandatory in the pattern above), but plain `tsc` under
      // this repo's `noUncheckedIndexedAccess` does not share that narrowing
      // for a `matchAll` result — the fallback is dead in practice, not unsafe.
      // oxlint-disable-next-line typescript/no-unnecessary-condition -- see above
      match[1] ?? '',
  );

describe('Failpoint.locations and call-site drift', () => {
  it.effect(
    'every named location has at least one Failpoint.hit call site, and every call site names a declared location',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const callSites = new Set<string>();
        for (const relative of INSTRUMENTED_FILES) {
          const path = new URL(relative, import.meta.url).pathname;
          const source = yield* fs.readFileString(path);
          for (const location of locationLiteralsIn(source)) {
            callSites.add(location);
          }
        }

        const declared = new Set<string>(locations);

        const undeclared = Array.from(callSites).filter(
          (location) => !declared.has(location),
        );
        expect(undeclared).toEqual([]);

        const uncalled = locations.filter(
          (location) => !callSites.has(location),
        );
        expect(uncalled).toEqual([]);
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});
