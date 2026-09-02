import { Context, Effect, Layer, Ref, Schema } from 'effect';

// A crash-injection seam for durable boundaries.
//
// `hit(location)` is called at every place the recording and recovery
// machinery treats as a durability boundary — immediately before or after an
// append that changes what a resumed run will see. In production nothing is
// listening, so every call is a `Context.Reference` read that falls through
// to its default: no allocation worth naming, no branch a caller has to
// reason about, and no service any caller has to provide. `test/chaos.test.ts`
// is the one place that actually arms a location, by providing
// {@link layerTest} around a scripted conversation and asserting that
// reopening the conversation after the simulated crash converges on the same
// result a crash-free run would have produced.
//
// ## Why a `Context.Reference` and not a plain service
//
// The same argument `ContextWindow.Service` makes (`context-window.ts`): the
// safe default is real, so this is not `LogStore` — nothing here hides
// persistence behind a defaulted reference; it hides nothing, because the
// default genuinely does nothing. A `Reference`'s identifier is `never`, so
// reading one adds nothing to `R`, and every call site across `dispatch.ts`,
// `internal/session-open.ts`, `recording-sink.ts`, and `conversation.ts`
// stays exactly as typed as it already was — that is what "callers never see
// the requirement" means concretely.
//
// Deliberately NOT also wired with an explicit
// `Effect.provide(_, Failpoint.layerNoop)` anywhere inside the agent's own
// construction. An inner `Effect.provide` for a tag shadows whatever an outer
// caller already provided for that same tag, and a chaos test needs its
// `layerTest` — provided around the conversation it is driving — to reach
// every one of these call sites undisturbed. The reference's own default
// already achieves "callers never see the requirement" with nothing to wire;
// a redundant internal provide would only dress that up as an explicit step
// while quietly breaking the one thing that has to keep working: a test's
// ability to override it from outside.
//
// ## Why a defect and not a typed failure
//
// `hit` returns `Effect<void>` — no error channel — so instrumenting a call
// site never changes its type. A real process crash does not unwind through
// typed error handling; it stops the process, full stop, with whatever
// finalizers the runtime itself still manages to run before that happens.
// `Effect.die` is the honest analogue: it terminates the fiber as a defect,
// which still runs `Stream.onExit`/`Effect.ensuring` finalizers exactly as an
// interruption or a thrown defect would in production, but crucially does so
// without adding `FailpointCrash` to any function's typed error union.
// That is what lets `recording-sink.ts`'s `settle` correctly leave an
// orphaned run behind for a crash inside the `ToolStarted`/`ToolOutcome`
// window — it already inspects `session.pendingToolState` for exactly this
// shape, and a defect exercises that path the same way a genuine crash would.

/**
 * One durable boundary a crash can land on. Closed by design —
 * `test/failpoint.test.ts` fails if this list and the call sites
 * instrumented with {@link hit} across the package ever disagree.
 */
export const locations = [
  'claim:after-acquire',
  'tool:before-started',
  'tool:after-started',
  'tool:before-outcome',
  'tool:after-outcome',
  'approval:after-suspended',
  'approval:after-resolved',
  'turn:before-finished',
  'turn:after-finished',
  'run:before-completed',
  'compaction:before-append',
  'compaction:after-append',
  'signal:after-received',
] as const;

export type Location = (typeof locations)[number];

/**
 * Raised by an armed {@link layerTest} handler; always surfaced as a defect
 * (see the module doc for why `hit` converts it rather than propagating it
 * typed). `Schema.TaggedError`, not `Data.TaggedError` — never crosses the
 * log, so `contributing.md`'s rule permitted either, but `Data.TaggedError`'s
 * generic base class currently leaks an `any` into its published
 * declaration, which `scripts/check-published-types.mjs` correctly refuses.
 */
export class FailpointCrash extends Schema.TaggedError<FailpointCrash>(
  '@sunfall/vesper-agent/internal/FailpointCrash',
)('FailpointCrash', { location: Schema.Literals([...locations]) }) {}

/** What `hit` reads. Internal shape — production code calls `hit`, never this. */
export interface Service {
  readonly hit: (location: Location) => Effect.Effect<void>;
}

const passthroughService: Service = { hit: () => Effect.void };

const FailpointRef = Context.Reference<Service>(
  '@sunfall/vesper-agent/internal/Failpoint',
  { defaultValue: () => passthroughService },
);

/** Call at a named durable boundary. A no-op unless a test has armed it. */
export const hit = (location: Location): Effect.Effect<void> =>
  Effect.flatMap(FailpointRef, (service) => service.hit(location));

/**
 * The production default, explicit for symmetry with {@link layerTest} and
 * for a caller that wants to say plainly "no chaos hooks here" rather than
 * lean on the reference's own fallback. Equivalent to providing nothing —
 * see the module doc for why nothing inside this package does that for you.
 */
export const layerNoop: Layer.Layer<never> = Layer.succeed(
  FailpointRef,
  passthroughService,
);

/** Decides what happens when a named location is hit; typed so `crashAt` and a
 * chaos runner's own handlers compose without an unsafe cast. */
export type Handler = (
  location: Location,
) => Effect.Effect<void, FailpointCrash>;

/** Never crashes. The disarmed state a chaos runner rearms a handler `Ref` to
 * before its recovery run. */
export const passthrough: Handler = () => Effect.void;

/** Crash the first (and every) time `target` is hit; pass through every other
 * location. A chaos runner disarms by swapping the `Ref` back to {@link passthrough}. */
export const crashAt =
  (target: Location): Handler =>
  (location) =>
    location === target
      ? Effect.fail(new FailpointCrash({ location }))
      : Effect.void;

/**
 * Arm chaos testing for the scope this layer is provided to, backed by a
 * `Ref` the caller owns so the same layer can be provided once around a
 * crashing run and its disarmed recovery continuation, rearming the `Ref` in
 * between rather than rebuilding the layer.
 *
 * A handler's typed `FailpointCrash` is a configuration convenience — this
 * converts it to a defect via `Effect.orDie` before it ever reaches a caller
 * of `hit`, so the public contract stays `Effect<void>` everywhere, matching
 * production.
 */
export const layerTest = (handlerRef: Ref.Ref<Handler>): Layer.Layer<never> =>
  Layer.effect(
    FailpointRef,
    Effect.sync(
      (): Service => ({
        hit: (location) =>
          Ref.get(handlerRef).pipe(
            Effect.flatMap((handler) => handler(location)),
            Effect.orDie,
          ),
      }),
    ),
  );
