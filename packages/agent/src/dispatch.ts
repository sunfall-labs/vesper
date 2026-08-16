import { Effect, Option, Schema, Stream } from 'effect';
import { AiError, type Tool, type Toolkit } from 'effect/unstable/ai';

import type { Interception } from './interception.js';
import type { AgentLog } from './log.js';

// The tool-dispatch seam: consult the log before running a tool.
//
// ## The gap this closes
//
// The deleted `@sunfall/vesper-durable` wrapped two functions — `generateText` and
// `streamText` — so a checkpoint covered the provider call and nothing after
// it. Tool execution happens inside `LanguageModel`'s resolution of the turn,
// past where every checkpoint sat. A run that died after tool A and before
// tool B therefore recovered by replaying the model call for free and
// **re-running tool A**, with its side effects, a second time. For a while
// the log did not prevent it either: it had the record — one `ToolOutcome`
// per settled call — and nothing read it back.
//
// ## Why this needs no change to the `LanguageModel` contract
//
// It looked like it would. Tool dispatch is buried in `LanguageModel`'s
// internals: `handleToolCall` calls `toolkit.handle(name, params, id)` on the
// resolved toolkit and there is no hook anywhere near it.
//
// But the toolkit is not a fixed value the loop hands over — the option is
// typed `ToolkitInput<Tools>`, which admits `Toolkit.WithHandler<Tools>` *or*
// an `Effect` producing one, and `LanguageModel` resolves it with
// `resolveToolkit` before use. A `Toolkit` is itself that effect. So the seam
// is a toolkit whose `handle` checks the log first and delegates otherwise —
// a value passed through a documented public option, not a patch, not a
// subclass, and not a fork of the turn loop. Every rule `LanguageModel`
// enforces around dispatch (approvals, concurrency, deferring `finish` until
// handlers settle) is untouched, because none of them are downstream of this.
//
// ## What it will and will not serve
//
// Only outcomes belonging to a run that started and never settled, which is
// {@link AgentLog.Session.settled}'s whole job — a conversation whose last
// run ended has an empty index and dispatch behaves exactly as it did before.
// Within that, a match is on tool name plus provider-assigned call id. Ids
// are unique within a conversation and random per call in practice, so the
// hazard this leaves is narrow: a provider that reissued a *different* call
// under an id a crashed run had already used would be served the old answer.
// Matching on parameters as well would not close it — the log records the
// decoded parameters and dispatch is handed the encoded ones — and it is the
// wrong shape of fix. Making the id genuinely unique is the provider's job,
// and the alternative to trusting it is re-running side effects.
//
// ## Two things with an opinion, and which one wins
//
// This is also where an interceptor's `beforeToolCall` sits, so two mechanisms
// can have a view of the same call. The order is fixed and it is not a
// toss-up:
//
//   1. the recovery index, if the session has an unsettled outcome for this
//      call — the tool is not dispatched and the interceptor is not consulted;
//   2. otherwise the interceptor, which may answer in the tool's place;
//   3. otherwise the tool.
//
// Recovery wins because a recovered call **already ran**. Its side effects
// happened in the run that crashed, and an approval gate that refused it now
// would show the model a refusal for work that was actually done — the one
// answer that is false rather than merely unhelpful. The interceptor also
// already had its say: it ran in the earlier run and let the call through,
// which is why there is an outcome to serve at all. Consulting it again would
// make recovery depend on whether a policy changed since the crash, so
// replaying a conversation would stop being a function of the conversation.
//
// The consequence worth stating plainly: **an interceptor cannot revoke
// permission for a tool call a crashed run already completed.** The way to
// stop that call from being served is to settle the run — a `RunSettled`
// record empties the index — not to refuse it here.
//
// The reverse direction has no such asymmetry. A call the interceptor answers
// is recorded as an ordinary `ToolOutcome`, so if *this* run then crashes, the
// substituted answer is what a later run recovers. That is the same rule
// applied consistently: what the log says happened is what happened.

/**
 * What may have an opinion about a call, besides the tool itself.
 *
 * One options object rather than two positional arguments, because the two
 * are independent — an agent may be intercepted without recording, and
 * recording without being intercepted — and because a boolean-blind
 * `gate(toolkit, undefined, interceptor)` at the one call site would be worse
 * than either.
 */
export interface GateOptions {
  /** The run's log claim, when it is recording. */
  readonly session?: AgentLog.Session | undefined;
  /** The agent's interceptor, when it has one. */
  readonly interceptor?: Interception.Interceptor | undefined;
  /** The agent's name, for {@link Interception.ToolCallContext}. */
  readonly agent: string;
}

/**
 * Wrap a toolkit so settled calls are served from the log instead of re-run,
 * and the rest are offered to an interceptor before they run.
 *
 * Returns the `Effect` form of a toolkit, which is what a plain `Toolkit`
 * already is, so it drops into `streamText({ toolkit })` unchanged. Neither
 * option is required: an agent that neither records nor intercepts never
 * reaches this function, because the loop passes its toolkit through
 * untouched.
 */
export const gate = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  options: GateOptions,
): Effect.Effect<Toolkit.WithHandler<Tools>, never, Tool.HandlersFor<Tools>> =>
  Effect.gen(function* () {
    const session = options.session;
    const interceptor = options.interceptor;
    const resolved = yield* toolkit;

    // Captured the way `Subagent.delegateTo` captures its children's
    // services, and for the same reason: decoding a stored result may need
    // the tool's own decoding services, and `handle`'s signature fixes what
    // its stream is allowed to require. Providing the ambient context inward
    // keeps the requirement off the signature instead of casting it away.
    const services = yield* Effect.context<never>();
    const decoders = new Map<string, Decode>();

    const decoderFor = (name: string): Decode => {
      const cached = decoders.get(name);
      if (cached !== undefined) return cached;

      const tool = Object.hasOwn(resolved.tools, name)
        ? resolved.tools[name]
        : undefined;

      // Mirrors `Toolkit`'s own `resultSchema`: with `failureMode: 'return'`
      // a failure comes back as a value, so the recorded result may be a
      // success, a declared failure, or an `AiError`.
      const schema =
        tool === undefined
          ? undefined
          : tool.failureMode === 'return'
            ? Schema.Union([
                tool.successSchema,
                tool.failureSchema,
                AiError.AiError,
              ])
            : tool.successSchema;

      const decode: Decode =
        schema === undefined
          ? (stored: unknown) => Effect.succeed(stored)
          : (stored: unknown) =>
              // The requirement channel is erased rather than declared. A
              // tool's decoding services are already in the agent's
              // `WithOwnHandlers`, so the caller has provided them and they
              // are in `services` — but `handle`'s signature fixes what its
              // effect may require, and declaring them here would put a
              // requirement on a value `LanguageModel` resolves internally.
              // Capturing and providing them is the same move
              // `Subagent.delegateTo` makes for a child's services.
              Schema.decodeUnknownEffect(schema)(stored).pipe(
                Effect.provide(services),
                // A stored result that no longer decodes is a tool whose
                // schema changed since the crash. Serving the encoded value
                // is what the model would have been shown anyway — it is
                // `encodedResult` that reaches the prompt — so the fallback
                // keeps recovery working rather than turning a schema edit
                // into an unrecoverable conversation.
                Effect.catchCause(() => Effect.succeed(stored)),
              ) as Effect.Effect<unknown>;

      decoders.set(name, decode);
      return decode;
    };

    // Loosely typed on purpose: nothing inside can honour the per-tool
    // relationship between a name and its result type, because a stored
    // result arrives as `unknown` from a `Schema.Unknown` column. The one
    // cast is at the end, and what it asserts — that this handles the same
    // names the wrapped toolkit does — is guaranteed by `tools` being passed
    // straight through.
    const underlying = resolved.handle as unknown as Dispatch;

    /**
     * A result nobody's handler produced, in the two forms a part carries.
     *
     * Shared by the two paths that answer without dispatching, because they
     * differ only in where the encoded value came from. The decoded half goes
     * back through the tool's own codec so a consumer of the live stream reads
     * what its type says; the encoded half is served as-is, because that is
     * the field `Prompt` builds the tool-result message from.
     */
    const answered = (
      name: string,
      encoded: unknown,
      isFailure: boolean,
    ): Effect.Effect<Stream.Stream<Tool.HandlerResult<Tool.Any>>> =>
      Effect.map(decoderFor(name)(encoded), (decoded) =>
        Stream.make({
          result: decoded,
          encodedResult: encoded,
          isFailure,
          preliminary: false,
        }),
      );

    const handle: Dispatch = (name, params, toolCallId) =>
      Effect.gen(function* () {
        const prior =
          toolCallId === undefined || session === undefined
            ? Option.none<AgentLog.Settled>()
            : session.settled(name, toolCallId);

        // Step 1. A call an unsettled earlier run already completed is served
        // from the log and goes no further — not to the interceptor, and not
        // to the tool. See the ordering note above.
        if (Option.isSome(prior)) {
          return yield* answered(
            name,
            prior.value.result,
            prior.value.outcome === 'failure',
          );
        }

        // Step 2. The interceptor, which may answer in the tool's place.
        //
        // Its requirement channel is erased in the same way and for the same
        // reason as the decoders': `handle`'s signature fixes what its effect
        // may require, so the services the seam needs are provided from the
        // context captured above rather than declared here. `intercepting` is
        // what makes them present — it puts the interceptor's `R` on the
        // agent's public requirement channel, so a caller who did not provide
        // them did not compile.
        if (interceptor?.beforeToolCall !== undefined) {
          const decision = yield* interceptor
            .beforeToolCall({
              agent: options.agent,
              conversationId: session?.conversationId,
              name,
              toolCallId,
              params,
            })
            .pipe(Effect.provide(services));

          if (decision._tag === 'Answer') {
            return yield* answered(name, decision.result, decision.isFailure);
          }
        }

        // Step 3. The tool.
        return yield* underlying(name, params, toolCallId);
      });

    return {
      tools: resolved.tools,
      handle: handle as unknown as Toolkit.WithHandler<Tools>['handle'],
    };
  });

/** A decoder for one tool's stored result. Never fails; falls back. */
type Decode = (stored: unknown) => Effect.Effect<unknown>;

/** `Toolkit.WithHandler['handle']` with the per-tool types erased. */
type Dispatch = (
  name: string,
  params: unknown,
  toolCallId?: string,
) => Effect.Effect<
  Stream.Stream<Tool.HandlerResult<Tool.Any>, unknown, unknown>,
  AiError.AiError
>;

export * as ToolDispatch from './dispatch.js';
