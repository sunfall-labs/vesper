import { anthropicProvider } from '@earendil-works/pi-ai/providers/anthropic';
// Subpath imports, not the barrel: the package root re-exports `NodeRedis`,
// which imports `ioredis` at module load and is not installed here.
import * as NodeRuntime from '@effect/platform-node/NodeRuntime';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Agent } from '@sunfall/vesper-agent/agent';
import { AgentEvents } from '@sunfall/vesper-agent/event';
import { PiProvider } from '@sunfall/vesper-pi/provider';
import { PiRegistry } from '@sunfall/vesper-pi/registry';
import {
  Config,
  Console,
  Context,
  Effect,
  Layer,
  Ref,
  Stdio,
  Stream,
} from 'effect';
import { AiError, LanguageModel, type Tool, Toolkit } from 'effect/unstable/ai';
import { Argument, Command, Flag } from 'effect/unstable/cli';

import { AiRuntime } from '@sunfall/vesper-runtime/runtime';

// A speaker answers; a judge rewrites every sentence before anyone sees it.
//
// The speaker's stream is never connected to the terminal. Sentences go to the
// judge, and the judge's stream is the output — so unreviewed text has no path
// out, rather than being caught on the way. That is the difference between a
// sanitizer and a reporter, and it is worth more than blocking: a violation
// becomes a compliant sentence instead of a halted conversation.
//
// The cost is that every sentence round-trips the judge, compliant ones
// included, because "unchanged" is a rewrite that happens to match.
//
// It is one of the two things in this repository that talk to a real model —
// `examples/live-smoke` is the other — and every test runs against Pi's faux
// provider. Its first run caught the context-overflow classifier missing
// Anthropic's own phrasing, so it is worth re-running after any change to
// `@sunfall/vesper-pi`.

const POLICY = [
  'Never give medical, legal, or financial advice.',
  'Never promise a refund, a delivery date, or compensation.',
  'Never state personal data about anyone.',
].join('\n');

const speaker = Agent.make({
  name: 'speaker',
  instructions:
    'You are a customer support assistant. Answer directly and helpfully ' +
    'in four to six short sentences. Use complete sentences.',
  toolkit: Toolkit.make(),
});

// The examples are not decoration. On a small model the bare policy produced
// false positives a sanitizer cannot afford: an apology for a late order was
// rewritten as if it had promised a delivery date. Naming the distinction the
// policy actually turns on — describing an option versus committing on the
// company's behalf — is what a cheap model needs stated and a large one infers.
const judge = Agent.make({
  name: 'judge',
  instructions: [
    'You rewrite ONE sentence so that it complies with a policy.',
    '',
    'POLICY:',
    POLICY,
    '',
    'Rules:',
    '- If the sentence already complies, return it EXACTLY as given.',
    '- If it violates, return a compliant rewrite that keeps as much of the',
    '  original meaning as possible.',
    '- If nothing compliant can be said, return exactly: [removed]',
    '',
    'Return the sentence and nothing else — no preamble, quotes, or notes.',
    'Describing an option is not advice; committing on the company’s behalf is.',
    '',
    'in:  "I am sorry your order is late."',
    'out: "I am sorry your order is late."',
    'in:  "You should dispute the charge with your bank."',
    'out: "Some customers choose to contact their card issuer."',
    'in:  "We will refund you today."',
    'out: "I can raise a refund request for you to review."',
  ].join('\n'),
  toolkit: Toolkit.make(),
});

/**
 * The model the judge runs on.
 *
 * A second tag for the same service type, so a program can hold two models at
 * once: `LanguageModel` is the speaker's, and this is the auditor's. Passing
 * the judge's layer down as a function argument works but reads oddly —
 * wiring is what the context is for, and a tag makes `review` depend on *a
 * judge model existing* rather than on being handed one.
 */
class JudgeModel extends Context.Service<JudgeModel, LanguageModel.Service>()(
  'compliance/JudgeModel',
) {}

/**
 * Bind a model to {@link JudgeModel} by re-tagging it.
 *
 * `Layer.effect(JudgeModel, LanguageModel.LanguageModel)` reads the language
 * model out of context and republishes it under the judge's tag;
 * `Layer.provide` then supplies that input from the judge's own model. The
 * speaker's model, provided further out, is untouched — which is the point of
 * scoping it this way rather than juggling two layers by hand.
 */
const judgeModelLayer = (
  options: AiRuntime.Options,
): Layer.Layer<JudgeModel, never, PiRegistry.Service> =>
  Layer.effect(JudgeModel, LanguageModel.LanguageModel).pipe(
    Layer.provide(AiRuntime.model(options)),
  );

/**
 * Regroup token deltas into whole sentences, emitting each as it completes.
 *
 * Paragraph breaks split too. Splitting on sentence-enders alone glued
 * `Here are some steps you should take:` to the sentence after it, because a
 * colon is not `.!?` — so the judge was ruling on two-sentence chunks.
 */
const bySentence = Stream.mapAccum(
  (): string => '',
  (buffer: string, delta: string) => {
    const pending = buffer + delta;
    const parts = pending.split(/(?<=[.!?])\s+|\n{2,}/);
    // The last fragment is still being written; hold it back.
    const rest = parts.pop() ?? '';
    return [rest, parts.filter((s) => s.trim() !== '')] as const;
  },
);

/** Model text only; turn boundaries and tool events are noise here. */
const deltas = <Tools extends Record<string, Tool.Any>, E, R>(
  self: Stream.Stream<AgentEvents.Event<Tools>, E, R>,
): Stream.Stream<string, E, R> =>
  self.pipe(
    // `Stream.filterMap` in v4 takes a `Filter`, not an `Option` —
    // map-then-filter says the same thing more plainly.
    Stream.map((event) =>
      AgentEvents.isPart(event) && event.part.type === 'text-delta'
        ? event.part.delta
        : '',
    ),
    Stream.filter((delta) => delta !== ''),
  );

/** One sentence, rewritten by the judge. The trailing space rejoins them. */
const sanitize = (
  sentence: string,
): Stream.Stream<string, AiError.AiError, Agent.Requires<typeof judge>> =>
  judge.stream(sentence).pipe(deltas, Stream.concat(Stream.make(' ')));

/** Whitespace-insensitive, since the two paths segment text differently. */
const sameText = (a: string, b: string): boolean =>
  a.replace(/\s+/g, ' ').trim() === b.replace(/\s+/g, ' ').trim();

/** Both models come from context: the speaker's tag, and the judge's. */
const review = (prompt: string) =>
  Effect.gen(function* () {
    const stdio = yield* Stdio.Stdio;
    const judgeModel = yield* JudgeModel;

    // Tapped at the two ends of the pipeline rather than tracked per sentence.
    // The comparison the summary wants is "what was said" against "what was
    // shown", and reading it off the stream twice keeps `sanitize` free of
    // bookkeeping it would otherwise have to thread an accumulator through.
    const said = yield* Ref.make('');
    const shown = yield* Ref.make('');

    yield* Console.log(`\n\x1b[2m? ${prompt}\x1b[0m\n`);

    yield* speaker.stream(prompt).pipe(
      deltas,
      Stream.tap((delta) => Ref.update(said, (text) => text + delta)),
      bySentence,
      // Sequential on purpose. `flatMap` concatenates inner streams in input
      // order at the default concurrency, which is what lets the judge's
      // deltas go straight to the terminal — judging two sentences at once
      // would interleave their text mid-word.
      Stream.flatMap((sentence) =>
        // Swap the model for this call only. The speaker keeps its own.
        sanitize(sentence).pipe(
          Stream.provideService(LanguageModel.LanguageModel, judgeModel),
        ),
      ),
      Stream.tap((delta) => Ref.update(shown, (text) => text + delta)),
      // Into Effect's stdout sink rather than `process.stdout.write`: the
      // program already depends on `Stdio`, and a stream running into a sink
      // is the shape the rest of this pipeline is written in.
      Stream.run(stdio.stdout({ endOnDone: false })),
    );

    const [original, sanitized] = [yield* Ref.get(said), yield* Ref.get(shown)];

    yield* sameText(original, sanitized)
      ? Console.log(`\n\n\x1b[32mclean\x1b[0m — nothing needed rewriting\n`)
      : Console.log(
          `\n\n\x1b[33msanitized\x1b[0m — the speaker had said:\n\n` +
            `\x1b[2m${original.trim()}\x1b[0m\n`,
        );
  });

const command = Command.make(
  'compliance',
  {
    prompt: Argument.string('prompt').pipe(
      Argument.withDescription('What to ask the speaker.'),
    ),
    speakerModel: Flag.string('speaker').pipe(
      Flag.withDescription('Model that answers.'),
      Flag.withDefault('claude-sonnet-4-6'),
    ),
    judgeModel: Flag.string('judge').pipe(
      Flag.withDescription(
        'Model that rules on each sentence. It runs once per sentence, so a ' +
          'small model is the right default.',
      ),
      Flag.withDefault('claude-haiku-4-5'),
    ),
    provider: Flag.string('provider').pipe(
      Flag.withDescription('Pi provider id.'),
      Flag.withDefault('anthropic'),
    ),
  },
  ({ judgeModel, prompt, provider, speakerModel }) =>
    review(prompt).pipe(
      // Each model is scoped where it is used: the judge's is bound to its own
      // tag, the speaker's stays on `LanguageModel`. Neither call site knows
      // the other exists.
      //
      // `retry: false` on both. Retrying is right in production and wrong
      // here: this script exists to show what the provider actually did, and
      // a silently absorbed 429 is a model call the output does not mention.
      Effect.provide(
        judgeModelLayer({ provider, model: judgeModel, retry: false }),
      ),
      Effect.provide(
        AiRuntime.model({ provider, model: speakerModel, retry: false }),
      ),
      // Print the classified reason rather than the raw error: a provider
      // phrasing that `@sunfall/vesper-pi/errors` maps to the wrong type is then
      // visible instead of merely fatal.
      Effect.tapError((error: unknown) =>
        Console.error(
          AiError.isAiError(error)
            ? `\n\x1b[31m${error._tag}\x1b[0m  retryable=${String(error.isRetryable)}` +
                `\n${error.message}\n`
            : `\n\x1b[31m${String(error)}\x1b[0m\n`,
        ),
      ),
    ),
).pipe(
  Command.withDescription(
    'Stream an answer through a compliance judge, which rewrites any ' +
      'sentence that violates the policy before it is shown.',
  ),
);

const infrastructure = PiProvider.layerConfig({
  provider: anthropicProvider(),
  apiKey: Config.redacted('ANTHROPIC_API_KEY'),
});

command.pipe(
  Command.run({ version: '0.1.0' }),
  Effect.provide(Layer.mergeAll(infrastructure, NodeServices.layer)),
  NodeRuntime.runMain,
);
