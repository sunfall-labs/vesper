import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic';
import { OpenAiClient, OpenAiLanguageModel } from '@effect/ai-openai';
import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';
import { Config, Layer } from 'effect';
import { LanguageModel, Model } from 'effect/unstable/ai';
import { HttpClient } from 'effect/unstable/http';

type Needs<L> =
  L extends Layer.Layer<infer _Out, infer _Err, infer R> ? R : never;
type Provides<L> =
  L extends Layer.Layer<infer Out, infer _Err, infer _R> ? Out : never;
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? 'yes'
    : 'no';
type Has<Member, Union> = [Member] extends [Union] ? 'yes' : 'no';

const anthropic = AnthropicLanguageModel.model('claude-sonnet-4-6', {
  max_tokens: 1_024,
});
const openai = OpenAiLanguageModel.model('gpt-5.6-luna', {
  max_output_tokens: 1_024,
});
const anthropicConfigured = anthropic.pipe(
  Layer.provide(
    AnthropicClient.layerConfig({
      apiKey: Config.redacted('ANTHROPIC_API_KEY'),
    }),
  ),
);
const openaiConfigured = openai.pipe(
  Layer.provide(
    OpenAiClient.layerConfig({ apiKey: Config.redacted('OPENAI_API_KEY') }),
  ),
);
const anthropicLive = anthropicConfigured.pipe(
  Layer.provide(NodeHttpClient.layerUndici),
);
const openaiLive = openaiConfigured.pipe(
  Layer.provide(NodeHttpClient.layerUndici),
);

const _anthropicNeedsClient: Equal<
  Needs<typeof anthropic>,
  AnthropicClient.AnthropicClient
> = 'yes';
const _openAiNeedsClient: Equal<
  Needs<typeof openai>,
  OpenAiClient.OpenAiClient
> = 'yes';
const _anthropicNeedsHttp: Equal<
  Needs<typeof anthropicConfigured>,
  HttpClient.HttpClient
> = 'yes';
const _openAiNeedsHttp: Equal<
  Needs<typeof openaiConfigured>,
  HttpClient.HttpClient
> = 'yes';
const _anthropicClosed: Equal<Needs<typeof anthropicLive>, never> = 'yes';
const _openAiClosed: Equal<Needs<typeof openaiLive>, never> = 'yes';

const _anthropicRejectsOpenAiClient: Has<
  OpenAiClient.OpenAiClient,
  Needs<typeof anthropic>
> = 'no';
const _openAiRejectsAnthropicClient: Has<
  AnthropicClient.AnthropicClient,
  Needs<typeof openai>
> = 'no';
const _anthropicProvidesModel: Has<
  LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
  Provides<typeof anthropic>
> = 'yes';
const _openAiProvidesModel: Has<
  LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
  Provides<typeof openai>
> = 'yes';
const _anthropicProvidesProviderName: Has<
  Model.ProviderName,
  Provides<typeof anthropic>
> = 'yes';
const _anthropicProvidesModelName: Has<
  Model.ModelName,
  Provides<typeof anthropic>
> = 'yes';
const _openAiProvidesProviderName: Has<
  Model.ProviderName,
  Provides<typeof openai>
> = 'yes';
const _openAiProvidesModelName: Has<
  Model.ModelName,
  Provides<typeof openai>
> = 'yes';

// @ts-expect-error Anthropic request options use `max_tokens`.
AnthropicLanguageModel.model('claude-sonnet-4-6', { max_output_tokens: 1 });
// @ts-expect-error OpenAI request options use `max_output_tokens`.
OpenAiLanguageModel.model('gpt-5.6-luna', { max_tokens: 1 });

void [
  _anthropicNeedsClient,
  _openAiNeedsClient,
  _anthropicNeedsHttp,
  _openAiNeedsHttp,
  _anthropicClosed,
  _openAiClosed,
  _anthropicRejectsOpenAiClient,
  _openAiRejectsAnthropicClient,
  _anthropicProvidesModel,
  _openAiProvidesModel,
  _anthropicProvidesProviderName,
  _anthropicProvidesModelName,
  _openAiProvidesProviderName,
  _openAiProvidesModelName,
];
