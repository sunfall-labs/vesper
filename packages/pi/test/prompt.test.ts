import type { Message as PiMessage } from '@earendil-works/pi-ai';
import {
  fauxAssistantMessage,
  fauxProvider,
} from '@earendil-works/pi-ai/providers/faux';
import { Effect, Layer } from 'effect';
import { LanguageModel, type Prompt } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { CredentialStore } from '../src/credentials.js';
import { PiModel } from '../src/model.js';
import { PiRegistry } from '../src/registry.js';

// What the provider actually received.
//
// Asserting on the converted `PiContext` directly would test a function
// against its own author's expectations; running the real prompt through the
// assembled `LanguageModel` and reading it back out of Pi's own faux provider
// tests what Pi is handed, which is the only thing that matters. Effect's
// `Prompt` decoding sits in between, so this also proves the encoded shapes
// used here are the ones a caller can really write.

const seenMessages = (prompt: Prompt.RawInput): Promise<PiMessage[]> => {
  const handle = fauxProvider({
    provider: 'faux',
    models: [{ id: 'faux-1' }],
    tokensPerSecond: 0,
  });
  handle.setResponses([
    (context) => fauxAssistantMessage(JSON.stringify(context.messages)),
  ]);

  const registry = PiRegistry.layer({
    register: (models) =>
      Effect.sync(() => models.setProvider(handle.provider)),
  }).pipe(Layer.provide(CredentialStore.layerMemory));

  return Effect.runPromise(
    Effect.gen(function* () {
      const response = yield* LanguageModel.generateText({ prompt });
      return JSON.parse(response.text) as PiMessage[];
    }).pipe(
      Effect.provide(
        PiModel.model('faux', 'faux-1').pipe(Layer.provide(registry)),
      ),
    ) as Effect.Effect<PiMessage[]>,
  );
};

const userContent = (messages: PiMessage[]) => {
  const first = messages[0];
  if (first?.role !== 'user' || typeof first.content === 'string') {
    throw new Error(`expected a structured user message, got ${first?.role}`);
  }
  return first.content;
};

// A one-pixel PNG. Real base64 rather than a placeholder string, so the
// data-URL and byte-array cases can be compared against a known payload.
const PIXEL_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==';

const PIXEL_BYTES = Uint8Array.from(
  globalThis.atob(PIXEL_BASE64),
  (character) => character.charCodeAt(0),
);

describe('image attachments', () => {
  it('sends a base64 image as an image part', async () => {
    const content = userContent(
      await seenMessages([
        {
          role: 'user',
          content: [
            { type: 'text', text: 'what is this?' },
            {
              type: 'file',
              mediaType: 'image/png',
              fileName: 'pixel.png',
              data: PIXEL_BASE64,
            },
          ],
        },
      ]),
    );

    expect(content).toEqual([
      { type: 'text', text: 'what is this?' },
      { type: 'image', data: PIXEL_BASE64, mimeType: 'image/png' },
    ]);
  });

  // The case that motivated touching this at all. Effect's own `FilePart`
  // documentation uses a data URL as the example value, and Pi's adapters wrap
  // `data:${mimeType};base64,` around whatever they are given — so passing one
  // straight through produces a doubled prefix and a rejected request.
  it('strips the data-URL wrapper Pi would otherwise double', async () => {
    const content = userContent(
      await seenMessages([
        {
          role: 'user',
          content: [
            {
              type: 'file',
              mediaType: 'image/png',
              data: `data:image/png;base64,${PIXEL_BASE64}`,
            },
          ],
        },
      ]),
    );

    expect(content).toEqual([
      { type: 'image', data: PIXEL_BASE64, mimeType: 'image/png' },
    ]);
  });

  // `FilePart.data` admits raw bytes, which is what an attachment store hands
  // back. Before this, they failed the `typeof data === 'string'` test and the
  // image disappeared without a word.
  it('encodes raw bytes rather than dropping them', async () => {
    const content = userContent(
      await seenMessages([
        {
          role: 'user',
          content: [
            { type: 'file', mediaType: 'image/png', data: PIXEL_BYTES },
          ],
        },
      ]),
    );

    expect(content).toEqual([
      { type: 'image', data: PIXEL_BASE64, mimeType: 'image/png' },
    ]);
  });
});

describe('attachments Pi cannot carry', () => {
  // Pi 0.80.2 has no document content type in its message algebra, so this is
  // a statement about the provider protocol and not about the model behind it.
  it('marks a document instead of dropping it silently', async () => {
    const content = userContent(
      await seenMessages([
        {
          role: 'user',
          content: [
            { type: 'text', text: 'summarise the attached report' },
            {
              type: 'file',
              mediaType: 'application/pdf',
              fileName: 'report.pdf',
              data: 'JVBERi0=',
            },
          ],
        },
      ]),
    );

    expect(content[0]).toEqual({
      type: 'text',
      text: 'summarise the attached report',
    });
    expect(content).toHaveLength(2);
    const marker = content[1]!;
    expect(marker.type).toBe('text');
    expect(marker.type === 'text' && marker.text).toContain('report.pdf');
    expect(marker.type === 'text' && marker.text).toContain('application/pdf');
    // Never as an image: a PDF announced as `image/png` is a request the
    // provider rejects, and one announced honestly has nowhere to go.
    expect(content.some((part) => part.type === 'image')).toBe(false);
  });

  // Resolving it would put a network fetch inside a conversion the durability
  // layer replays. The marker says so rather than sending a URL string where
  // base64 is expected.
  it('marks a URL attachment rather than sending the URL as image data', async () => {
    const content = userContent(
      await seenMessages([
        {
          role: 'user',
          content: [
            {
              type: 'file',
              mediaType: 'image/png',
              fileName: 'remote.png',
              data: new URL('https://example.invalid/remote.png'),
            },
          ],
        },
      ]),
    );

    expect(content).toHaveLength(1);
    const marker = content[0]!;
    expect(marker.type).toBe('text');
    expect(marker.type === 'text' && marker.text).toContain('remote.png');
    expect(content.some((part) => part.type === 'image')).toBe(false);
  });

  it('marks a data URL that is not base64-encoded', async () => {
    const content = userContent(
      await seenMessages([
        {
          role: 'user',
          content: [
            {
              type: 'file',
              mediaType: 'image/svg+xml',
              data: 'data:image/svg+xml,%3Csvg%2F%3E',
            },
          ],
        },
      ]),
    );

    const marker = content[0]!;
    expect(marker.type).toBe('text');
    expect(content.some((part) => part.type === 'image')).toBe(false);
  });
});

describe('attachment conversion is replay-safe', () => {
  // Provider-seam checkpointing keys on the converted request, so a prompt
  // that converts differently on a second pass would miss its own checkpoint.
  it('converts the same prompt to the same content twice', async () => {
    const prompt: Prompt.RawInput = [
      {
        role: 'user',
        content: [
          { type: 'text', text: 'look' },
          { type: 'file', mediaType: 'image/png', data: PIXEL_BYTES },
          {
            type: 'file',
            mediaType: 'application/pdf',
            fileName: 'report.pdf',
            data: 'JVBERi0=',
          },
        ],
      },
    ];

    const [first, second] = await Promise.all([
      seenMessages(prompt),
      seenMessages(prompt),
    ]);

    expect(second).toEqual(first);
  });
});

// A tool result carries whether the tool failed, and so does Pi's `toolResult`
// message — its Anthropic adapter writes the flag straight into
// `tool_result.is_error`. It used to be hardcoded `false` here, which told
// every model on every turn after a failure, and on every resumed conversation
// containing one, that the failing tool had succeeded.
describe('tool results', () => {
  const toolResults = (isFailure: boolean) =>
    seenMessages([
      {
        role: 'assistant',
        content: [
          {
            type: 'tool-call',
            id: 'call_1',
            name: 'charge_card',
            params: { amountCents: 4999 },
            providerExecuted: false,
          },
        ],
      },
      {
        role: 'tool',
        content: [
          {
            type: 'tool-result',
            id: 'call_1',
            name: 'charge_card',
            isFailure,
            result: { declined: 'issuer declined', code: 'DO_NOT_HONOR' },
          },
        ],
      },
    ]);

  const resultMessage = (messages: PiMessage[]) => {
    const found = messages.find((message) => message.role === 'toolResult');
    if (found?.role !== 'toolResult') {
      throw new Error('expected a toolResult message');
    }
    return found;
  };

  it('marks a failed tool result as an error', async () => {
    expect(resultMessage(await toolResults(true)).isError).toBe(true);
  });

  it('leaves a successful tool result unmarked', async () => {
    expect(resultMessage(await toolResults(false)).isError).toBe(false);
  });
});
