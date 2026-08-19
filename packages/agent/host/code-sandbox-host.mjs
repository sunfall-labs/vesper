import { createInterface } from 'node:readline';
import vm from 'node:vm';

const reader = createInterface({ input: process.stdin });
const pending = new Map();
let running = false;

const send = (event) => process.stdout.write(`${JSON.stringify(event)}\n`);

const cloneJson = (value) => {
  const encoded = JSON.stringify(value, (_key, item) => {
    if (
      item === undefined ||
      typeof item === 'function' ||
      typeof item === 'symbol' ||
      typeof item === 'bigint' ||
      (typeof item === 'number' && !Number.isFinite(item))
    ) {
      throw new TypeError('Value must be JSON serializable');
    }
    return item;
  });
  if (encoded === undefined)
    throw new TypeError('Value must be JSON serializable');
  return JSON.parse(encoded);
};

const execute = async (request) => {
  const toolNames = new Set(request.tools.map(({ name }) => name));
  const encoder = new TextEncoder();
  let nextId = 1;
  let outputBytes = 0;
  let nestedCalls = 0;

  const callTool = (name, input) => {
    if (!toolNames.has(name)) throw new Error(`Unknown tool: ${name}`);
    nestedCalls += 1;
    if (nestedCalls > request.limits.maxNestedCalls) {
      throw new Error(
        `Nested tool calls exceed ${request.limits.maxNestedCalls}`,
      );
    }
    const id = String(nextId++);
    const promise = new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
    });
    send({ _tag: 'ToolCall', id, name, input: cloneJson(input) });
    return promise;
  };
  const emitText = (output) => {
    outputBytes += encoder.encode(output).byteLength;
    if (outputBytes > request.limits.maxOutputBytes) {
      throw new Error(
        `Code output exceeds ${request.limits.maxOutputBytes} bytes`,
      );
    }
    send({ _tag: 'Output', value: output });
  };
  const sandbox = Object.assign(Object.create(null), {
    __vesperBridge: { callTool, emitText },
    __vesperState: JSON.stringify(cloneJson(request.state)),
    __vesperTools: JSON.stringify(cloneJson(request.tools)),
  });
  const context = vm.createContext(sandbox, {
    codeGeneration: { strings: false, wasm: false },
  });
  const control = new vm.Script(`
    (() => {
      const { callTool, emitText } = globalThis.__vesperBridge
      const state = new Map(Object.entries(JSON.parse(globalThis.__vesperState)))
      const descriptors = JSON.parse(globalThis.__vesperTools)
      const toolNames = new Set(descriptors.map(({ name }) => name))
      delete globalThis.__vesperBridge
      delete globalThis.__vesperState
      delete globalThis.__vesperTools

      const clone = (value) => {
        const encoded = JSON.stringify(value, (_key, item) => {
          if (
            item === undefined ||
            typeof item === "function" ||
            typeof item === "symbol" ||
            typeof item === "bigint" ||
            (typeof item === "number" && !Number.isFinite(item))
          ) throw new TypeError("Value must be JSON serializable")
          return item
        })
        if (encoded === undefined) throw new TypeError("Value must be JSON serializable")
        return JSON.parse(encoded)
      }
      const tools = new Proxy(Object.create(null), {
        get: (_target, name) => {
          if (typeof name !== "string" || !toolNames.has(name)) {
            throw new Error(\`Unknown tool: \${String(name)}\`)
          }
          return async (input) => {
            try {
              return clone(await Reflect.apply(callTool, undefined, [name, clone(input)]))
            } catch (cause) {
              throw new Error(
                cause !== null && typeof cause === "object" && typeof cause.message === "string"
                  ? cause.message
                  : String(cause),
              )
            }
          }
        },
      })
      const text = (value) => {
        const output = typeof value === "string" ? value : JSON.stringify(clone(value))
        Reflect.apply(emitText, undefined, [output])
      }
      const store = (key, value) => {
        if (typeof key !== "string") throw new TypeError("State key must be a string")
        state.set(key, clone(value))
      }
      const load = (key) => clone(state.get(key) ?? null)
      Object.defineProperties(globalThis, {
        ALL_TOOLS: { value: Object.freeze(descriptors) },
        load: { value: load },
        store: { value: store },
        text: { value: text },
        tools: { value: tools },
      })
      return Object.freeze({
        snapshot: () => JSON.stringify(Object.fromEntries(state)),
      })
    })()
  `).runInContext(context, { timeout: request.limits.wallClockMillis });
  const script = new vm.Script(
    `"use strict"; (async () => {\n${request.source}\n})()`,
  );
  await script.runInContext(context, {
    timeout: request.limits.wallClockMillis,
  });
  send({ _tag: 'Completion', state: JSON.parse(control.snapshot()) });
};

reader.on('line', (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    send({ _tag: 'Failure', message: 'Host received invalid JSON' });
    process.exitCode = 1;
    reader.close();
    return;
  }
  if (message.type === 'tool_response') {
    const response = message.response;
    const waiter = pending.get(response.id);
    if (waiter === undefined) return;
    pending.delete(response.id);
    if (response.outcome === 'success')
      waiter.resolve(cloneJson(response.value));
    else waiter.reject(new Error(String(response.value)));
    return;
  }
  if (message.type !== 'execute' || running) {
    send({ _tag: 'Failure', message: 'Host protocol violation' });
    process.exitCode = 1;
    reader.close();
    return;
  }
  running = true;
  execute(message.request)
    .catch((cause) => {
      send({
        _tag: 'Failure',
        message:
          cause !== null &&
          typeof cause === 'object' &&
          typeof cause.message === 'string'
            ? cause.message
            : String(cause),
      });
      process.exitCode = 1;
    })
    .finally(() => reader.close())
    .catch(() => {
      process.exitCode = 1;
    });
});
