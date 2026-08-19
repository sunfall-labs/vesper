import { stripTypeScriptTypes } from 'node:module';
import { createInterface } from 'node:readline';
import vm from 'node:vm';

/** @typedef {{name: string}} ToolDescriptor */
/** @typedef {{maxNestedCalls: number, maxOutputBytes: number, wallClockMillis: number}} Limits */
/** @typedef {{source: string, state: Record<string, unknown>, tools: ReadonlyArray<ToolDescriptor>, limits: Limits}} ExecuteRequest */
/** @typedef {{id: string, outcome: 'success', value: unknown} | {id: string, outcome: 'failure', error: unknown}} ToolResponse */
/** @typedef {{type: 'execute', request: ExecuteRequest} | {type: 'tool_response', response: ToolResponse}} HostMessage */
/** @typedef {{resolve: (value: unknown) => void, reject: (reason: unknown) => void}} Waiter */

const reader = createInterface({ input: process.stdin });
/** @type {Map<string, Waiter>} */
const pending = new Map();
let running = false;

/** @param {unknown} event */
const send = (event) => process.stdout.write(`${JSON.stringify(event)}\n`);

/** @param {unknown} value */
const cloneJson = (value) => {
  const encoded = JSON.stringify(value, (_key, /** @type {unknown} */ item) => {
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
  return /** @type {unknown} */ (JSON.parse(encoded));
};

/** @param {unknown} value @returns {value is Record<string, unknown>} */
const isRecord = (value) => typeof value === 'object' && value !== null;

/** @param {unknown} value @returns {value is ToolDescriptor} */
const isToolDescriptor = (value) =>
  isRecord(value) && typeof value.name === 'string';

/** @param {unknown} value @returns {value is ExecuteRequest} */
const isExecuteRequest = (value) => {
  if (!isRecord(value)) {
    return false;
  }
  if (typeof value.source !== 'string' || !isRecord(value.state)) {
    return false;
  }
  if (!Array.isArray(value.tools)) {
    return false;
  }
  const tools = /** @type {ReadonlyArray<unknown>} */ (value.tools);
  if (!tools.every(isToolDescriptor)) {
    return false;
  }
  if (!isRecord(value.limits)) {
    return false;
  }
  return (
    typeof value.limits.maxNestedCalls === 'number' &&
    typeof value.limits.maxOutputBytes === 'number' &&
    typeof value.limits.wallClockMillis === 'number'
  );
};

/** @param {unknown} value @returns {value is ToolResponse} */
const isToolResponse = (value) => {
  if (!isRecord(value) || typeof value.id !== 'string') {
    return false;
  }
  if (value.outcome === 'success') {
    return 'value' in value;
  }
  return value.outcome === 'failure' && 'error' in value;
};

/** @param {unknown} value @returns {value is {snapshot: () => string}} */
const isControl = (value) =>
  isRecord(value) && typeof value.snapshot === 'function';

/** @param {string} line @returns {HostMessage} */
const parseMessage = (line) => {
  const value = /** @type {unknown} */ (JSON.parse(line));
  if (!isRecord(value) || typeof value.type !== 'string') {
    throw new TypeError('Host message must be an object');
  }
  if (value.type === 'execute' && isExecuteRequest(value.request)) {
    return { type: 'execute', request: value.request };
  }
  if (value.type === 'tool_response' && isToolResponse(value.response)) {
    return { type: 'tool_response', response: value.response };
  }
  throw new TypeError('Host message has invalid shape');
};

/** @param {ExecuteRequest} request */
const execute = async (request) => {
  const toolNames = new Set(request.tools.map(({ name }) => name));
  const encoder = new TextEncoder();
  let nextId = 1;
  let outputBytes = 0;
  let nestedCalls = 0;

  /** @param {string} name @param {unknown} input */
  const callTool = (name, input) => {
    if (!toolNames.has(name)) {
      throw new Error(`Unknown tool: ${name}`);
    }
    nestedCalls += 1;
    if (nestedCalls > request.limits.maxNestedCalls) {
      throw new Error(
        `Nested tool calls exceed ${String(request.limits.maxNestedCalls)}`,
      );
    }
    const id = String(nextId++);
    const promise = new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
    });
    send({ _tag: 'ToolCall', id, name, input: cloneJson(input) });
    return promise;
  };
  /** @param {string} output */
  const emitText = (output) => {
    outputBytes += encoder.encode(output).byteLength;
    if (outputBytes > request.limits.maxOutputBytes) {
      throw new Error(
        `Code output exceeds ${String(request.limits.maxOutputBytes)} bytes`,
      );
    }
    send({ _tag: 'Output', value: output });
  };
  /** @type {Record<string, unknown>} */
  const sandbox = {};
  Object.setPrototypeOf(sandbox, null);
  sandbox.__vesperBridge = { callTool, emitText };
  sandbox.__vesperState = JSON.stringify(cloneJson(request.state));
  sandbox.__vesperTools = JSON.stringify(cloneJson(request.tools));
  const context = vm.createContext(sandbox, {
    codeGeneration: { strings: false, wasm: false },
  });
  const controlScript = new vm.Script(`
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
      class ToolCallError extends Error {
        constructor(tool, failure) {
          super(failure.message)
          this.name = "ToolCallError"
          this.code = failure.code
          this.tool = tool
          if ("value" in failure) this.value = clone(failure.value)
        }
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
              if (
                cause !== null &&
                typeof cause === "object" &&
                typeof cause.code === "string" &&
                typeof cause.message === "string"
              ) throw new ToolCallError(name, cause)
              throw cause
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
        ToolCallError: { value: ToolCallError },
        load: { value: load },
        store: { value: store },
        text: { value: text },
        tools: { value: tools },
      })
      return Object.freeze({
        snapshot: () => JSON.stringify(Object.fromEntries(state)),
      })
    })()
  `);
  const controlValue = /** @type {unknown} */ (
    controlScript.runInContext(context, {
      timeout: request.limits.wallClockMillis,
    })
  );
  if (!isControl(controlValue)) {
    throw new TypeError('Sandbox control initialization failed');
  }
  const control = controlValue;
  /** @type {string} */
  let source;
  try {
    source = stripTypeScriptTypes(
      `"use strict"; (async () => {\n${request.source}\n})()`,
      { mode: 'strip' },
    );
  } catch {
    throw new Error('TypeScript source must use erasable syntax');
  }
  const script = new vm.Script(source);
  const result = /** @type {unknown} */ (
    await script.runInContext(context, {
      timeout: request.limits.wallClockMillis,
    })
  );
  const structured = result === undefined ? undefined : cloneJson(result);
  if (structured !== undefined) {
    const encodedStructured = JSON.stringify(structured);
    outputBytes += encoder.encode(encodedStructured).byteLength;
    if (outputBytes > request.limits.maxOutputBytes) {
      throw new Error(
        `Code output exceeds ${String(request.limits.maxOutputBytes)} bytes`,
      );
    }
  }
  const state = /** @type {unknown} */ (JSON.parse(control.snapshot()));
  if (!isRecord(state)) {
    throw new TypeError('Sandbox state snapshot must be an object');
  }
  send({
    _tag: 'Completion',
    state,
    ...(structured === undefined ? {} : { result: structured }),
  });
};

reader.on('line', (line) => {
  /** @type {HostMessage} */
  let message;
  try {
    message = parseMessage(line);
  } catch (cause) {
    send({
      _tag: 'Failure',
      message:
        cause instanceof SyntaxError
          ? 'Host received invalid JSON'
          : 'Host protocol violation',
    });
    process.exitCode = 1;
    reader.close();
    return;
  }
  if (message.type === 'tool_response') {
    const response = message.response;
    const waiter = pending.get(response.id);
    if (waiter === undefined) {
      return;
    }
    pending.delete(response.id);
    if (response.outcome === 'success') {
      waiter.resolve(cloneJson(response.value));
    } else {
      waiter.reject(cloneJson(response.error));
    }
    return;
  }
  if (running) {
    send({ _tag: 'Failure', message: 'Host protocol violation' });
    process.exitCode = 1;
    reader.close();
    return;
  }
  running = true;
  execute(message.request)
    .catch((/** @type {unknown} */ cause) => {
      const failureMessage =
        isRecord(cause) && typeof cause.message === 'string'
          ? cause.message
          : String(cause);
      send({ _tag: 'Failure', message: failureMessage });
      process.exitCode = 1;
    })
    .finally(() => {
      reader.close();
    })
    .catch(() => {
      process.exitCode = 1;
    });
});
