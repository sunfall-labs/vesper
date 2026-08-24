import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';

import {
  Cause,
  Deferred,
  Effect,
  Layer,
  Option,
  Queue,
  Schema,
  Stream,
} from 'effect';

import { CodeExecutor } from './code-executor.js';

export interface Options {
  /** Override used by packaged hosts and fail-closed deployment tests. */
  readonly hostUrl?: URL;
}

type Runtime = 'bun' | 'deno' | 'node';

const defaultHostUrl = new URL(
  '../host/code-sandbox-host.mjs',
  import.meta.url,
);

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Unknown),
);

const runtime = (): Runtime => {
  if ('deno' in process.versions) {
    return 'deno';
  }
  if ('bun' in process.versions) {
    return 'bun';
  }
  return 'node';
};

const hostArguments = (
  hostPath: string,
  heapMiB: number,
): Effect.Effect<ReadonlyArray<string>, CodeExecutor.ExecutorError> => {
  switch (runtime()) {
    case 'bun':
      return Effect.succeed(['--smol', hostPath]);
    case 'deno':
      return Effect.succeed([
        'run',
        '--no-config',
        '--no-prompt',
        `--allow-read=${hostPath}`,
        `--v8-flags=--max-old-space-size=${String(heapMiB)}`,
        hostPath,
      ]);
    case 'node': {
      const permissionFlag = process.allowedNodeEnvironmentFlags.has(
        '--permission',
      )
        ? '--permission'
        : process.allowedNodeEnvironmentFlags.has('--experimental-permission')
          ? '--experimental-permission'
          : undefined;
      return permissionFlag === undefined
        ? Effect.fail(
            new CodeExecutor.ExecutorError(
              'unavailable',
              'This Node.js runtime cannot enforce sandbox host permissions',
            ),
          )
        : Effect.succeed([
            permissionFlag,
            `--allow-fs-read=${hostPath}`,
            `--max-old-space-size=${String(heapMiB)}`,
            hostPath,
          ]);
    }
    default:
      return Effect.fail(
        new CodeExecutor.ExecutorError(
          'unavailable',
          'This JavaScript runtime cannot start the code sandbox host',
        ),
      );
  }
};

const isEvent = (value: unknown): value is CodeExecutor.Event => {
  if (typeof value !== 'object' || value === null || !('_tag' in value)) {
    return false;
  }
  switch (value._tag) {
    case 'ToolCall':
      return (
        'id' in value &&
        typeof value.id === 'string' &&
        'name' in value &&
        typeof value.name === 'string' &&
        'input' in value
      );
    case 'Output':
      return 'value' in value && typeof value.value === 'string';
    case 'Completion':
      return (
        'state' in value &&
        typeof value.state === 'object' &&
        value.state !== null &&
        !Array.isArray(value.state) &&
        (!('result' in value) || value.result !== undefined)
      );
    case 'Failure':
      return 'message' in value && typeof value.message === 'string';
    default:
      return false;
  }
};

const write = (
  child: ChildProcessWithoutNullStreams,
  value: unknown,
): Effect.Effect<void, CodeExecutor.ExecutorError> =>
  Effect.try({
    try: () => {
      child.stdin.write(`${encodeJson(value)}\n`);
    },
    catch: (cause) =>
      new CodeExecutor.ExecutorError(
        'protocol',
        `Could not write to code sandbox host: ${String(cause)}`,
      ),
  });

const start = (
  options: Options,
  request: CodeExecutor.Request,
): Effect.Effect<CodeExecutor.Execution, CodeExecutor.ExecutorError> =>
  Effect.gen(function* () {
    const sourceBytes = new TextEncoder().encode(request.source).byteLength;
    if (sourceBytes > request.limits.maxSourceBytes) {
      return yield* Effect.fail(
        new CodeExecutor.ExecutorError(
          'protocol',
          `Code source exceeds ${String(request.limits.maxSourceBytes)} bytes`,
        ),
      );
    }

    const queue = yield* Queue.unbounded<
      CodeExecutor.Event,
      CodeExecutor.ExecutorError | Cause.Done
    >();
    const stopped = yield* Deferred.make<void>();
    const heapMiB = Math.max(
      1,
      Math.floor(request.limits.maxHeapBytes / (1024 * 1024)),
    );
    const hostPath = fileURLToPath(options.hostUrl ?? defaultHostUrl);
    const arguments_ = yield* hostArguments(hostPath, heapMiB);
    const child = yield* Effect.try({
      try: () =>
        spawn(process.execPath, arguments_, {
          env: {},
          stdio: ['pipe', 'pipe', 'pipe'],
        }),
      catch: (cause) =>
        new CodeExecutor.ExecutorError(
          'unavailable',
          `Could not start code sandbox host: ${String(cause)}`,
        ),
    });

    let terminal = false;
    let timedOut = false;
    let forceKill: ReturnType<typeof setTimeout> | undefined;
    const clearTimers = () => {
      clearTimeout(timeout);
      if (forceKill !== undefined) {
        clearTimeout(forceKill);
        forceKill = undefined;
      }
    };
    const terminate = () => {
      if (child.exitCode !== null || child.signalCode !== null) {
        return;
      }
      child.kill();
      forceKill ??= setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) {
          child.kill('SIGKILL');
        }
      }, 1_000);
    };
    const timeout = setTimeout(() => {
      if (terminal) {
        return;
      }
      timedOut = true;
      terminate();
    }, request.limits.wallClockMillis);

    const finish = (event: CodeExecutor.Completion | CodeExecutor.Failure) => {
      if (terminal) {
        return;
      }
      terminal = true;
      clearTimers();
      Queue.offerUnsafe(queue, event);
      Queue.endUnsafe(queue);
    };
    const fail = (error: CodeExecutor.ExecutorError) => {
      if (terminal) {
        return;
      }
      terminal = true;
      clearTimers();
      Queue.failCauseUnsafe(queue, Cause.fail(error));
    };

    const lines = createInterface({ input: child.stdout });
    child.stderr.resume();
    lines.on('line', (line) => {
      let parsed: unknown;
      try {
        parsed = decodeJson(line);
      } catch {
        fail(
          new CodeExecutor.ExecutorError(
            'protocol',
            'Code sandbox host emitted invalid JSON',
          ),
        );
        terminate();
        return;
      }
      if (!isEvent(parsed)) {
        fail(
          new CodeExecutor.ExecutorError(
            'protocol',
            'Code sandbox host emitted an invalid event',
          ),
        );
        terminate();
      } else if (parsed._tag === 'Completion' || parsed._tag === 'Failure') {
        finish(parsed);
      } else if (!terminal) {
        Queue.offerUnsafe(queue, parsed);
      }
    });
    child.once('error', (cause) => {
      fail(
        new CodeExecutor.ExecutorError(
          'unavailable',
          `Code sandbox host is unavailable: ${cause.message}`,
        ),
      );
      Deferred.doneUnsafe(stopped, Effect.void);
    });
    child.stdin.on('error', (cause) => {
      fail(
        new CodeExecutor.ExecutorError(
          'protocol',
          `Code sandbox host input failed: ${cause.message}`,
        ),
      );
      terminate();
    });
    child.once('exit', (code, signal) => {
      clearTimers();
      Deferred.doneUnsafe(stopped, Effect.void);
      if (timedOut) {
        finish({
          _tag: 'Failure',
          message: `Code execution exceeded ${String(request.limits.wallClockMillis)}ms`,
        });
      } else if (!terminal) {
        fail(
          new CodeExecutor.ExecutorError(
            'unavailable',
            `Code sandbox host exited before completion (${String(signal ?? code ?? 'unknown')})`,
          ),
        );
      }
    });

    const interrupt = Effect.gen(function* () {
      clearTimers();
      terminate();
      const graceful = yield* Deferred.await(stopped).pipe(
        Effect.timeoutOption('1 second'),
      );
      if (Option.isNone(graceful)) {
        child.kill('SIGKILL');
        yield* Deferred.await(stopped).pipe(Effect.timeoutOption('1 second'));
      }
      lines.close();
    });

    yield* write(child, { type: 'execute', request }).pipe(
      Effect.tapError(() => interrupt),
    );

    return {
      events: Stream.fromQueue(queue).pipe(Stream.ensuring(interrupt)),
      respond: (response) =>
        terminal
          ? Effect.fail(
              new CodeExecutor.ExecutorError(
                'protocol',
                'Code sandbox host has already stopped',
              ),
            )
          : write(child, { type: 'tool_response', response }),
      interrupt,
    };
  });

/** A credential-free, separate-process TypeScript executor for Node.js, Bun, and Deno. */
export const layer = (
  options: Options = {},
): Layer.Layer<CodeExecutor.Service> =>
  Layer.succeed(CodeExecutor.Service, {
    start: (request) => start(options, request),
  });

export * as NodeCodeExecutor from './code-executor-node.js';
