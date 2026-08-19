import type { CodeExecutor } from './code-executor.js';

const identifier = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

const isRecord = (
  value: CodeExecutor.JsonValue | undefined,
): value is Readonly<Record<string, CodeExecutor.JsonValue>> =>
  value !== undefined &&
  value !== null &&
  typeof value === 'object' &&
  !Array.isArray(value);

const isJsonArray = (
  value: CodeExecutor.JsonValue,
): value is ReadonlyArray<CodeExecutor.JsonValue> => Array.isArray(value);

const property = (name: string): string =>
  identifier.test(name) ? name : JSON.stringify(name);

const literal = (value: CodeExecutor.JsonValue): string =>
  value === null ? 'null' : JSON.stringify(value);

const member = (
  schema: Readonly<Record<string, CodeExecutor.JsonValue>>,
  key: string,
): CodeExecutor.JsonValue | undefined => schema[key];

const union = (types: ReadonlyArray<string>): string => {
  const unique = Array.from(new Set(types));
  return unique.length === 0 ? 'unknown' : unique.join(' | ');
};

const reference = (
  root: Readonly<Record<string, CodeExecutor.JsonValue>>,
  path: string,
): CodeExecutor.JsonValue | undefined => {
  if (!path.startsWith('#/')) {
    return undefined;
  }
  let current: CodeExecutor.JsonValue = root;
  for (const encoded of path.slice(2).split('/')) {
    if (!isRecord(current)) {
      return undefined;
    }
    const key = encoded.replaceAll('~1', '/').replaceAll('~0', '~');
    const next: CodeExecutor.JsonValue | undefined = current[key];
    if (next === undefined) {
      return undefined;
    }
    current = next;
  }
  return current;
};

const renderSchema = (
  value: CodeExecutor.JsonValue,
  root: Readonly<Record<string, CodeExecutor.JsonValue>>,
  depth = 0,
  references: ReadonlySet<string> = new Set(),
): string => {
  if (value === true) {
    return 'unknown';
  }
  if (value === false || !isRecord(value) || depth >= 16) {
    return 'never';
  }

  const ref = member(value, '$ref');
  if (typeof ref === 'string') {
    if (references.has(ref)) {
      return 'unknown';
    }
    const resolved = reference(root, ref);
    if (resolved === undefined) {
      return 'unknown';
    }
    return renderSchema(
      resolved,
      root,
      depth + 1,
      new Set([...references, ref]),
    );
  }

  const constant = member(value, 'const');
  if (constant !== undefined) {
    return literal(constant);
  }

  const enumeration = member(value, 'enum');
  if (Array.isArray(enumeration)) {
    return union(enumeration.map(literal));
  }

  for (const keyword of ['oneOf', 'anyOf'] as const) {
    const choices = member(value, keyword);
    if (choices !== undefined && isJsonArray(choices)) {
      return union(
        choices.map((choice) =>
          renderSchema(choice, root, depth + 1, references),
        ),
      );
    }
  }

  const all = member(value, 'allOf');
  if (all !== undefined && isJsonArray(all)) {
    const rendered = all.map((choice) =>
      renderSchema(choice, root, depth + 1, references),
    );
    return rendered.length === 0 ? 'unknown' : rendered.join(' & ');
  }

  const declaredType = member(value, 'type');
  if (Array.isArray(declaredType)) {
    return union(
      declaredType.map((type) =>
        typeof type === 'string'
          ? renderSchema({ ...value, type }, root, depth + 1, references)
          : 'unknown',
      ),
    );
  }

  const type =
    typeof declaredType === 'string'
      ? declaredType
      : member(value, 'properties') !== undefined
        ? 'object'
        : undefined;
  switch (type) {
    case undefined:
      return 'unknown';
    case 'null':
      return 'null';
    case 'boolean':
      return 'boolean';
    case 'integer':
    case 'number':
      return 'number';
    case 'string':
      return 'string';
    case 'array': {
      const items = member(value, 'items');
      return `ReadonlyArray<${items === undefined ? 'unknown' : renderSchema(items, root, depth + 1, references)}>`;
    }
    case 'object': {
      const properties = member(value, 'properties');
      const requiredValue = member(value, 'required');
      const required = new Set(
        Array.isArray(requiredValue)
          ? requiredValue.filter(
              (item): item is string => typeof item === 'string',
            )
          : [],
      );
      const fields = isRecord(properties)
        ? Object.entries(properties).map(
            ([name, schema]) =>
              `readonly ${property(name)}${required.has(name) ? '' : '?'}: ${renderSchema(schema, root, depth + 1, references)}`,
          )
        : [];
      const additional = member(value, 'additionalProperties');
      if (additional !== false) {
        fields.push(
          `readonly [key: string]: ${isRecord(additional) ? renderSchema(additional, root, depth + 1, references) : 'unknown'}`,
        );
      }
      return fields.length === 0
        ? additional === false
          ? 'Record<string, never>'
          : 'Record<string, unknown>'
        : `{ ${fields.join('; ')} }`;
    }
    default:
      return 'unknown';
  }
};

const schemaType = (schema: CodeExecutor.JsonValue): string =>
  isRecord(schema) ? renderSchema(schema, schema) : 'unknown';

const comment = (description: string): string =>
  description.replaceAll('*/', '* /').replaceAll(/\s+/g, ' ').trim();

/** Render the complete model-visible TypeScript API for one hidden toolkit. */
export const renderCodeSdk = (
  tools: ReadonlyArray<CodeExecutor.ToolDescriptor>,
): string => {
  const methods = tools.flatMap((tool) => {
    const documentation = comment(tool.description);
    return [
      ...(documentation.length === 0 ? [] : [`  /** ${documentation} */`]),
      `  readonly ${property(tool.name)}: (input: ${schemaType(tool.parameters)}) => Promise<${schemaType(tool.result)}>;`,
    ];
  });
  return [
    'type JsonValue = null | boolean | number | string | ReadonlyArray<JsonValue> | { readonly [key: string]: JsonValue };',
    'type ToolFailureCode = "tool_failure" | "approval_required" | "dispatch_failed";',
    'declare class ToolCallError extends Error {',
    '  readonly name: "ToolCallError";',
    '  readonly code: ToolFailureCode;',
    '  readonly tool: keyof typeof tools & string;',
    '  readonly value?: JsonValue;',
    '}',
    'declare const tools: {',
    ...methods,
    '};',
    'declare function text(value: string | JsonValue): void;',
    'declare function store(key: string, value: JsonValue): void;',
    'declare function load(key: string): JsonValue | null;',
  ].join('\n');
};
