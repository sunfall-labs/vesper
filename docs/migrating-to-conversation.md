# Migrating to Conversation

Durable operations are bound to a conversation identity through
`@sunfall/vesper-agent/conversation`. Agent definitions now contain only agent
behavior; persistence, resumption, branching, records, and signals live on a
`Conversation.Instance`.

```ts
import { Conversation } from '@sunfall/vesper-agent/conversation';

const conversation = Conversation.make(agent, conversationId);
```

## API replacements

| Removed API                             | Replacement                                                |
| --------------------------------------- | ---------------------------------------------------------- |
| `agent.recordingTo(id).run(input)`      | `Conversation.make(agent, id).run(input)`                  |
| `agent.recordingTo(id).stream(input)`   | `Conversation.make(agent, id).stream(input)`               |
| `agent.recordingTo(id, policy)`         | `Conversation.make(agent, id, policy)`                     |
| `agent.resume(id, input)`               | `Conversation.make(agent, id).run(input)`                  |
| `agent.branchFrom(id, at, input)`       | `Conversation.make(agent, id).branchFrom(at, input)`       |
| `agent.forkFrom(id, at, target, input)` | `Conversation.make(agent, id).forkFrom(at, target, input)` |
| `Agent.streamFrom(id, after)`           | `Conversation.make(agent, id).follow(after)`               |
| `AgentSignals.send(id, signal)`         | `Conversation.make(agent, id).send(signal)`                |

`conversation.records(after?)` reads a finite snapshot and completes.
`conversation.follow(after?)` first replays existing records and then waits for
future appends until interrupted.

Bind once when several operations share an identity:

```ts
const conversation = Conversation.make(agent, conversationId);

yield * conversation.run('Start');
yield * conversation.run('Continue'); // automatically continues the history
const records = yield * conversation.records().pipe(Stream.runCollect);
```

Recording policy is also bound once and applies to every continuation, branch,
and fork:

```ts
const conversation = Conversation.make(agent, conversationId, policy);
```
