import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

/** The budget hand-written `RunStarted` fixtures record; every run carries one. */
export const testBudget: ConversationRecord.RunBudget = {
  id: 'test-budget',
  root: LogVocabulary.ConversationId.make('test-root'),
  limits: {},
};
