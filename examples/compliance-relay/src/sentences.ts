import { Stream } from 'effect';

/** Regroup arbitrary text deltas into sentences and flush the final fragment. */
export const bySentence = <E, R>(
  self: Stream.Stream<string, E, R>,
): Stream.Stream<string, E, R> =>
  self.pipe(
    // A synthetic paragraph break makes the accumulator emit an unterminated
    // final sentence when the source stream ends.
    Stream.concat(Stream.make('\n\n')),
    Stream.mapAccum(
      (): string => '',
      (buffer: string, delta: string) => {
        const parts = (buffer + delta).split(/(?<=[.!?])\s+|\n{2,}/);
        const rest = parts.pop() ?? '';
        return [rest, parts.filter((part) => part.trim() !== '')] as const;
      },
    ),
  );
