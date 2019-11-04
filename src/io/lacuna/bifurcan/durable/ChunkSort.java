package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.LongStream;

public class ChunkSort {

  private static class Chunk<T> {
    static final int MAX_SIZE = 1 << 16;

    private final SortedMap<T, LinearList<T>> entries;
    private int size;

    Chunk(Comparator<T> comparator) {
      this.entries = new SortedMap<T, LinearList<T>>(comparator).linear();
    }

    void add(T entry) {
      size++;
      entries.getOrCreate(entry, LinearList::new).addLast(entry);
    }

    int size() {
      return size;
    }

    Iterable<T> entries() {
      return () -> entries.values().stream().flatMap(IList::stream).iterator();
    }

    Iterator<T> spill(BiConsumer<T, DurableOutput> encode, Function<DurableInput, T> decode) {
      DurableAccumulator acc = new DurableAccumulator();
      entries().forEach(e -> encode.accept(e, acc));
      DurableInput in = DurableInput.from(acc.contents());

      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          return in.remaining() > 0;
        }

        @Override
        public T next() {
          T e = decode.apply(in);
          if (in.remaining() == 0) {
            // once it's exhausted, we don't need it anymore
            in.close();
          }
          return e;
        }
      };
    }
  }

  public static <T> Iterator<T> sortedEntries(
      Iterable<T> entries,
      BiConsumer<T, DurableOutput> encode,
      Function<DurableInput, T> decode,
      Comparator<T> comparator) {
    LinearList<Iterator<T>> iterators = new LinearList<>();
    Chunk<T> chunk = new Chunk<>(comparator);

    for (T e : entries) {
      chunk.add(e);
      if (chunk.size() >= Chunk.MAX_SIZE) {
        iterators.addLast(chunk.spill(encode, decode));
        chunk = new Chunk<>(comparator);
      }
    }

    if (chunk.size > 0) {
      iterators.addLast(chunk.entries().iterator());
    }

    return Util.mergeSort(iterators, comparator);
  }

  ///

  public static <V> Iterator<V> sortedEntries(ICollection<?, V> c, Comparator<V> comparator) {
    return Iterators.map(
        sortedEntries(
            () -> LongStream.range(0, c.size()).iterator(),
            (i, out) -> out.writeVLQ(i),
            DurableInput::readVLQ,
            (a, b) -> comparator.compare(c.nth(a), c.nth(b))),
        c::nth);
  }


}
