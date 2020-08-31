package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.allocator.IBuffer;
import io.lacuna.bifurcan.durable.codecs.TempStream;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Function;

public class ChunkSort {

  private static class Chunk<T> {
    private final T[] entries;
    private Comparator<T> comparator;
    private int size;

    Chunk(int maxSize, Comparator<T> comparator) {
      this.entries = (T[]) new Object[maxSize];
      this.comparator = comparator;
    }

    void add(T entry) {
      entries[size++] = entry;
    }

    int size() {
      return size;
    }

    Iterator<T> entries() {
      Arrays.sort(entries, 0, size, comparator);
      return Iterators.range(0, size, i -> entries[(int) i]);
    }

    <S> S spill(Function<Iterator<T>, S> encode) {
      return encode.apply(entries());
    }
  }

  public static class Accumulator<T, S> {

    private final Function<Iterator<T>, S> encode;
    private final Function<S, Iterator<T>> decode;
    private final Comparator<T> comparator;
    private final int maxRealizedElements;

    private LinearList<S> spilled = new LinearList<>();
    private Chunk<T> curr;

    public Accumulator(
        Function<Iterator<T>, S> encode,
        Function<S, Iterator<T>> decode,
        Comparator<T> comparator,
        int maxRealizedElements
    ) {
      this.encode = encode;
      this.decode = decode;
      this.comparator = comparator;
      this.maxRealizedElements = maxRealizedElements;

      this.curr = new Chunk<>(maxRealizedElements, comparator);
    }

    public void add(T x) {
      curr.add(x);
      if (curr.size() >= maxRealizedElements) {
        spilled.addLast(curr.spill(encode));
        curr = new Chunk<>(maxRealizedElements, comparator);
      }
    }

    private S spill(IList<Iterator<T>> iterators) {
      return encode.apply(Iterators.mergeSort(iterators, comparator));
    }

    public Iterator<T> sortedIterator() {
      IList<Iterator<T>> iterators = spilled.stream().map(decode).collect(Lists.linearCollector());
      if (curr != null && curr.size > 0) {
        iterators.addLast(curr.entries());
        curr = null;
      }

      while (iterators.size() > maxRealizedElements) {
        IList<Iterator<T>> merged = new LinearList<>();
        TempStream.push();
        for (int i = 0; i < iterators.size(); i += maxRealizedElements) {
          merged.addLast(decode.apply(spill(iterators.slice(i, Math.min(iterators.size(), i + maxRealizedElements)))));
        }
        TempStream.pop();
        iterators = merged;
      }

      return Iterators.mergeSort(iterators, comparator);
    }
  }

  ///

  public static <V> ChunkSort.Accumulator<V, ?> accumulator(
      Comparator<V> comparator,
      IDurableEncoding elementEncoding,
      int maxRealizedElements
  ) {
    TempStream.push();
    return new Accumulator<>(
        it -> TempStream.encode(it, elementEncoding),
        bufs -> TempStream.decode((IList<IBuffer>) bufs, elementEncoding),
        comparator,
        maxRealizedElements
    );
  }

  public static <T, S> Iterator<T> sortedEntries(
      Iterator<T> entries,
      Function<Iterator<T>, S> encode,
      Function<S, Iterator<T>> decode,
      Comparator<T> comparator,
      int maxRealizedElements
  ) {
    TempStream.push();
    Accumulator<T, S> acc = new Accumulator<>(encode, decode, comparator, maxRealizedElements);
    entries.forEachRemaining(acc::add);
    return acc.sortedIterator();
  }

  public static <V> Iterator<V> sortedEntries(
      Iterator<V> entries,
      Comparator<V> comparator,
      IDurableEncoding elementEncoding,
      int maxRealizedElements
  ) {
    return sortedEntries(
        entries,
        it -> TempStream.encode(it, elementEncoding),
        bufs -> TempStream.decode((IList<IBuffer>) bufs, elementEncoding),
        comparator,
        maxRealizedElements
    );
  }
}
