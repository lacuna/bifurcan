package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.allocator.IBuffer;
import io.lacuna.bifurcan.durable.blocks.TempStream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Function;

public class ChunkSort {

  private static class Chunk<T> {
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

    Iterator<T> entries() {
      return entries.values().stream().flatMap(IList::stream).iterator();
    }

    <S> S spill(Function<Iterator<T>, S> encode) {
      return encode.apply(entries());
    }
  }

  private static class Accumulator<T, S> {

    private final Function<Iterator<T>, S> encode;
    private final Function<S, Iterator<T>> decode;
    private final Comparator<T> comparator;
    private final int maxRealizedElements;

    private LinearList<S> spilled = new LinearList<>();
    private Chunk<T> curr;

    public Accumulator(Function<Iterator<T>, S> encode,
                       Function<S, Iterator<T>> decode,
                       Comparator<T> comparator,
                       int maxRealizedElements) {
      this.encode = encode;
      this.decode = decode;
      this.comparator = comparator;
      this.maxRealizedElements = maxRealizedElements;

      this.curr = new Chunk<>(comparator);
    }

    public void add(T x) {
      curr.add(x);
      if (curr.size() >= maxRealizedElements) {
        spilled.addLast(curr.spill(encode));
        curr = new Chunk<>(comparator);
      }
    }

    private S spill(IList<Iterator<T>> iterators) {
      return encode.apply(Util.mergeSort(iterators, comparator));
    }

    public Iterator<T> sortedIterator() {
      IList<Iterator<T>> iterators = spilled.stream().map(decode).collect(Lists.linearCollector());
      if (curr != null && curr.size > 0) {
        iterators.addLast(curr.entries());
        curr = null;
      }

      while (iterators.size() > maxRealizedElements) {
        IList<Iterator<T>> merged = new LinearList<>();
        for (int i = 0; i < iterators.size(); i += maxRealizedElements) {
          merged.addLast(decode.apply(spill(iterators.slice(i, Math.min(iterators.size(), i + maxRealizedElements)))));
        }
        TempStream.release();
        iterators = merged;
      }

      return Util.mergeSort(iterators, comparator);
    }
  }

  ///

  public static <T, S> Iterator<T> sortedEntries(
      Iterator<T> entries,
      Function<Iterator<T>, S> encode,
      Function<S, Iterator<T>> decode,
      Comparator<T> comparator,
      int maxRealizedElements) {

    Accumulator<T, S> acc = new Accumulator<>(encode, decode, comparator, maxRealizedElements);
    entries.forEachRemaining(acc::add);
    return acc.sortedIterator();
  }

  public static <V> Iterator<V> sortedEntries(
      Iterator<V> entries,
      Comparator<V> comparator,
      IDurableEncoding elementEncoding,
      int maxRealizedElements) {
    return sortedEntries(
        entries,
        it -> TempStream.encode(it, elementEncoding),
        bufs -> TempStream.decode((IList<IBuffer>) bufs, elementEncoding),
        comparator,
        maxRealizedElements);
  }
}
