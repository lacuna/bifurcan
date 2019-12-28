package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.LongStream;

public class ChunkSort {

  public interface CloseableIterator<T> extends Iterator<T> {
    void close();

    static <T> CloseableIterator<T> from(Iterator<T> iterator, Runnable closeFn) {
      return new CloseableIterator<T>() {
        public void close() {
          closeFn.run();
        }

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public T next() {
          return iterator.next();
        }
      };
    }
  }

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

    DurableInput spill(BiConsumer<Iterator<T>, DurableOutput> encode) {
      DurableBuffer acc = new DurableBuffer();
      encode.accept(entries(), acc);
      return acc.toInput();
    }
  }

  private static class Accumulator<T> {

    private final BiConsumer<Iterator<T>, DurableOutput> encode;
    private final Function<DurableInput, Iterator<T>> decode;
    private final Comparator<T> comparator;
    private final int maxRealizedElements;

    private LinearList<DurableInput> spilled = new LinearList<>();
    private Chunk<T> curr;

    public Accumulator(BiConsumer<Iterator<T>, DurableOutput> encode,
                       Function<DurableInput, Iterator<T>> decode,
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

    private DurableInput spill(IList<Iterator<T>> iterators) {
      DurableBuffer acc = new DurableBuffer();
      encode.accept(Util.mergeSort(iterators, comparator), acc);
      return acc.toInput();
    }

    public CloseableIterator<T> sortedIterator() {
      IList<DurableInput> bytes = spilled;
      IList<Iterator<T>> iterators = bytes.stream().map(decode).collect(Lists.linearCollector());
      if (curr != null && curr.size > 0) {
        iterators.addLast(curr.entries());
        curr = null;
      }

      while (iterators.size() > maxRealizedElements) {
        IList<DurableInput> merged = new LinearList<>();
        for (int i = 0; i < iterators.size(); i += maxRealizedElements) {
          merged.addLast(spill(iterators.slice(i, Math.min(iterators.size(), i + maxRealizedElements))));
        }
        bytes.forEach(DurableInput::close);
        bytes = merged;
        iterators = merged.stream().map(decode).collect(Lists.linearCollector());
      }

      final IList<DurableInput> resources = bytes;
      return CloseableIterator.from(Util.mergeSort(iterators, comparator), () -> resources.forEach(DurableInput::close));
    }
  }

  ///

  public static <T> CloseableIterator<T> sortedEntries(
      Iterator<T> entries,
      BiConsumer<Iterator<T>, DurableOutput> encode,
      Function<DurableInput, Iterator<T>> decode,
      Comparator<T> comparator,
      int maxRealizedElements) {
    Accumulator<T> acc = new Accumulator<>(encode, decode, comparator, maxRealizedElements);
    entries.forEachRemaining(acc::add);
    return acc.sortedIterator();
  }

  public static <V> CloseableIterator<V> sortedIndexedEntries(ICollection<?, V> c, Comparator<V> comparator) {
    CloseableIterator<Long> it = sortedEntries(
        LongStream.range(0, c.size()).iterator(),
        (l, out) -> l.forEachRemaining(out::writeVLQ),
        in -> Iterators.from(() -> in.remaining() > 0, in::readVLQ),
        (a, b) -> comparator.compare(c.nth(a), c.nth(b)),
        1 << 16);

    return CloseableIterator.from(Iterators.map(it, c::nth), it::close);
  }

  public static <V> CloseableIterator<V> sortedEntries(
      Iterator<V> entries,
      Comparator<V> comparator,
      IDurableEncoding.List listEncoding,
      int maxRealizedElements) {
    return sortedEntries(
        entries,
        (it, out) -> DurableList.encode(it, listEncoding, out),
        in -> (Iterator<V>) DurableList.decode(in.pool(), null, listEncoding).iterator(),
        comparator,
        maxRealizedElements);
  }


}
