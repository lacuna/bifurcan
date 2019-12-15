package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.LongStream;

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

    IList<T> entries() {
      return entries.values().stream().flatMap(IList::stream).collect(Lists.linearCollector());
    }

    Iterator<T> spill(BiConsumer<IList<T>, DurableOutput> encode, Function<DurableInput, Iterator<T>> decode) {
      SwapBuffer acc = new SwapBuffer();
      encode.accept(entries(), acc);
      DurableInput in = DurableInput.from(acc.contents());

      return Iterators.onExhaustion(decode.apply(in), in::close);
    }
  }
  
  public static class Accumulator<T> {

    private final BiConsumer<IList<T>, DurableOutput> encode;
    private final Function<DurableInput, Iterator<T>> decode;
    private final Comparator<T> comparator;
    private final int maxRealizedElements;
    
    private final LinearList<Iterator<T>> iterators = new LinearList<>();
    private Chunk<T> curr;
    
    public Accumulator(BiConsumer<IList<T>, DurableOutput> encode,
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
        iterators.addLast(curr.spill(encode, decode));
        curr = new Chunk<>(comparator);
      }
    }

    public Iterator<T> sortedIterator() {
      if (curr != null && curr.size > 0) {
        iterators.addLast(curr.entries().iterator());
        curr = null;
      }

      LinearList<Iterator<T>> iterators = this.iterators;
      while (iterators.size() > maxRealizedElements) {
        LinearList<Iterator<T>> merged = new LinearList<>();
        for (int i = 0; i < iterators.size(); i += maxRealizedElements) {
          merged.addLast(Util.mergeSort(iterators.slice(i, Math.min(iterators.size(), i + maxRealizedElements)), comparator));
        }
        iterators = merged;
      }

      return Util.mergeSort(iterators, comparator);
    }
  }

  ///

  public static <T> Iterator<T> sortedEntries(
      Iterator<T> entries,
      BiConsumer<IList<T>, DurableOutput> encode,
      Function<DurableInput, Iterator<T>> decode,
      Comparator<T> comparator,
      int maxRealizedElements) {
    Accumulator<T> acc = new Accumulator<>(encode, decode, comparator, maxRealizedElements);
    entries.forEachRemaining(acc::add);
    return acc.sortedIterator();
  }

  public static <V> Iterator<V> sortedIndexedEntries(ICollection<?, V> c, Comparator<V> comparator) {
    return Iterators.map(
        sortedEntries(
            LongStream.range(0, c.size()).iterator(),
            (l, out) -> l.forEach(out::writeVLQ),
            in -> Iterators.from(() -> in.remaining() > 0, in::readVLQ),
            (a, b) -> comparator.compare(c.nth(a), c.nth(b)),
            1 << 16),
        c::nth);
  }

  public static <V> Iterator<V> sortedEntries(
      Iterator<V> entries,
      Comparator<V> comparator,
      IDurableEncoding.List listEncoding,
      int maxRealizedElements) {
    return sortedEntries(
        entries,
        (l, out) -> io.lacuna.bifurcan.durable.blocks.List.encode(l.iterator(), listEncoding, out),
        in -> (Iterator<V>) io.lacuna.bifurcan.durable.blocks.List.decode(in, null, listEncoding).iterator(),
        comparator,
        maxRealizedElements);
  }


}
