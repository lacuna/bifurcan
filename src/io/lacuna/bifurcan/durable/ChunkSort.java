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

      return Iterators.onExhaustion(
          Iterators.from(() -> in.remaining() > 0, () -> decode.apply(in)),
          in::close);
    }
  }
  
  public static class Accumulator<T> {

    private final BiConsumer<T, DurableOutput> encode;
    private final Function<DurableInput, T> decode;
    private final Comparator<T> comparator;
    
    private final IList<Iterator<T>> iterators = new LinearList<>();
    private Chunk<T> curr;
    
    public Accumulator(BiConsumer<T, DurableOutput> encode,
                       Function<DurableInput, T> decode,
                       Comparator<T> comparator) {
      this.encode = encode;
      this.decode = decode;
      this.comparator = comparator;
      
      this.curr = new Chunk<>(comparator);
    }
    
    public void add(T x) {
      curr.add(x);
      if (curr.size() >= Chunk.MAX_SIZE) {
        iterators.addLast(curr.spill(encode, decode));
        curr = new Chunk<>(comparator);
      }
    }

    public Iterator<T> sortedIterator() {
      if (curr != null && curr.size > 0) {
        iterators.addLast(curr.entries().iterator());
        curr = null;
      }

      return Util.mergeSort(iterators, comparator);
    }
  }

  public static <T> Iterator<T> sortedEntries(
      Iterable<T> entries,
      BiConsumer<T, DurableOutput> encode,
      Function<DurableInput, T> decode,
      Comparator<T> comparator) {
    Accumulator<T> acc = new Accumulator<>(encode, decode, comparator);
    entries.forEach(acc::add);
    return acc.sortedIterator();
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
