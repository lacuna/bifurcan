package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Iterators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.ToIntFunction;

public class DurableHashMap {

  private static class Entry implements Comparable<Entry> {
    public final int hash;
    public final long index;

    public Entry(int hash, long index) {
      this.hash = hash;
      this.index = index;
    }

    @Override
    public int compareTo(Entry o) {
      return Integer.compare(hash, o.hash);
    }
  }

  private static class Chunk {

    static final int BUFFER_SIZE = 1 << 20;
    static final int MAX_SIZE = 1 << 16;

    private final IntMap<LinearList<Entry>> entries;
    private int size;

    public Chunk() {
      this.entries = new IntMap<LinearList<Entry>>().linear();
    }

    public void add(int hash, long index) {
      entries.getOrCreate((long) hash, LinearList::new).addLast(new Entry(hash, index));
      size++;
    }

    public int size() {
      return size;
    }

    public DurableInput close() throws IOException {
      Iterable<ByteBuffer> buffers = DurableOutput.capture(BUFFER_SIZE, out -> {
        for (LinearList<Entry> l : entries.values()) {
          for (Entry e : l) {
            out.writeInt(e.hash);
            out.writeVLQ(e.index);
          }
        }
      });

      return ByteChannelDurableInput.from(buffers, BUFFER_SIZE);
    }

    public Iterator<Entry> entries() {
      return entries.values().stream().flatMap(IList::stream).iterator();
    }
  }

  private static Iterator<Entry> spilledEntries(DurableInput in) throws IOException {
    return new Iterator<Entry>() {
      @Override
      public boolean hasNext() {
        return in.remaining() > 0;
      }

      @Override
      public Entry next() {
        try {
          Entry e = new Entry(in.readInt(), in.readVLQ());
          if (in.remaining() == 0) {
            // once it's exhausted, we don't need it anymore
            in.close();
          }
          return e;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    };
  }


  public static <K, V> Iterator<IEntry<K, V>> sortEntries(IMap<K, V> m, ToIntFunction<K> hashFn) throws IOException {
    LinearList<Iterator<Entry>> iterators = new LinearList<>();
    Chunk chunk = new Chunk();

    int idx = 0;
    for (IEntry<K, V> e : m.entries()) {
      chunk.add(hashFn.applyAsInt(e.key()), idx++);
      if (chunk.size() >= Chunk.MAX_SIZE) {
        iterators.addLast(spilledEntries(chunk.close()));
        chunk = new Chunk();
      }
    }

    if (chunk.size > 0) {
      iterators.addLast(chunk.entries());
    }

    return Iterators.map(Util.mergeSort(iterators), e -> m.nth(e.index));
  }
}
