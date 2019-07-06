package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.lacuna.bifurcan.allocator.SlabAllocator.free;

public class DurableHashMap {

  public static class Entry implements Comparable<Entry> {
    public final int hash;
    public final Iterable<ByteBuffer> key, value;

    public Entry(int hash, Iterable<ByteBuffer> key, Iterable<ByteBuffer> value) {
      this.hash = hash;
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(Entry o) {
      return Integer.compare(hash, o.hash);
    }
  }

  private static class Chunk<K, V> {

    private final DurableConfig config;
    private final int seed;

    private final IntMap<LinearList<Entry>> entries;
    private int size;

    public Chunk(int seed, DurableConfig config) {
      this.config = config;
      this.seed = seed;

      this.entries = new IntMap<LinearList<Entry>>().linear();
    }

    public void add(K key, V value) throws IOException {
      Iterable<ByteBuffer> k = config.serialize(key);
      Iterable<ByteBuffer> v = config.serialize(value);

      size += Util.size(k) + Util.size(v);
      int hash = config.hash(seed, k.iterator());
      entries.getOrCreate((long) hash, LinearList::new).addLast(new Entry(hash, k, v));
    }

    public int size() {
      return size;
    }

    public DurableInput close() throws IOException {
      ByteBufferWritableChannel acc = new ByteBufferWritableChannel(config.defaultBuffersize);

      ByteChannelDurableOutput out = new ByteChannelDurableOutput(acc, config.defaultBuffersize);
      for (LinearList<Entry> l : entries.values()) {
        for (Entry e : l) {
          out.writeInt(e.hash);
          out.enterBlock(DurableOutput.BlockType.UNCOMPRESSED, false, config,
            () -> out.write(e.key),
            () -> out.write(e.value));
          free(e.key);
          free(e.value);
        }
      }
      out.close();

      return ByteChannelDurableInput.from(acc.buffers(), config.defaultBuffersize);
    }
  }

  /*
  private static Iterator<Entry> spilledEntries(DurableInput in) throws IOException {
    return new Iterator<Entry>() {
      @Override
      public boolean hasNext() {
        return in.remaining() > 0;
      }

      @Override
      public Entry next() {
        try {
          Entry e = new Entry(in.readInt(), in.readBlock(), in.readBlock());
          if (in.remaining() == 0) {
            // once it's exhausted, we don't need it anymore
            in.close();
            Files.delete(path);
          }
          return e;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    };
  }

  public static <K, V> Iterator<Entry> chunkSort(
    IMap<K, V> m,
    ToIntFunction<ByteBuffer> hashFn,
    Function<K, ByteBuffer> keySerializer,
    Function<V, ByteBuffer> valueSerializer,
    int chunkSize,
    int bufferSize) throws IOException {

    LinearList<Iterator<Entry>> iterators = new LinearList<>();
    Chunk chunk = new Chunk(hashFn);
    for (IEntry<K, V> e : m) {
      ByteBuffer
        key = keySerializer.apply(e.key()),
        value = valueSerializer.apply(e.value());

      chunk.add(key, value);
      if (chunk.size() >= chunkSize) {
        iterators.addLast(spilledEntries(chunk.spill(bufferSize), bufferSize));
        chunk = new Chunk(hashFn);
      }
    }

    if (chunk.size > 0) {
      iterators.addLast(chunk.entries());
    }

    return Util.mergeSort(iterators);
  }

  public static void writeDurableMap(
    DurableOutput out,
    HashTable.Writer table,
    Function<ByteBuffer, ByteBuffer> compressor,
    Iterator<Entry> entries,
    int preferredBlockSize) throws IOException {

    CompressedBlockDurableOutput compressedOut =
      new CompressedBlockDurableOutput(out, compressor, preferredBlockSize);

    Entry prev = null;
    long offset = 0;
    while (entries.hasNext()) {
      Entry e = entries.next();

      if (prev == null || prev.hash != e.hash) {
        compressedOut.mark();
        offset = out.written();
      }
      compressedOut.writeBlock(e.key);
      compressedOut.writeBlock(e.value);

      table.put(e.hash, offset);

      prev = e;
    }

    compressedOut.flush();
  }

  */


}
