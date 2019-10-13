package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.allocator.SlabAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * @author ztellman
 */
public class HashTable {

  private static final int ENTRY_SIZE = 10;

  public static class Entry {
    // int32
    public final int hash;

    // uint48
    public final long offset;

    public Entry(int hash, long offset) {
      this.hash = hash;
      this.offset = offset;
    }

    boolean isEmpty() {
      return hash == 0 & offset == 0;
    }

    @Override
    public String toString() {
      return "[hash=" + hash + ", offset=" + offset + "]";
    }
  }


  public static class Writer {

    private final IntMap<ByteBuffer> buffers;
    private final long size;

    public Writer(Iterable<ByteBuffer> buffers) {
      IntMap<ByteBuffer> m = new IntMap<ByteBuffer>().linear();
      long size = 0;
      for (ByteBuffer b : buffers) {
        m.put(size, b);
        size += b.remaining() / ENTRY_SIZE;
      }

      this.buffers = m.forked();
      this.size = size;
    }

    public IList<ByteBuffer> buffers() {
      return buffers.values();
    }

    private ByteBuffer buffer(long idx) {
      IEntry<Long, ByteBuffer> e = buffers.floor(idx);
      return (ByteBuffer) e.value().duplicate().position((int) (idx - e.key()) * ENTRY_SIZE);
    }

    public void put(int hash, long offset) {
      put(new Entry(hash, offset));
    }

    public void put(Entry e) {
      long idx = estimatedIndex(e, size);
      ByteBuffer buf = buffer(idx);
      for (long dist = 0; ; idx++, dist++) {
        if (buf.remaining() == 0) {
          idx = wrap(idx, size);
          buf = buffer(idx);
        }

        Entry curr = read(buf);
        if (curr.isEmpty()) {
          overwrite(buf, e);
          break;
        } else if (curr.hash == e.hash) {
          throw new IllegalStateException();
        }

        long currDist = probeDistance(curr, idx, size);
        if (dist > currDist) {
          overwrite(buf, e);
          e = curr;
          dist = currDist;
        }
      }
    }
  }

  public Writer create(long entries, double loadFactor) throws IOException {
    return new Writer(SlabAllocator.allocate(requiredBytes(entries, loadFactor)));
  }

  public static Entry get(DurableInput in, int hash) throws IOException {
    long entries = in.remaining() / ENTRY_SIZE;

    long offset = in.position();
    long idx = estimatedIndex(hash, entries);
    in.seek(idx * ENTRY_SIZE);
    for (long dist = 0; ; idx++, dist++) {
      if (in.remaining() == 0) {
        in.seek(offset);
      }

      Entry curr = read(in);
      if (curr.isEmpty()) {
        return null;
      } else if (curr.hash == hash) {
        return curr;
      } else if (dist > probeDistance(curr, idx, entries)) {
        return null;
      }
    }
  }

  public static long requiredBytes(long entries, double loadFactor) {
    return (int) Math.ceil(entries / loadFactor) * ENTRY_SIZE;
  }

  ///

  public static IList<Entry> entries(DurableInput in) throws IOException {
    in.seek(0);
    LinearList<Entry> entries = new LinearList<>();
    while (in.remaining() > 0) {
      entries.addLast(read(in));
    }
    return entries;
  }

  private static Entry read(ByteBuffer buf) {
    int
      a = buf.getInt();

    long
      b = buf.getInt() & 0xFFFFFFFFL,
      c = buf.getShort() & 0xFFFF;

    return new Entry(a, (b << 16) | c);
  }

  private static Entry read(DurableInput in) throws IOException {
    int
      a = in.readInt();

    long
      b = in.readInt() & 0xFFFFFFFFL,
      c = in.readShort() & 0xFFFF;

    return new Entry(a, (b << 16) | c);
  }

  private static void overwrite(ByteBuffer buf, Entry e) {
    buf.position(buf.position() - ENTRY_SIZE);

    buf.putInt(e.hash);
    buf.putInt((int) (e.offset >> 16));
    buf.putShort((short) (e.offset & 0xFFFF));
  }

  private static long estimatedIndex(Entry e, long entries) {
    return estimatedIndex(e.hash, entries);
  }

  private static long estimatedIndex(int hash, long entries) {
    return hash % entries;
  }

  private static long probeDistance(Entry val, long idx, long entries) {
    return wrap(idx + entries - (val.hash % entries), entries);
  }

  private static long wrap(long idx, long entries) {
    return idx >= entries ? (idx - entries) : idx;
  }
}
