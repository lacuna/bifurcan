package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.allocator.SlabAllocator;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.DurableAccumulator;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.utils.Bits;

import java.nio.ByteBuffer;
import java.util.function.LongFunction;

import static io.lacuna.bifurcan.allocator.SlabAllocator.free;

/**
 * @author ztellman
 */
public class HashTable {

  public static final int NONE = 0;
  public static final int FALLBACK = -1;

  public static class Entry {

    public static final Entry ENTRY = new Entry(0, 0);

    // int32
    public final int hash;

    // up to 64 bits
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

    private final DurableAccumulator acc = new DurableAccumulator(16384);
    private final double loadFactor;

    private long count, maxOffset;

    public Writer(double loadFactor) {
      this.loadFactor = loadFactor;
    }

    public void put(int hash, long offset) {
      assert (hash != NONE);

      acc.writeInt(hash);
      acc.writeLong(offset);
      count++;
      maxOffset = Math.max(maxOffset, offset);
    }

    public int entryBytes() {
      return maxOffset == 0 ? 0 : 4 + offsetBytes();
    }

    private int offsetBytes() {
      int highestBit = Bits.bitOffset(Bits.highestBit(maxOffset));
      return (int) Math.ceil((highestBit + 1) / 8.0);
    }

    private long tableSize() {
      return (long) Math.ceil(count / loadFactor) * entryBytes();
    }

    /**
     * Used for testing, in practice you should always prefer `flushTo` since it automatically frees the buffers.
     */
    public Iterable<ByteBuffer> contents() {
      int entryBytes = entryBytes();
      assert (entryBytes > 0);

      int maxBufferSize = (Integer.MAX_VALUE / entryBytes) * entryBytes;

      long tableSize = tableSize();
      long allocated = 0;
      IntMap<ByteBuffer> m = new IntMap<ByteBuffer>().linear();
      while (allocated < tableSize) {
        ByteBuffer buf = SlabAllocator.allocate((int) Math.min(tableSize - allocated, maxBufferSize));
        m.put(allocated, buf);
        allocated += buf.remaining();
      }

      DurableInput entries = DurableInput.from(acc.contents());
      long numEntries = tableSize / entryBytes;
      for (int i = 0; i < count; i++) {
        int hash = entries.readInt();
        long offset = entries.readLong();
        put(new Entry(hash, offset), idx -> buffer(m, entryBytes, idx), numEntries, entryBytes);
      }

      SlabAllocator.free(acc.contents());

      return m.values();
    }

    public long flushTo(DurableOutput out) {
      long offset = out.written();

      Iterable<ByteBuffer> bufs = contents();
      BlockPrefix.write(new BlockPrefix(Util.size(bufs), BlockPrefix.BlockType.TABLE), out);
      out.write(bufs);
      free(bufs);

      return out.written() - offset;
    }

    private static ByteBuffer buffer(IntMap<ByteBuffer> buffers, int entryBytes, long idx) {
      IEntry<Long, ByteBuffer> e = buffers.floor(idx);
      return (ByteBuffer) e.value().duplicate().position((int) (idx - e.key()) * entryBytes);
    }

    private void put(Entry e, LongFunction<ByteBuffer> buffer, long numEntries, int entryBytes) {
      long idx = estimatedIndex(e, numEntries);
      ByteBuffer buf = buffer.apply(idx);
      for (long dist = 0; ; idx++, dist++) {
        if (buf.remaining() == 0) {
          idx = wrap(idx, numEntries);
          buf = buffer.apply(idx);
        }

        Entry curr = read(buf, entryBytes);
        if (curr.isEmpty()) {
          overwrite(buf, e, entryBytes);
          break;
        } else if (curr.hash == e.hash) {
          if (e.offset < curr.offset) {
            overwrite(buf, e, entryBytes);
          }
          return;
        }

        long currDist = probeDistance(curr, idx, numEntries);
        if (dist > currDist) {
          overwrite(buf, e, entryBytes);
          e = curr;
          dist = currDist;
        }
      }
    }
  }

  public static class Reader {

    public final DurableInput in;
    public final long numEntries;
    public final int entryBytes;

    public Reader(DurableInput in, int entryBytes) {
      this.in = in;
      this.entryBytes = entryBytes;
      this.numEntries = in.size() / entryBytes;
    }

    public Entry get(int hash) {
      DurableInput in = this.in.duplicate();

      long idx = estimatedIndex(hash, numEntries);
      in.seek(idx * entryBytes);
      for (long dist = 0; ; idx++, dist++) {
        if (in.remaining() == 0) {
          in.seek(0);
        }

        Entry curr = read(in, entryBytes);
        if (curr.isEmpty()) {
          return null;
        } else if (curr.hash == hash) {
          return curr;
        } else if (dist > probeDistance(curr, idx, numEntries)) {
          return null;
        }
      }
    }

    public IList<Entry> entries() {
      in.seek(0);
      LinearList<Entry> entries = new LinearList<>();
      while (in.remaining() > 0) {
        entries.addLast(read(in, entryBytes));
      }
      return entries;
    }
  }

  ///

  private static Entry read(ByteBuffer buf, int entryBytes) {
    int hash = buf.getInt();

    long offset = 0;
    int offsetBytes = entryBytes - 4;
    for (int i = 0; i < offsetBytes; i++) {
      offset = (offset << 8) | (buf.get() & 0xFF);
    }

    return new Entry(hash, offset);
  }

  private static Entry read(DurableInput in, int entryBytes) {
    int hash = in.readInt();

    long offset = 0;
    int offsetBytes = entryBytes - 4;
    for (int i = 0; i < offsetBytes; i++) {
      offset = (offset << 8) | in.readUnsignedByte();
    }

    return new Entry(hash, offset);
  }

  private static void overwrite(ByteBuffer buf, Entry e, int entryBytes) {
    buf.position(buf.position() - entryBytes);

    buf.putInt(e.hash);
    for (int i = entryBytes - 5; i >= 0; i--) {
      buf.put((byte) ((e.offset >> (i * 8)) & 0xFF));
    }
  }

  private static long estimatedIndex(Entry e, long entries) {
    return estimatedIndex(e.hash, entries);
  }

  private static long estimatedIndex(int hash, long entries) {
    return Math.abs(hash % entries);
  }

  private static long probeDistance(Entry val, long idx, long entries) {
    return wrap(idx + entries - (val.hash % entries), entries);
  }

  private static long wrap(long idx, long entries) {
    return idx >= entries ? (idx - entries) : idx;
  }
}
