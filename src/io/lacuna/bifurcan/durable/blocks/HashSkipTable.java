package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;

import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

public class HashSkipTable {

  private static final long HASH_OFFSET = -((long) Integer.MIN_VALUE);

  public static class Entry {
    public static final HashSkipTable.Entry ORIGIN = new HashSkipTable.Entry(0, 0);

    public final int hash;
    public final long offset;

    public Entry(int hash, long offset) {
      this.hash = hash;
      this.offset = offset;
    }

    @Override
    public String toString() {
      return "[" + hash + ", " + offset + "]";
    }

  }

  private final SkipTable table;

  public HashSkipTable(DurableInput.Pool in, int tiers) {
    this.table = new SkipTable(in, tiers);
  }

  public Entry floor(int hash) {
    SkipTable.Entry e = table.floor((long) hash + HASH_OFFSET);
    return e != null
        ? new Entry((int) (e.index - HASH_OFFSET), e.offset)
        : null;
  }

  public static class Writer {

    private long prevHash = 0;

    private final SkipTable.Writer table = new SkipTable.Writer();

    public void append(IntStream hashes, long offset) {
      int[] ary = hashes.toArray();
      PrimitiveIterator.OfLong it = IntStream.of(ary).mapToLong(n -> ((long) n) + HASH_OFFSET).iterator();
      long h = it.nextLong();
      if (prevHash > h) {
        System.out.println(ary[0]);
        throw new IllegalStateException(String.format("out-of-order hashes: %d > %d", prevHash, h));
      }

      if (h != prevHash) {
        prevHash = h;
        table.append(h, offset);
      }

      while (it.hasNext()) {
        prevHash = it.nextLong();
      }
    }

    public int tiers() {
      return table.tiers();
    }

    public void flushTo(DurableOutput out) {
      table.flushTo(out);
    }

    public void free() {
      table.free();
    }
  }


}
