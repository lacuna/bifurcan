package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.DurableAccumulator;

import java.nio.ByteBuffer;

import static io.lacuna.bifurcan.durable.BlockPrefix.BlockType.TABLE;

public class SkipTable {
  private static final int LOG_BRANCHING_FACTOR = 5;
  private static final int BRANCHING_FACTOR = 1 << LOG_BRANCHING_FACTOR;
  private static final long BIT_MASK = BRANCHING_FACTOR - 1;

  public static class Writer {

    private final DurableAccumulator acc = new DurableAccumulator(1024);
    private Writer parent = null;

    private long lastIndex = 0, lastOffset = 0, count = 0;

    public void append(long index, long offset) {
      if (index == 0) {
        assert offset == 0;
        return;
      }

      if ((count & BIT_MASK) == BIT_MASK) {
        if (parent == null) {
          parent = new Writer();
        }

        // null terminator
        acc.writeVLQ(0);

        parent.append(index, acc.written());
        acc.writeVLQ(offset);
      } else {
        acc.writeVLQ(index - lastIndex);
        acc.writeVLQ(offset - lastOffset);
      }

      count++;
      lastIndex = index;
      lastOffset = offset;
    }

    public int tiers() {
      if (acc.written() == 0) {
        return 0;
      } else {
        int tiers = 1;
        Writer parent = this.parent;
        while (parent != null) {
          tiers++;
          parent = parent.parent;
        }

        return tiers;
      }
    }

    private void flush(DurableOutput out, int tier) {
      if (parent != null) {
        parent.flush(out, tier + 1);
      }
      out.writeVLQ(this.acc.written());
      this.acc.flushTo(out);
    }

    /**
     * Used for testing, in practice you should always prefer `flushTo` since it automatically frees the buffers.
     */
    public Iterable<ByteBuffer> contents() {
      DurableAccumulator acc = new DurableAccumulator();
      flushTo(acc);
      return acc.contents();
    }

    public long flushTo(DurableOutput out) {
      long offset = out.written();
      DurableAccumulator.flushTo(out, BlockType.TABLE, acc -> flush(acc, 1));
      return out.written() - offset;
    }
  }

  public static class Entry {
    public static final Entry ENTRY = new Entry(0, 0);

    public final long index, offset;

    public Entry(long index, long offset) {
      this.index = index;
      this.offset = offset;
    }

    @Override
    public String toString() {
      return "[" + index + ", " + offset + "]";
    }

  }

  public static class Reader {

    public final DurableInput in;
    public final int tiers;

    public Reader(DurableInput in, int tiers) {
      this.in = in;
      this.tiers = tiers;
    }

    public Entry floor(long index) {
      DurableInput in = this.in.duplicate().seek(0);

      long currIndex = 0, currOffset = 0, nextTier = 0;
      for (int i = 0; i < tiers; i++) {

        // get tier bounds
        in.seek(nextTier);
        long len = in.readVLQ();
        nextTier = in.position() + len;

        // recalibrate wrt offset on the next tier
        if (currOffset > 0) {
          in.skipBytes(currOffset);
          currOffset = in.readVLQ();
        }

        // scan
        while (in.position() < nextTier) {
          long indexDelta = in.readVLQ();

          if (indexDelta == 0 || (currIndex + indexDelta) > index) {
            break;
          }

          currIndex += indexDelta;
          currOffset += in.readVLQ();
        }
      }

      return new Entry(currIndex, currOffset);
    }
  }
}

