package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.DurableBuffer;

/**
 * A sorted map of integer onto integer, which assumes that both keys and values are monotonically increasing.
 *
 * It is encoded as a sequence of tiers, where each tier is:
 * - the byte length of the tier [VLQ]
 * - one or more blocks
 *
 * And each block is:
 * - one or more pairs of VLQs, representing deltas for the key/value pair
 * - if the block is full, a "terminating" pair where the key is set to {@code 0}
 *
 * Tiers are encoded in descending order.  In all tiers but the last, each entry represents a block in the tier beneath it.
 * The value of these entries represents the the byte offset of the terminating pair in the lower tier.  In the lowest tier,
 * the value of each entry represents the actual value associated with the key.
 *
 * In other words, this is a flattened representation of an n-ary tree.  The map {0 0, 1 1, 2 2, 3 3, 4 4}, would be
 * encoded as the bytes [2 4 3 4 2 3 0 7 8 1 1 0 2 1 1 0 4], which is interpreted as this tree:
 *
 * 2: [4, 3]
 *        └---------------┐
 * 4: [2, 3]          [0, 7]
 *        └-------┐       └-------┐
 * 8: [1, 1], [0, 2], [1, 1], [0, 4]
 *
 * Note that the {0 0} entry is implicit.  Also note that the zeroed out key in each terminating pair doesn't matter,
 * since we already have the key value from the previous tier.
 *
 * In practice, we use a 32-ary tree, but this can be configured on the write-side without any repercussions on the
 * read-side.
 *
 * @author ztellman
 */
public class SkipTable {

  public static class Entry {
    public static final Entry ORIGIN = new Entry(0, 0);

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

  public final DurableInput in;
  public final int tiers;

  public SkipTable(DurableInput in, int tiers) {
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

  ///

  public static class Writer {

    // these values can be changed without affecting decoding
    private static final int LOG_BRANCHING_FACTOR = 5;
    private static final int BRANCHING_FACTOR = 1 << LOG_BRANCHING_FACTOR;
    private static final long BIT_MASK = BRANCHING_FACTOR - 1;

    private final DurableBuffer acc = new DurableBuffer();
    private Writer parent = null;

    private long lastIndex = 0, lastOffset = 0, count = 0;

    public void append(long index, long offset) {
      if (index == 0) {
        assert offset == 0;
        return;
      }

      assert (index > lastIndex & offset >= lastOffset);

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

    public void free() {
      if (parent != null) {
        parent.free();
      }
      acc.free();
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

    public long flushTo(DurableOutput out) {
      long offset = out.written();
      DurableBuffer.flushTo(out, BlockType.TABLE, acc -> flush(acc, 1));
      return out.written() - offset;
    }
  }
}


