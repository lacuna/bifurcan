package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.io.DurableBuffer;

/**
 * A sorted map of int64 onto int64, which assumes keys are appended in sorted order.
 * <p>
 * It begins with a pair of the initial key/value [VLQ, VLQ].
 * <p>
 * This is followed by a sequence of tiers, where each tier is:
 * - the byte length of the tier [UVLQ]
 * - one or more blocks
 * <p>
 * And each block is:
 * - one or more pairs of [UVLQ, VLQ], representing deltas for the key/value pair
 * - if the block is full, a "terminating" pair where the key is set to {@code 0}
 * <p>
 * Tiers are encoded in descending order.  In all tiers but the last, each entry represents a block in the tier beneath it.
 * The value of these entries represents the the byte offset of the terminating pair in the lower tier.  In the lowest tier,
 * the value of each entry represents the actual value associated with the key.
 * <p>
 * In other words, this is a flattened representation of an n-ary tree.  The map {0 0, 1 1, 2 2, 3 3, 4 4}, would be
 * encoded as the bytes [0 0 2 4 3 4 2 3 0 7 8 1 1 0 2 1 1 0 4], which is interpreted as this tree:
 * <code>
 * [0, 0]
 *
 * 2: [4, 3]
 *        └---------------┐
 * 4: [2, 3]          [0, 7]
 *        └-------┐       └-------┐
 * 8: [1, 1], [0, 2], [1, 1], [0, 4]
 * </code>
 * Note that the zeroed out key in each terminating pair doesn't matter, since we already have the key value from the
 * previous tier.
 * <p>
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

  public final DurableInput.Pool in;
  public final int tiers;

  public SkipTable(DurableInput.Pool in, int tiers) {
    this.in = in;
    this.tiers = tiers;
  }

  public Entry floor(long key) {
    DurableInput in = this.in.instance();

    long currKey = in.readVLQ();
    long initOffset = in.readVLQ();
    long nextTier = in.position();
    long currOffset = 0;
    for (int i = 0; i < tiers; i++) {
      // get tier bounds
      in.seek(nextTier);
      long len = in.readUVLQ();
      nextTier = in.position() + len;

      // recalibrate wrt offset on the next tier
      if (currOffset > 0) {
        in.skipBytes(currOffset);
        currOffset = in.readVLQ();

        // if we're at the beginning of the bottom tier, adjust wrt the init offset
      } else if (i == tiers - 1) {
        currOffset = initOffset;
      }

      // scan
      while (in.position() < nextTier) {
        long keyDelta = in.readUVLQ();

        if (keyDelta == 0 || (currKey + keyDelta) > key) {
          break;
        }

        currKey += keyDelta;
        currOffset += in.readVLQ();
      }
    }

    return new Entry(currKey, currOffset);
  }

  ///

  public static class Writer {

    // these values can be changed without affecting decoding
    private static final int LOG_BRANCHING_FACTOR = 5;
    private static final int BRANCHING_FACTOR = 1 << LOG_BRANCHING_FACTOR;
    private static final long BIT_MASK = BRANCHING_FACTOR - 1;

    private final DurableBuffer acc = new DurableBuffer();
    private Writer parent = null;

    private boolean init = false;
    private long firstKey, firstOffset, lastKey, lastOffset, count = 0;

    public Writer() {
    }

    public Writer(long firstKey, long firstOffset) {
      this.firstKey = this.lastKey = firstKey;
      this.firstOffset = this.lastOffset = firstOffset;
      this.init = true;
    }

    public void append(long key, long offset) {
      if (!init) {
        firstKey = lastKey = key;
        firstOffset = lastOffset = offset;
        init = true;
        return;
      }

      if (key <= lastKey) {
        throw new IllegalArgumentException(String.format("keys are out of order, %d <= %d", key, lastKey));
      }

      if ((count & BIT_MASK) == BIT_MASK) {
        if (parent == null) {
          parent = new Writer(firstKey, 0);
        }

        // null terminator
        acc.writeUVLQ(0);

        parent.append(key, acc.written());
        acc.writeVLQ(offset);
      } else {
        acc.writeUVLQ(key - lastKey);
        acc.writeVLQ(offset - lastOffset);
      }

      count++;
      lastKey = key;
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
      out.writeUVLQ(this.acc.written());
      this.acc.flushTo(out);
    }

    public long flushTo(DurableOutput out) {
      assert acc.written() > 0;

      long offset = out.written();
      DurableBuffer.flushTo(out, BlockType.TABLE, acc -> {
        acc.writeVLQ(firstKey);
        acc.writeVLQ(firstOffset);
        flush(acc, 1);
      });
      return out.written() - offset;
    }
  }
}


