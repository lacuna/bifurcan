package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.io.DurableBuffer;

import java.util.Comparator;
import java.util.OptionalLong;

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
 * encoded as the bytes [0 0 2 4 3 4 2 3 0 7 8 1 1 0 2 1 1 0 4], which is interpreted as this binary tree (note that in
 * practice we use a 32-ary tree):
 * <code>
 * [0, 0]
 * <p>
 * 2: [4, 3]
 *        └---------------┐
 * 4: [2, 3]          [0, 7]
 *        └-------┐       └-------┐
 * 8: [1, 1], [0, 2], [1, 1], [0, 4]
 * </code>
 * Note that the zeroed out key in each terminating pair doesn't matter, since we already have the key value from the
 * previous tier.
 * <p>
 *
 * @author ztellman
 */
public class SkipTable {

  private static final int LOG_BRANCHING_FACTOR = 1;
  private static final int BRANCHING_FACTOR = 1 << LOG_BRANCHING_FACTOR;
  private static final long BIT_MASK = BRANCHING_FACTOR - 1;

  public static class Entry {
    public final long key, value;

    public Entry(long key, long value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String toString() {
      return "[" + key + ", " + value + "]";
    }

  }

  public final DurableInput.Pool in;

  private SkipTable(DurableInput.Pool in) {
    this.in = in;
  }

  public static ISortedMap<Long, Long> decode(IDurableCollection.Root root, DurableInput in) {
    DurableInput block = in.sliceBlock(BlockType.TABLE);
    return new SkipTable((root == null ? block : root.cached(block)).pool()).toSortedMap();
  }

  private Reader reader() {
    return new Reader();
  }

  private Entry floor(long key) {
    Reader r = reader();
    for (; ; ) {
      if (r.key > key) {
        if (r.tier == 0) {
          return r.prevEntry();
        } else {
          r.descendPrev();
        }
      } else if (!r.hasNext() || r.key == key) {
        if (r.tier == 0) {
          return r.entry();
        } else {
          r.descend();
        }
      } else {
        r.next();
      }
    }
  }

  private OptionalLong floorIndex(long key) {
    Reader r = reader();
    if (key < r.key) {
      return OptionalLong.empty();
    }

    for (; ; ) {
      if (r.key > key) {
        if (r.tier == 0) {
          return OptionalLong.of(r.idx - 1);
        } else {
          r.descendPrev();
        }
      } else if (!r.hasNext() || r.key == key) {
        if (r.tier == 0) {
          return OptionalLong.of(r.idx);
        } else {
          r.descend();
        }
      } else {
        r.next();
      }
    }
  }

  private long size() {
    Reader r = reader();
    while (r.tier > 0 || r.hasNext()) {
      if (r.hasNext()) {
        r.next();
      } else {
        r.descend();
      }
    }
    return r.idx + 1;
  }

  private Entry nth(long idx) {
    Reader r = reader();
    while (r.idx < idx || r.tier > 0) {
      if ((idx - r.idx) >= r.step) {
        r.next();
      } else {
        r.descend();
      }
    }
    return r.entry();
  }

  private ISortedMap<Long, Long> toSortedMap() {
    if (in.instance().size() == 0) {
      return SortedMap.empty();
    } else {
      IList<Long> elements = Lists.from(size(), i -> nth(i).key);
      ISortedSet<Long> keys = Sets.from(elements, Comparator.naturalOrder(), this::floorIndex);
      return Maps.from(keys, k -> floor(k).value);
    }
  }

  ///

  private class Reader {
    private final DurableInput in = SkipTable.this.in.instance();

    int tier;
    long idx, key, value;

    private long prevKey, prevValue, nextTier, step;
    private final long initValue;

    public Reader() {
      tier = in.readUnsignedByte();
      prevKey = key = in.readVLQ();
      initValue = in.readVLQ();
      nextTier = in.position();

      step = 1L << tier;
      if (tier > 0) {
        descend();
      } else {
        value = initValue;
      }
    }

    /**
     * Returns true if there is another entry in this "chunk".
     */
    boolean hasNext() {
      return in.position() < nextTier;
    }

    /**
     * Advances to the next entry on this level, if it exists.
     */
    void next() {
      assert hasNext();
      prevKey = key;
      prevValue = value;

      key += in.readUVLQ();
      if (prevKey != key) {
        idx += step;
        value += in.readVLQ();
      } else {
        // we've hit the end of this chunk, make sure hasNext() return false
        in.seek(nextTier);
      }
    }

    /**
     * Descend to the next tier.
     */
    void descend() {
      assert tier > 0;
      tier--;
      step >>= LOG_BRANCHING_FACTOR;

      // get tier bounds
      in.seek(nextTier);
      long len = in.readUVLQ();
      nextTier = in.position() + len;

      // recalibrate wrt offset on the next tier
      if (value > 0) {
        in.skipBytes(value);
        prevValue = value = in.readVLQ();
        prevKey = key;
      } else if (tier == 0) {
        prevValue = value = initValue;
      }
    }

    /**
     * Move back to the previous entry, and down to the next tier.
     */
    void descendPrev() {
      key = prevKey;
      value = prevValue;
      idx -= step;
      descend();
    }

    Entry entry() {
      assert tier == 0;
      return new Entry(key, value);
    }

    Entry prevEntry() {
      assert tier == 0;
      return key == prevKey ? null : new Entry(prevKey, prevValue);
    }
  }

  ///

  public static class Writer {

    private final DurableBuffer acc = new DurableBuffer();
    private Writer parent = null;

    private boolean init = false;
    private long firstKey, firstValue, lastKey, lastValue, count = 0;

    public Writer() {
    }

    public Writer(long firstKey, long firstValue) {
      this.firstKey = this.lastKey = firstKey;
      this.firstValue = this.lastValue = firstValue;
      this.init = true;
    }

    public void append(long key, long value) {
      if (!init) {
        firstKey = lastKey = key;
        firstValue = lastValue = value;
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
        acc.writeVLQ(value);
      } else {
        acc.writeUVLQ(key - lastKey);
        acc.writeVLQ(value - lastValue);
      }

      count++;
      lastKey = key;
      lastValue = value;
    }

    public void free() {
      if (parent != null) {
        parent.free();
      }
      acc.free();
    }

    private int tiers() {
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
      long offset = out.written();
      DurableBuffer.flushTo(out, BlockType.TABLE, acc -> {
        if (init) {
          acc.writeUnsignedByte(tiers());
          acc.writeVLQ(firstKey);
          acc.writeVLQ(firstValue);
          flush(acc, 1);
        } else {
          free();
        }
      });
      return out.written() - offset;
    }

    public ISortedMap<Long, Long> toOffHeapMap() {
      DurableBuffer tmp = new DurableBuffer();
      flushTo(tmp);
      return SkipTable.decode(null, tmp.toOffHeapInput());
    }
  }
}


