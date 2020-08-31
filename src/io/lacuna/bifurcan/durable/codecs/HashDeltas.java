package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.PrimitiveIterator.OfLong;

/**
 * A block representing a sorted sequence of integers:
 * - the initial value [VLQ]
 * - zero or more deltas from the previous value [UVLQs]
 *
 * @author ztellman
 */
public class HashDeltas {

  public static class Writer {
    private final DurableBuffer acc = new DurableBuffer();
    private long prevHash;
    private boolean init = false;

    public Writer() {
    }

    public void append(long hash) {
      if (init) {
        acc.writeUVLQ(hash - prevHash);
      } else {
        init = true;
        acc.writeVLQ(hash);
      }
      prevHash = hash;
    }

    public void flushTo(DurableOutput out) {
      acc.flushTo(out, BlockType.TABLE);
    }
  }

  public static class IndexRange {
    public final int start, end;
    public final boolean isBounded;

    public IndexRange(int start, int end, boolean isBounded) {
      this.start = start;
      this.end = end;
      this.isBounded = isBounded;
    }

    public boolean isEmpty() {
      return start < 0;
    }

    public boolean contains(int n) {
      return start <= n & n < end;
    }

    @Override
    public String toString() {
      return "[" + start + ", " + end + ", " + isBounded + "]";
    }
  }

  public static HashDeltas decode(DurableInput in) {
    return new HashDeltas(in.sliceBlock(BlockType.TABLE).pool());
  }

  public final DurableInput.Pool pool;

  private HashDeltas(DurableInput.Pool pool) {
    this.pool = pool;
  }

  public long nth(long index) {
    OfLong it = iterator();
    Iterators.drop(it, index);
    return it.nextLong();
  }

  public OfLong iterator() {
    DurableInput in = pool.instance();

    return new OfLong() {
      boolean hasNext = true;
      int next = (int) in.readVLQ();

      @Override
      public long nextLong() {
        long result = next;
        if (in.remaining() > 0) {
          next += (int) in.readUVLQ();
        } else {
          hasNext = false;
        }
        return result;
      }

      @Override
      public boolean hasNext() {
        return hasNext;
      }
    };
  }

  public IndexRange candidateIndices(long hash) {
    int start = -1, end = -1;
    OfLong it = iterator();

    for (int i = 0; it.hasNext(); i++) {
      long curr = it.nextLong();
      if (curr == hash) {
        start = i;
        break;
      } else if (curr > hash) {
        return new IndexRange(-1, -1, true);
      }
    }

    for (end = start; it.hasNext(); end++) {
      if (it.nextLong() > hash) {
        return new IndexRange(start, end + 1, true);
      }
    }

    return new IndexRange(start, end + 1, false);
  }
}

