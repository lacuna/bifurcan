package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.DurableAccumulator;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfInt;

/**
 * A block representing a sorted sequence of 32-bit integers:
 * - the initial value [int32]
 * - zero or more deltas from the previous value [VLQs]
 *
 * @author ztellman
 */
public class HashDeltas {

  public static class Writer {
    private final DurableAccumulator acc = new DurableAccumulator();
    private int prevHash;
    private boolean init = false;

    public Writer() {
    }

    public void append(int hash) {
      if (init) {
        acc.writeVLQ((long) hash - (long) prevHash);
      } else {
        init = true;
        acc.writeInt(hash);
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
    return new HashDeltas(in.sliceBlock(BlockType.TABLE));
  }

  public final DurableInput in;

  private HashDeltas(DurableInput in) {
    this.in = in;
  }

  public int nth(long index) {
    OfInt it = iterator();
    Iterators.drop(it, index);
    return it.nextInt();
  }

  public OfInt iterator() {
    DurableInput in = this.in.duplicate().seek(0);

    return new OfInt() {
      boolean hasNext = true;
      int next = in.readInt();

      @Override
      public int nextInt() {
        int result = next;
        if (in.remaining() > 0) {
          next += (int) in.readVLQ();
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

  public IndexRange candidateIndices(int hash) {
    int start = -1, end = -1;
    OfInt it = iterator();

    int currHash = Integer.MIN_VALUE;
    for (int i = 0; currHash < hash && it.hasNext(); i++) {
      if (it.nextInt() == hash) {
        start = i;
        break;
      }
    }

    for (end = start; it.hasNext(); end++) {
      if (it.nextInt() > hash) {
        return new IndexRange(start, end + 1, true);
      }
    }

    return new IndexRange(start, end + 1, false);
  }
}

