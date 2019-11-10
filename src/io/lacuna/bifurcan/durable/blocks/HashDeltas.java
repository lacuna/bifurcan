package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.DurableAccumulator;

public class HashDeltas {

  public static class Writer {
    private final DurableAccumulator acc = new DurableAccumulator();
    private int prevHash;
    private boolean init = false;

    public Writer() {
    }

    public void append(int hash) {
      if (init) {
        acc.writeVLQ(hash - prevHash);
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

  public static Reader decode(DurableInput in) {
    return new Reader(in.sliceBlock(BlockType.TABLE));
  }

  public static class Reader {

    public final DurableInput in;

    private Reader(DurableInput in) {
      this.in = in;
    }

    public IndexRange candidateIndices(int hash) {
      DurableInput in = this.in.duplicate();

      int start = -1, end = -1;
      int currHash = in.readInt();

      int i = 0;
      if (currHash == hash) {
        start = end = 0;
      } else {
        for (i = 1; in.remaining() > 0; i++) {
          currHash += in.readVLQ();
          if (currHash == hash) {
            start = i;
            break;
          }
        }
      }

      for (; in.remaining() > 0; end++) {
        currHash += in.readVLQ();
        if (currHash > hash) {
          return new IndexRange(start, end, true);
        }
      }

      return new IndexRange(start, end + 1, false);
    }
  }
}
