package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SkipTable {
  private static final int LOG_BRANCHING_FACTOR = 2;
  private static final int BRANCHING_FACTOR = 1 << LOG_BRANCHING_FACTOR;
  private static final long BIT_MASK = BRANCHING_FACTOR - 1;


  public static class Writer {

    private final DurableAccumulator acc = new DurableAccumulator(1024);
    private Writer parent = null;

    private long lastIndex = 0, lastOffset = 0, count = 0;

    public void append(long index, long offset) {
      if ((count & BIT_MASK) == BIT_MASK) {
        if (parent == null) {
          parent = new Writer();
        }

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

    private void flush(DurableOutput out, int level) {
      if (parent == null) {
        out.writeVLQ(level);
      } else {
        parent.flush(out, level + 1);
      }
      this.acc.close();
      out.writeVLQ(this.acc.written());
      out.write(this.acc.contents());
    }

    public Iterable<ByteBuffer> contents() {
      DurableAccumulator acc = new DurableAccumulator();
      flushTo(acc);
      return acc.contents();
    }

    public void flushTo(DurableOutput out) {
      flush(out, 1);
      acc.flushTo(out);
    }
  }

  public static class Entry {
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

  public static Entry lookup(DurableInput in, long index) throws IOException {
    int tiers = (int) in.readVLQ();

    long currIndex = 0, currOffset = 0;
    long nextTier = in.position();
    for (int i = 0; i < tiers; i++) {

      // get tier bounds
      in.seek(nextTier);
      long len = in.readVLQ();
      nextTier = in.position() + len;

      // recalibrate wrt offset on the next tier
      if (currOffset > 0) {
        in.skip(currOffset);
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

