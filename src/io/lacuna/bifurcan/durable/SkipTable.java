package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Bits;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SkipTable {
  private static final int LOG_BRANCHING_FACTOR = 2;
  private static final int BRANCHING_FACTOR = 1 << LOG_BRANCHING_FACTOR;
  private static final long BIT_MASK = BRANCHING_FACTOR - 1;


  public static class Writer {

    private final ByteBufferWritableChannel acc = new ByteBufferWritableChannel(1024);
    private final DurableOutput out = new ByteChannelDurableOutput(acc, 1024);
    private Writer parent = null;

    private long lastIndex = 0, lastOffset = 0, count = 0;

    public void append(long index, long offset) {
      try {
        if ((count & BIT_MASK) == BIT_MASK) {
          if (parent == null) {
            parent = new Writer();
          }

          out.writeVLQ(0);
          parent.append(index, out.written());
          out.writeVLQ(offset);
        } else {
          out.writeVLQ(index - lastIndex);
          out.writeVLQ(offset - lastOffset);
        }
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }

      count++;
      lastIndex = index;
      lastOffset = offset;
    }

    private void flush(DurableOutput out, int level) throws IOException {
      if (parent == null) {
        out.writeVLQ(level);
      } else {
        parent.flush(out, level + 1);
      }
      this.out.close();
      out.writeVLQ(this.out.written());
      out.write(this.acc.buffers());
    }

    public Iterable<ByteBuffer> buffers() {
      return DurableOutput.capture(out -> flush(out, 1));
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
      while(in.position() < nextTier) {
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

