package io.lacuna.bifurcan.durable.encodings;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.SwapBuffer;

import java.util.Arrays;

public class Tuple implements DurableEncoding {

  private final DurableEncoding[] encodings;
  private int blockSize = -1;

  public Tuple(DurableEncoding[] encodings) {
    this.encodings = encodings;
  }

  @Override
  public String description() {
    StringBuffer sb = new StringBuffer("(");
    for (DurableEncoding e : encodings) {
      sb.append(e.description()).append(", ");
    }
    sb.delete(sb.length() - 2, sb.length()).append(")");

    return sb.toString();
  }

  @Override
  public boolean encodesPrimitives() {
    return true;
  }

  @Override
  public int blockSize() {
    if (blockSize == -1) {
      int blockSize = encodings[0].blockSize();
      for (int i = 1; i < encodings.length; i++) {
        blockSize = Math.min(encodings[i].blockSize(), blockSize);
      }
      this.blockSize = blockSize;
    }

    return blockSize;
  }

  @Override
  public void encode(IList<Object> primitives, DurableOutput out) {
    int index = 0;
    for (DurableEncoding e : encodings) {
      final int i = index++;
      SwapBuffer.flushTo(
          out,
          BlockType.OTHER,
          inner -> e.encode(Lists.lazyMap(primitives, t -> ((Object[]) t)[i]), inner));
    }
  }

  @Override
  public SkippableIterator decode(DurableInput in) {
    SkippableIterator[] iterators = new SkippableIterator[encodings.length];
    for (int i = 0; i < encodings.length; i++) {
      iterators[i] = encodings[i].decode(in.sliceBlock(BlockType.OTHER));
    }

    return new SkippableIterator() {
      @Override
      public void skip() {
        for (SkippableIterator it : iterators) {
          it.skip();
        }
      }

      @Override
      public boolean hasNext() {
        return iterators[0].hasNext();
      }

      @Override
      public Object next() {
        Object[] result = new Object[encodings.length];
        for (int i = 0; i < result.length; i++) {
          result[i] = iterators[i].next();
        }
        return result;
      }
    };
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Tuple) {
      return Arrays.equals(encodings, ((Tuple) obj).encodings);
    }
    return false;
  }
}
