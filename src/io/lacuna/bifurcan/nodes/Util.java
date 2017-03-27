package io.lacuna.bifurcan.nodes;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

import static io.lacuna.bifurcan.utils.Bits.bitOffset;
import static io.lacuna.bifurcan.utils.Bits.highestBit;
import static io.lacuna.bifurcan.utils.Bits.lowestBit;
import static java.lang.Integer.bitCount;

/**
 * @author ztellman
 */
public class Util {

  private static final PrimitiveIterator.OfInt EMPTY_INT = new PrimitiveIterator.OfInt() {
    @Override
    public int nextInt() {
      throw new NoSuchElementException();
    }

    @Override
    public boolean hasNext() {
      return false;
    }
  };

  static final Object DEFAULT_VALUE = new Object();

  static final int NONE_NONE = 0;
  static final int NODE_NONE = 0x1;
  static final int ENTRY_NONE = 0x2;
  static final int NONE_NODE = 0x4;
  static final int NONE_ENTRY = 0x8;
  static final int ENTRY_NODE = ENTRY_NONE | NONE_NODE;
  static final int NODE_ENTRY = NODE_NONE | NONE_ENTRY;
  static final int  ENTRY_ENTRY = ENTRY_NONE | NONE_ENTRY;
  static final int NODE_NODE = NODE_NONE | NONE_NODE;

  static int mergeState(int mask, int nodeA, int dataA, int nodeB, int dataB) {
    int state = 0;
    state |= (mask & nodeA) != 0 ? 0x1 : 0;
    state |= (mask & dataA) != 0 ? 0x2 : 0;
    state |= (mask & nodeB) != 0 ? 0x4 : 0;
    state |= (mask & dataB) != 0 ? 0x8 : 0;

    return state;
  }

  static int compressedIndex(int bitmap, int hashMask) {
    return bitCount(bitmap & (hashMask - 1));
  }

  public static int startIndex(int bitmap) {
    return bitOffset(lowestBit(bitmap & 0xFFFFFFFFL));
  }

  public static int endIndex(int bitmap) {
    return bitOffset(highestBit(bitmap & 0xFFFFFFFFL));
  }

  static PrimitiveIterator.OfInt masks(int bitmap) {
    if (bitmap == 0) {
      return EMPTY_INT;
    }

    long start = 1L << startIndex(bitmap);
    long end = 1L << (endIndex(bitmap) + 1);

    return new PrimitiveIterator.OfInt() {
      long mask = start;
      @Override
      public int nextInt() {
        long result = mask;
        mask <<= 1;
        return (int) result;
      }

      @Override
      public boolean hasNext() {
        return mask < end;
      }
    };
  }

  static PrimitiveIterator.OfInt reverseMasks(int bitmap) {
    if (bitmap == 0) {
      return EMPTY_INT;
    }

    long start = 1L << startIndex(bitmap);
    long end = 1L << endIndex(bitmap);

    return new PrimitiveIterator.OfInt() {
      long mask = end;
      @Override
      public int nextInt() {
        long result = mask;
        mask >>= 1;
        return (int) result;
      }

      @Override
      public boolean hasNext() {
        return mask >= start;
      }
    };
  }


}
