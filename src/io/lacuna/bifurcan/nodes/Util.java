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

  static final Object DEFAULT_VALUE = new Object();

  static final int NONE_NONE = 0;
  static final int NODE_NONE = 0x1;
  static final int ENTRY_NONE = 0x2;
  static final int NONE_NODE = 0x4;
  static final int NONE_ENTRY = 0x8;
  static final int ENTRY_NODE = ENTRY_NONE | NONE_NODE;
  static final int NODE_ENTRY = NODE_NONE | NONE_ENTRY;
  static final int ENTRY_ENTRY = ENTRY_NONE | NONE_ENTRY;
  static final int NODE_NODE = NODE_NONE | NONE_NODE;

  public static int mergeState(int mask, int nodeA, int dataA, int nodeB, int dataB) {
    int state = 0;

    // this compiles down to no branches, apparently
    state |= ((mask & nodeA) != 0 ? 1 : 0);
    state |= ((mask & dataA) != 0 ? 1 : 0) << 1;
    state |= ((mask & nodeB) != 0 ? 1 : 0) << 2;
    state |= ((mask & dataB) != 0 ? 1 : 0) << 3;

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

  public static PrimitiveIterator.OfInt masks(int bitmap) {
    return new PrimitiveIterator.OfInt() {
      int b = bitmap;

      @Override
      public int nextInt() {
        int result = lowestBit(b);
        b &= ~result;
        return result;
      }

      @Override
      public boolean hasNext() {
        return b != 0;
      }
    };
  }

  static PrimitiveIterator.OfInt reverseMasks(int bitmap) {
    return new PrimitiveIterator.OfInt() {
      int b = bitmap;

      @Override
      public int nextInt() {
        int result = highestBit(b);
        b &= ~result;
        return result;
      }

      @Override
      public boolean hasNext() {
        return b != 0;
      }
    };
  }


}
