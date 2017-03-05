package io.lacuna.bifurcan.nodes;

import static java.lang.Integer.bitCount;

/**
 * @author ztellman
 */
public class Util {

  static int compressedIndex(int bitmap, int hashMask) {
    return bitCount(bitmap & (hashMask - 1));
  }

  static int hashMask(int hash, int shift) {
    return 1 << ((hash >>> shift) & 31);
  }

}
