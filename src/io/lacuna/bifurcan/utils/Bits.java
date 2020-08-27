package io.lacuna.bifurcan.utils;

/**
 * @author ztellman
 */
public class Bits {

  private static final byte deBruijnIndex[] =
      new byte[]{0, 1, 2, 53, 3, 7, 54, 27, 4, 38, 41, 8, 34, 55, 48, 28,
          62, 5, 39, 46, 44, 42, 22, 9, 24, 35, 59, 56, 49, 18, 29, 11,
          63, 52, 6, 26, 37, 40, 33, 47, 61, 45, 43, 21, 23, 58, 17, 10,
          51, 25, 36, 32, 60, 20, 57, 16, 50, 31, 19, 15, 30, 14, 13, 12};

  /**
   * @param n a number, which must be a power of two
   * @return the offset of the bit
   */
  public static int bitOffset(long n) {
    return deBruijnIndex[0xFF & (int) ((n * 0x022fdd63cc95386dL) >>> 58)];
  }

  /**
   * @param n a number
   * @return the same number, with all but the lowest bit zeroed out
   */
  public static long lowestBit(long n) {
    return n & -n;
  }

  /**
   * @param n a number
   * @return the same number, with all but the lowest bit zeroed out
   */
  public static int lowestBit(int n) {
    return n & -n;
  }

  /**
   * @param n a number
   * @return the same number, with all but the highest bit zeroed out
   */
  public static long highestBit(long n) {
    return Long.highestOneBit(n);
  }

  /**
   * @param n a number
   * @return the same number, with all but the highest bit zeroed out
   */
  public static int highestBit(int n) {
    return Integer.highestOneBit(n);
  }

  /**
   * @param n a number
   * @return the log2 of that value, rounded down
   */
  public static int log2Floor(long n) {
    return bitOffset(highestBit(n));
  }

  /**
   * @param n a number
   * @return the log2 of the value, rounded up
   */
  public static int log2Ceil(long n) {
    int log2 = log2Floor(n);
    return isPowerOfTwo(n) ? log2 : log2 + 1;
  }

  /**
   * @param n      a number
   * @param offset the offset of the bit being tested
   * @return true if the bit is 1, false otherwise
   */
  public static boolean test(int n, int offset) {
    return (n & (1 << offset)) != 0;
  }

  /**
   * @param n      a number
   * @param offset the offset of the bit being tested
   * @return true if the bit is 1, false otherwise
   */
  public static boolean test(long n, int offset) {
    return (n & (1L << offset)) != 0;
  }

  /**
   * @param bits a bit offset
   * @return a mask, with all bits below that offset set to one
   */
  public static long maskBelow(int bits) {
    return (1L << bits) - 1;
  }

  /**
   * @param bits a bit offset
   * @return a mask, with all bits above that offset set to one
   */
  public static long maskAbove(int bits) {
    return -1L & ~maskBelow(bits);
  }

  /**
   * @return the offset of the highest bit which differs between {@code a} and {@code b}
   */
  public static int branchingBit(long a, long b) {
    if (a == b) {
      return -1;
    } else {
      return bitOffset(highestBit(a ^ b));
    }
  }

  /**
   * @param n a number
   * @return true, if the number is a power of two
   */
  public static boolean isPowerOfTwo(long n) {
    return (n & (n - 1)) == 0;
  }

  public static long slice(long n, int start, int end) {
    return (n >> start) & maskBelow(end - start);
  }
}
