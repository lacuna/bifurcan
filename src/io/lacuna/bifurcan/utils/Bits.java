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

  public static int bitLog2(long n) {
    return deBruijnIndex[0xFF & (int) ((n * 0x022fdd63cc95386dL) >>> 58)];
  }

  public static long lowestBit(long n) {
    return n & -n;
  }

  public static long highestBit(long n, long estimate) {
    long x = n & ~(estimate - 1);
    long m;
    while (true) {
      m = lowestBit(x);
      if (x == m) return m;
      x -= m;
    }
  }

  public static long maskBelow(int bits) {
    return (1L << bits) - 1;
  }

  public static long maskAbove(int bits) {
    return -1L & ~maskBelow(bits);
  }

  public static int branchingBit(long a, long b) {
    return bitLog2(highestBit(a ^ b, 1));
  }

}
