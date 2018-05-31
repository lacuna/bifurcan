package io.lacuna.bifurcan.utils;

/**
 * @author ztellman
 */
public class Encodings {

  private static long NEGATIVE_ZERO = Double.doubleToLongBits(-0.0);

  /**
   * Converts a double into a corresponding long that shares the same ordering semantics.
   */
  public static long doubleToLong(double value) {
    long v = Double.doubleToRawLongBits(value);
    if (v == NEGATIVE_ZERO) {
      return 0;
    }

    if (value < -0.0) {
      v ^= Long.MAX_VALUE;
    }
    return v;
  }

  /**
   * The inverse operation for {@code doubleToLong()}.
   */
  public static double longToDouble(long value) {
    if (value < -0.0) {
      value ^= Long.MAX_VALUE;
    }
    return Double.longBitsToDouble(value);
  }
}
