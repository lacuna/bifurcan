package io.lacuna.bifurcan.utils;

/**
 * @author ztellman
 */
public class Encodings {

  private static long NAN = doubleToLong(Double.NaN);
  private static long NEGATIVE_ZERO = Double.doubleToLongBits(-0.0);
  private static long NEGATIVE_EPSILON = doubleToLong(-2.23e-308);

  /**
   * The inverse operation for {@code doubleToLong()}.
   */
  public static double longToDouble(long value) {
    if (value == NAN) {
      return Double.NaN;
    } else if (value == NEGATIVE_EPSILON) {
      return -0.0;
    } else if (value < 0.0) {
      value ^= Long.MAX_VALUE;
    }
    return Double.longBitsToDouble(value);
  }

  /**
   * Converts a double into a corresponding long that shares the same comparison semantics.
   */
  public static long doubleToLong(double value) {
    long v = Double.doubleToRawLongBits(value);
    if (v == NEGATIVE_ZERO) {
      return NEGATIVE_EPSILON;
    }

    if (value < 0.0) {
      v ^= Long.MAX_VALUE;
    }
    return v;
  }
}
