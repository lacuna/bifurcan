package io.lacuna.bifurcan.utils;

import java.util.Arrays;

/**
 * A series of utility functions that use a bit-vector to store a sorted set of integers with arbitrary bit-lengths.
 *
 * @author ztellman
 */
public class BitIntSet {

  /**
   * @return a bit-int set, with an implied size of 0.
   */
  public static long[] create() {
    return BitVector.create(64);
  }

  /**
   * @param set            the bit-int set
   * @param bitsPerElement the bits per element
   * @param idx            the index
   * @return the value stored at the given index
   */
  public static long get(long[] set, int bitsPerElement, int idx) {
    return BitVector.get(set, idx * bitsPerElement, bitsPerElement);
  }

  /**
   * Performs a binary search for the given value.
   *
   * @param set            the bit-int set
   * @param bitsPerElement the bits per element
   * @param size           the number of elements in the set
   * @param val            the value to search for
   * @return If idx >= 0, the actual index of the value.  Otherwise, the return value represents the index where the
   * value would be inserted, where -1 represents the 0th element, -2 represents the 1st element, and so on.
   */
  public static int indexOf(long[] set, int bitsPerElement, int size, long val) {
    int low = 0;
    int high = size - 1;
    int mid = 0;

    while (low <= high) {
      mid = (low + high) >>> 1;
      long curr = get(set, bitsPerElement, mid);

      if (curr < val) {
        low = mid + 1;
      } else if (curr > val) {
        high = mid - 1;
      } else {
        return mid;
      }
    }

    return -(low + 1);
  }

  /**
   * @param set            the bit-int set
   * @param bitsPerElement the bits per element
   * @param size           the number of elements in the set
   * @param val            the value to add
   * @return an updated long[] array if the value is not already in the set, otherwise 'set' is returned unchanged
   */
  public static long[] add(long[] set, int bitsPerElement, int size, long val) {
    int idx = indexOf(set, bitsPerElement, size, val);
    if (idx < 0) {
      idx = -idx - 1;
      return BitVector.insert(set, (bitsPerElement * size), val, (bitsPerElement * idx), bitsPerElement);
    } else {
      return set;
    }
  }   

  /**
   * @param set            the bit-int set
   * @param bitsPerElement the bits per element
   * @param size           the number of elements in the set
   * @param val            the value to remove
   * @return an updated long[] array if the value was in the set, otherwise 'set' is returned unchanged
   */
  public static long[] remove(long[] set, int bitsPerElement, int size, long val) {
    int idx = indexOf(set, bitsPerElement, size, val);
    if (idx < 0) {
      return set;
    } else {
      return BitVector.remove(set, (bitsPerElement * size), (bitsPerElement * idx), bitsPerElement);
    }
  }
}
