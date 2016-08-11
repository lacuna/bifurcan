package io.lacuna.bifurcan.utils;

/**
 * Static methods that implement an immutable n-bit number vector, using long[] as the underlying storage.  Numbers
 * larger than 64-bits can be represented via long[], as well.
 *
 * @author ztellman
 */
public final class ChunkVector {

  public static long get(int bitsPerNumber, int len, long[] array, int idx) {
    return BitVector.get(array, idx * bitsPerNumber, bitsPerNumber);
  }

  public static long[] insert(int bitsPerNumber, int len, long[] array, int idx, long val) {
    int offset = idx * bitsPerNumber;
    long[] updated = BitVector.insert(array, len, offset, bitsPerNumber);
    BitVector.overwrite(updated, val, offset, bitsPerNumber);
    return updated;
  }

  public static long[] remove(int bitsPerNumber, int len, long[] array, int idx) {
    return BitVector.remove(array, len, idx * bitsPerNumber, bitsPerNumber);
  }

  public static int binarySearch(int bitsPerNumber, int len, long[] array, long val) {
    int low = 0;
    int high = len - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long curr = get(bitsPerNumber, len, array, mid);

      if (curr < val) {
        low = mid + 1;
      } else if (curr > val) {
        high = mid - 1;
      } else {
        return mid;
      }
    }

    return -1;
  }

  public static long[] getArray(int bitsPerNumber, int len, long[] array, int idx) {
    long[] value = BitVector.create(bitsPerNumber);

    int offset = idx * bitsPerNumber;
    int bits = bitsPerNumber;
    while (bits > 0) {
      value[offset] = BitVector.get(array, offset, Math.min(64, bits));
      offset += 64;
      bits -= 64;
    }

    return value;
  }

  public static long[] insert(int bitsPerNumber, int len, long[] array, int idx, long[] val) {
    int offset = idx * bitsPerNumber;
    long[] updated = BitVector.insert(array, len, offset, bitsPerNumber);

    int i = 0;
    int bits = bitsPerNumber;
    while (bits > 0) {
      BitVector.overwrite(array, val[i], offset, Math.min(64, bits));
      i++;
      bits -= 64;
      offset += offset;
    }

    return updated;
  }

  public static int binarySearch(int bitsPerNumber, int len, long[] array, long val[]) {
    int low = 0;
    int high = len - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long[] curr = getArray(bitsPerNumber, len, array, mid);
      long diff = BitVector.compareTo(curr, val);

      if (diff < 0) {
        low = mid + 1;
      } else if (diff > 0) {
        high = mid - 1;
      } else {
        return mid;
      }
    }

    return -1;
  }

}
