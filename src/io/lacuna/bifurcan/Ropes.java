package io.lacuna.bifurcan;

import io.lacuna.bifurcan.hash.PerlHash;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author ztellman
 */
public class Ropes {

  private Ropes() {
  }

  /**
   * lexicographically compares two UTF-8 binary streams by code points, assumes both are of equal length
   */
  static int compare(Iterator<ByteBuffer> a, Iterator<ByteBuffer> b) {

    ByteBuffer x = a.next();
    ByteBuffer y = b.next();

    for (; ; ) {

      int len = Math.min(x.remaining(), y.remaining());
      for (int k = 0; k < len; k++) {
        byte bx = x.get();
        byte by = y.get();
        if (bx != by) {
          return (bx & 0xFF) - (by & 0xFF);
        }
      }

      if (!x.hasRemaining()) {
        if (!a.hasNext()) {
          break;
        }
        x = a.next();
      }

      if (!y.hasRemaining()) {
        y = b.next();
      }
    }

    return 0;
  }

  public static int hash(Rope r) {
    return PerlHash.hash(0, r.bytes());
  }

  public static boolean equals(Rope a, Rope b) {
    if (a.size() != b.size()) {
      return false;
    }

    if (a.size() == 0) {
      return true;
    }

    return compare(a.bytes(), b.bytes()) == 0;
  }
}
