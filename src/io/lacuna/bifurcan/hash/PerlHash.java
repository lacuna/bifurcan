package io.lacuna.bifurcan.hash;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author ztellman
 */
public class PerlHash {

  public static int hash(ByteBuffer buf) {
    return hash(0, buf);
  }

  public static int hash(int seed, ByteBuffer buf) {
    return hash(seed, buf, buf.position(), buf.remaining());
  }

  public static int hash(int seed, ByteBuffer buf, int offset, int len) {
    int key = seed;

    int limit = offset + len;
    for (int i = offset; i < limit; i++) {
      key += buf.get(i) & 0xFF;
      key += key << 10;
      key ^= key >>> 6;
    }
    key += key << 3;
    key ^= key >>> 11;
    key += key << 15;

    return key;
  }

  public static int hash(int seed, Iterator<ByteBuffer> buffers) {
    int key = seed;

    while (buffers.hasNext()) {
      ByteBuffer buf = buffers.next();
      for (int i = buf.position(); i < buf.limit(); i++) {
        key += buf.get(i) & 0xFF;
        key += key << 10;
        key ^= key >>> 6;
      }
    }
    key += key << 3;
    key ^= key >>> 11;
    key += key << 15;

    return key;
  }
}
