package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Bits;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @author ztellman
 */
public class Util {
  public final static Charset UTF_16 = Charset.forName("utf-16");
  public static final Charset UTF_8 = Charset.forName("utf-8");
  public static final Charset ASCII = Charset.forName("ascii");

  public static <V, E> Iterator<IList<V>> partitionBy(
      Iterator<V> it,
      int blockSize,
      Predicate<V> isSingleton) {
    return new Iterator<IList<V>>() {
      LinearList<V> next = null;

      @Override
      public boolean hasNext() {
        return next != null || it.hasNext();
      }

      @Override
      public IList<V> next() {
        IList<V> curr = next;
        next = null;

        if (curr == null) {
          curr = LinearList.of(it.next());
        }

        if (!isSingleton.test(curr.first())) {
          while (it.hasNext() && curr.size() < blockSize) {
            V v = it.next();
            if (isSingleton.test(v)) {
              next = LinearList.of(v);
              break;
            } else {
              curr.addLast(v);
            }
          }
        }

        return curr;
      }
    };
  }

  /**
   * Writes a signed variable-length quantity.
   */
  public static void writeVLQ(long val, DurableOutput out) {
    if (val < 0) {
      writePrefixedUVLQ(1, 1, -val, out);
    } else {
      writePrefixedUVLQ(0, 1, val, out);
    }
  }

  /**
   * Reads a signed variable-length quantity.
   */
  public static long readVLQ(DurableInput in) {
    int b = in.readByte() & 0xFF;
    long val = readPrefixedUVLQ(b, 1, in);
    return (b & 128) > 0 ? -val : val;

  }

  /**
   * Writes an unsigned variable-length quantity.
   */
  public static void writeUVLQ(long val, DurableOutput out) {
    writeUVLQ(val, Bits.log2Floor(val) + 1, out);
  }

  private static void writeUVLQ(long val, int bits, DurableOutput out) {
    assert bits > 0;

    int shift = (int) Math.ceil(bits / 7.0) * 7;
    for (; ; shift -= 7) {
      byte b = (byte) Bits.slice(val, shift - 7, shift);
      if (shift == 7) {
        out.writeByte(b);
        break;
      } else {
        out.writeByte((byte) (b | 128));
      }
    }
  }

  /**
   * Reads an unsigned variable-length quantity.
   */
  public static long readUVLQ(DurableInput in) {
    return readUVLQ(0, in);
  }

  public static long readUVLQ(long result, DurableInput in) {
    for (; ; ) {
      long b = in.readByte() & 0xFFL;
      result = (result << 7) | (b & 127);
      if ((b & 128) == 0) {
        break;
      }
    }

    return result;
  }

  /**
   * @param firstByte
   * @param prefixLength
   * @param in
   * @return
   */
  public static long readPrefixedUVLQ(int firstByte, int prefixLength, DurableInput in) {
    int continueOffset = 7 - prefixLength;

    long result = firstByte & Bits.maskBelow(continueOffset);
    return Bits.test(firstByte, continueOffset)
        ? readUVLQ(result, in)
        : result;
  }

  /**
   * @param prefix
   * @param prefixLength
   * @param n
   * @param out
   */
  public static void writePrefixedUVLQ(int prefix, int prefixLength, long n, DurableOutput out) {
    prefix <<= 8 - prefixLength;

    int continueBit = 1 << (7 - prefixLength);
    if (n < continueBit) {
      out.writeByte(prefix | (int) n);
    } else {
      int bits = Bits.log2Floor(n) + 1;
      int rem = bits % 7;

      int firstByte = prefix | continueBit;
      if (0 < rem && rem < (7 - prefixLength)) {
//        System.out.println(Long.toBinaryString(n) + " " + bits + " " + rem + " " + Long.toBinaryString(Bits.slice(n, bits - rem, bits)));
        bits -= rem;
        firstByte |= Bits.slice(n, bits, bits + rem);
      }

      out.writeByte(firstByte);
//      System.out.println(Integer.toBinaryString(firstByte) + " " + bits);
      writeUVLQ(n, bits, out);
    }
  }

}
