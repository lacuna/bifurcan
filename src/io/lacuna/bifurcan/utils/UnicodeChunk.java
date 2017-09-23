package io.lacuna.bifurcan.utils;

import java.nio.charset.Charset;
import java.util.PrimitiveIterator;

import static java.lang.Character.*;
import static java.lang.System.arraycopy;

/**
 * An immutable UTF-8 encoded block of no more than 255 UTF-16 code units, which allows lookups by both code point and
 * code unit.
 */
public class UnicodeChunk {

  private static final Charset UTF8 = Charset.forName("utf-8");

  public static final byte[] EMPTY = new byte[]{0, 0};

  public static byte[] from(CharSequence cs) {
    return from(cs, 0, cs.length());
  }

  public static byte[] from(CharSequence cs, int start, int end) {
    if (end - start > 255) {
      throw new IllegalArgumentException("cannot encode a block of more than 255 UTF-16 code units");
    }

    int numBytes = 0;
    int[] codePoints = new int[cs.length()];
    int codePointIdx = 0;
    for (int charIdx = start; charIdx < end; codePointIdx++) {
      char c = cs.charAt(charIdx);
      int codePoint;
      if (isHighSurrogate(c)) {
        codePoint = Character.toCodePoint(c, cs.charAt(charIdx + 1));
        charIdx += 2;
      } else {
        codePoint = c & 0xFFFF;
        charIdx += 1;
      }
      numBytes += encodedLength(codePoint);
      codePoints[codePointIdx] = codePoint;
    }

    byte[] chunk = new byte[numBytes + 2];
    chunk[0] = (byte) codePointIdx;
    chunk[1] = (byte) (end - start);

    int offset = 2;
    for (int i = 0; i < codePointIdx; i++) {
      int codePoint = codePoints[i];
      offset += overwrite(chunk, offset, codePoint);
    }

    return chunk;
  }

  public static CharSequence toCharSequence(byte[] chunk) {
    return new CharSequence() {
      @Override
      public int length() {
        return numCodeUnits(chunk);
      }

      @Override
      public char charAt(int index) {
        return nthUnit(chunk, index);
      }

      @Override
      public CharSequence subSequence(int start, int end) {
        return CharSequences.subSequence(this, start, end);
      }

      @Override
      public String toString() {
        return UnicodeChunk.toString(chunk);
      }
    };
  }

  public static String toString(byte[] chunk) {
    char[] cs = new char[numCodeUnits(chunk)];
    for (int bi = 2, ci = 0; ci < cs.length; ) {
      int codePoint = decode(chunk, bi);
      bi += codePoint < 0x80 ? 1 : encodedLength(chunk[bi]);

      if (isBmpCodePoint(codePoint)) {
        cs[ci++] = (char) codePoint;
      } else {
        cs[ci++] = highSurrogate(codePoint);
        cs[ci++] = lowSurrogate(codePoint);
      }
    }
    return new String(cs);
  }

  public static byte[] concat(byte[] a, byte[] b) {
    if (numCodeUnits(a) + numCodeUnits(b) > 255) {
      throw new IllegalArgumentException("cannot create a chunk larger than 255 UTF-16 code units");
    }

    byte[] newChunk = new byte[a.length + b.length - 2];
    arraycopy(a, 2, newChunk, 2, a.length - 2);
    arraycopy(b, 2, newChunk, a.length, b.length - 2);
    newChunk[0] = (byte) (numCodePoints(a) + numCodePoints(b));
    newChunk[1] = (byte) (numCodeUnits(a) + numCodeUnits(b));

    return newChunk;
  }

  public static byte[] slice(byte[] chunk, int start, int end) {
    if (end > numCodePoints(chunk) || start < 0) {
      System.out.println(start + " " + end + " " + numCodePoints(chunk));
      throw new IllegalArgumentException("slice range out of bounds");
    } else if (start == 0 && end == numCodePoints(chunk)) {
      return chunk;
    }

    int startOffset = offset(chunk, start);
    int codeUnits = 0;
    int endOffset;
    if (isAscii(chunk)) {
      endOffset = end + 2;
      codeUnits = end - start;
    } else {
      endOffset = startOffset;
      for (int i = start; i < end; i++) {
        byte b = chunk[endOffset];
        int len = b >= 0 ? 1 : encodedLength(b);
        codeUnits += len == 4 ? 2 : 1;
        endOffset += len;
      }
    }

    byte[] newChunk = new byte[(endOffset - startOffset) + 2];
    arraycopy(chunk, startOffset, newChunk, 2, newChunk.length - 2);
    newChunk[0] = (byte) (end - start);
    newChunk[1] = (byte) codeUnits;

    return newChunk;
  }

  public static char nthUnit(byte[] chunk, int idx) {
    if (isAscii(chunk)) {
      return (char) chunk[idx + 2];
    } else {
      return findNthUnit(chunk, idx);
    }
  }

  public static int nthPoint(byte[] chunk, int idx) {
    return decode(chunk, offset(chunk, idx));
  }

  public static int numCodePoints(byte[] chunk) {
    return chunk[0] & 0xFF;
  }

  public static int numCodeUnits(byte[] chunk) {
    return chunk[1] & 0xFF;
  }

  public static int writeCodeUnits(char[] array, int offset, byte[] chunk) {
    for (int aryIdx = offset, chunkIdx = 2; chunkIdx < chunk.length; ) {
      byte b = chunk[chunkIdx];
      if (b >= 0) {
        array[aryIdx++] = (char) b;
        chunkIdx++;
      } else {
        int codePoint = decode(chunk, chunkIdx);
        chunkIdx += encodedLength(codePoint);
        if (isBmpCodePoint(codePoint)) {
          array[aryIdx++] = (char) codePoint;
        } else {
          array[aryIdx++] = highSurrogate(codePoint);
          array[aryIdx++] = lowSurrogate(codePoint);
        }
      }
    }

    return numCodeUnits(chunk);
  }

  public static int writeCodePoints(int[] array, int offset, byte[] chunk) {
    for (int aryIdx = offset, chunkIdx = 2; chunkIdx < chunk.length; ) {
      int codePoint = decode(chunk, chunkIdx);
      array[aryIdx++] = codePoint;
      chunkIdx += encodedLength(codePoint);
    }

    return numCodePoints(chunk);
  }

  ///

  private static char findNthUnit(byte[] chunk, int idx) {
    int offset = 2;

    for (; ; ) {
      if (idx == 0) {
        int point = decode(chunk, offset);
        return isBmpCodePoint(point) ? (char) point : highSurrogate(point);
      }

      byte b = chunk[offset];
      if (b >= 0) {
        idx--;
        offset++;
      } else {
        int len = encodedLength(b);
        int codeUnits = 1 << (len >> 2);
        idx -= codeUnits;
        if (idx < 0) {
          return lowSurrogate(decode(chunk, offset));
        }
        offset += len;
      }
    }
  }

  private static int offset(byte[] chunk, int idx) {
    if (isAscii(chunk)) {
      return idx + 2;
    } else {
      return findNthPoint(chunk, idx);
    }
  }

  private static int findNthPoint(byte[] chunk, int idx) {
    int offset = 2;
    while (idx-- > 0) {
      byte b = chunk[offset];
      if (b >= 0) {
        offset++;
      } else {
        offset += encodedLength(b);
      }
    }
    return offset;
  }

  private static boolean isAscii(byte[] chunk) {
    return (chunk[0] & 0xFF) == (chunk.length - 2);
  }

  private static int encodedLength(int codePoint) {
    if (codePoint < 0x80) {
      return 1;
    } else if (codePoint < 0x800) {
      return 2;
    } else if (codePoint < 0x10000) {
      return 3;
    } else {
      return 4;
    }
  }

  private static int overwrite(byte[] chunk, int offset, int codePoint) {
    if (codePoint < 0x80) {
      chunk[offset] = (byte) codePoint;
      return 1;
    } else {
      return overwriteMultibyte(chunk, offset, codePoint);
    }
  }

  private static int overwriteMultibyte(byte[] chunk, final int offset, int codePoint) {
    final int length;
    final int mask;

    if (codePoint < 0x800) {
      length = 2;
      mask = 0b11000000;
    } else if (codePoint < 0x10000) {
      length = 3;
      mask = 0b11100000;
    } else {
      length = 4;
      mask = 0b11110000;
    }

    for (int i = offset + length - 1; i > offset; i--) {
      chunk[i] = (byte) ((codePoint & 0b00111111) | 0b10000000);
      codePoint >>= 6;
    }
    chunk[offset] = (byte) (codePoint | mask);

    return length;
  }

  public static int encodedLength(byte leadingByte) {
    int mask = 0b001000000;
    for (int i = 1; ; i++) {
      if ((leadingByte & mask) == 0) {
        return i;
      }
      mask >>= 1;
    }
  }

  private static int decode(byte[] array, int offset) {
    byte b = array[offset];
    if (b >= 0) {
      return b;
    }

    int len = encodedLength(b);
    int codePoint = b & ((1 << (7 - len)) - 1);

    for (int i = offset + 1; i < offset + len; i++) {
      codePoint = (codePoint << 6) | (array[i] & 0b00111111);
    }

    return codePoint;
  }


}
