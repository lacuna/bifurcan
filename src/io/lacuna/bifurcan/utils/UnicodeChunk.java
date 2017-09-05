package io.lacuna.bifurcan.utils;

import static java.lang.Character.*;
import static java.lang.System.arraycopy;

/**
 * An immutable UTF-8 encoded block of no more than 255 UTF-16 code units, which allows lookups by both code point and
 * code unit.
 */
public class UnicodeChunk {

  public static byte[] from(CharSequence cs) {
    if (cs.length() > 255) {
      throw new IllegalArgumentException("cannot encode a block of more than 255 UTF-16 code units");
    }

    int numBytes = 0;
    int[] codePoints = new int[cs.length()];
    int codePointIdx = 0;
    for (int charIdx = 0; charIdx < cs.length(); codePointIdx++) {
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
    chunk[1] = (byte) cs.length();

    int offset = 2;
    for (int i = 0; i < codePointIdx; i++) {
      int codePoint = codePoints[i];
      offset += overwrite(chunk, offset, codePoint);
    }

    return chunk;
  }

  public static byte[] remove(byte[] chunk, int idx) {
    int offset = offset(chunk, idx);
    int len = encodedLength(chunk[offset]);

    byte[] newChunk = new byte[chunk.length - len];
    arraycopy(chunk, 0, newChunk, 0, offset);
    arraycopy(chunk, offset + len, newChunk, offset, newChunk.length - offset);
    newChunk[0] = (byte) (numCodePoints(chunk) - 1);
    newChunk[1] = (byte) (numChars(chunk) - (len == 4 ? 2 : 1));

    return newChunk;
  }

  public static byte[] insert(byte[] chunk, int idx, int codePoint) {
    int offset = offset(chunk, idx);
    int len = encodedLength(codePoint);

    byte[] newChunk = new byte[chunk.length + len];
    arraycopy(chunk, 0, newChunk, 0, offset);
    overwrite(chunk, offset, codePoint);
    arraycopy(chunk, offset, newChunk, offset + len, chunk.length - offset);
    newChunk[0] = (byte) (numCodePoints(chunk) + 1);
    newChunk[1] = (byte) (numChars(chunk) + (len == 4 ? 2 : 1));

    return newChunk;
  }

  public static byte[] concat(byte[] a, byte[] b) {
    if (numChars(a) + numChars(b) > 255) {
      throw new IllegalArgumentException("cannot create a chunk larger than 255 UTF-16 code units");
    }

    byte[] newChunk = new byte[a.length + b.length - 2];
    arraycopy(a, 2, newChunk, 0, a.length - 2);
    arraycopy(b, 2, newChunk, a.length, b.length - 2);
    newChunk[0] = (byte) (numCodePoints(a) + numCodePoints(b));
    newChunk[1] = (byte) (numChars(a) + numChars(b));

    return newChunk;
  }

  public static byte[] slice(byte[] chunk, int start, int end) {
    if (end > numCodePoints(chunk) || start < 0) {
      throw new IllegalArgumentException("slice range out of bounds");
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
        int len = encodedLength(chunk[endOffset]);
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

  public static char nthChar(byte[] chunk, int idx) {
    if (isAscii(chunk)) {
      return (char) chunk[idx + 2];
    } else {
      return findNthChar(chunk, idx);
    }
  }

  public static int nth(byte[] chunk, int idx) {
    return decode(chunk, offset(chunk, idx));
  }

  public static int numCodePoints(byte[] chunk) {
    return chunk[0] & 0xFF;
  }

  public static int numChars(byte[] chunk) {
    return chunk[1] & 0xFF;
  }

  ///

  private static char findNthChar(byte[] chunk, int idx) {
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
      return findOffset(chunk, idx);
    }
  }

  private static int findOffset(byte[] chunk, int idx) {
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

  private static int encodedLength(byte leadingByte) {
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
    if (b > 0) {
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
