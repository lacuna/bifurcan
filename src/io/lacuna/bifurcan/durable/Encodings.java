package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.blocks.HashMap;
import io.lacuna.bifurcan.durable.blocks.List;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

public class Encodings {

  public enum Mode {
    NORMAL,
    MIGRATION,
    COMPACT
  }

  public static void encodePrimitives(IList<Object> os, IDurableEncoding.Primitive encoding, DurableOutput out) {
    DurableBuffer.flushTo(out, BlockPrefix.BlockType.PRIMITIVE, acc -> encoding.encode(os, acc));
  }

  public static void encodeSingleton(Object o, IDurableEncoding encoding, DurableOutput out) {
    if (o instanceof IMap && encoding instanceof IDurableEncoding.Map) {
      HashMap.encodeUnsortedEntries(((IMap) o).entries(), (IDurableEncoding.Map) encoding, out);
    } else if (o instanceof ISet && encoding instanceof IDurableEncoding.Set) {
      throw new IllegalArgumentException();
    } else if (o instanceof IList && encoding instanceof IDurableEncoding.List) {
      io.lacuna.bifurcan.durable.blocks.List.encode(((IList) o).iterator(), (IDurableEncoding.List) encoding, out);
    } else if (encoding instanceof IDurableEncoding.Primitive) {
      encodePrimitives(LinearList.of(o), (IDurableEncoding.Primitive) encoding, out);
    } else {
      throw new IllegalArgumentException(String.format("cannot encode %s with %s", o.getClass().getName(), encoding.description()));
    }
  }

  public static void encodeBlock(IList<Object> os, IDurableEncoding encoding, DurableOutput out) {
    if (os.size() == 1) {
      encodeSingleton(os.first(), encoding, out);
    } else if (encoding instanceof IDurableEncoding.Primitive) {
      encodePrimitives(os, (IDurableEncoding.Primitive) encoding, out);
    } else {
      throw new IllegalArgumentException(String.format("cannot encode primitive with %s", encoding.description()));
    }
  }

  /**
   * Decodes a singleton collection.  This does NOT advance the input.
   */
  public static IDurableCollection decodeCollection(BlockPrefix prefix, IDurableCollection.Root root, IDurableEncoding encoding, DurableInput.Pool pool) {
    switch (prefix.type) {
      case HASH_MAP:
        if (!(encoding instanceof IDurableEncoding.Map)) {
          throw new IllegalArgumentException(String.format("cannot decode map with %s", encoding.description()));
        }
        return HashMap.decode((IDurableEncoding.Map) encoding, root, pool);
      case LIST:
        if (!(encoding instanceof IDurableEncoding.List)) {
          throw new IllegalArgumentException(String.format("cannot decode list with %s", encoding.description()));
        }
        return List.decode((IDurableEncoding.List) encoding, root, pool);
      default:
        throw new IllegalArgumentException("Unexpected collection block type: " + prefix.type.name());
    }
  }

  /**
   * Decodes a block of encoded values, which may or may not be a singleton collection.  This does NOT advance the input.
   */
  public static IDurableEncoding.SkippableIterator decodeBlock(DurableInput in, IDurableCollection.Root root, IDurableEncoding encoding) {
    BlockPrefix prefix = in.peekPrefix();
    if (prefix.type == BlockPrefix.BlockType.PRIMITIVE) {
      if (!(encoding instanceof IDurableEncoding.Primitive)) {
        throw new IllegalArgumentException(String.format("cannot decode primitive value using %s", encoding.description()));
      }
      return ((IDurableEncoding.Primitive) encoding).decode(in.duplicate().sliceBlock(BlockPrefix.BlockType.PRIMITIVE), root);
    } else {
      return Iterators.skippable(Iterators.singleton(decodeCollection(prefix, root, encoding, in.pool())));
    }
  }

  ///

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
