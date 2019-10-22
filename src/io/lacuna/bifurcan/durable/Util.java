package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.CRC32;

/**
 * @author ztellman
 */
public class Util {

  public final static Charset UTF_16 = Charset.forName("utf-16");
  public static final Charset UTF_8 = Charset.forName("utf-8");
  public static final Charset ASCII = Charset.forName("ascii");

  public static long size(Iterable<ByteBuffer> bufs) {
    long size = 0;
    for (ByteBuffer b : bufs) {
      size += b.remaining();
    }
    return size;
  }
  
  public static int crc32(byte[] block) {
    CRC32 crc = new CRC32();
    crc.update(block, 0, block.length);
    return (int) crc.getValue();
  }

  public static int crc32(ByteBuffer block) {
    CRC32 crc = new CRC32();
    crc.update(block);
    return (int) crc.getValue();
  }

  public static void writeVLQ(long val, DataOutput out) throws IOException {
    assert(val >= 0);

    int highestBit = Bits.bitOffset(Bits.highestBit(val));

    int shift = Math.floorDiv(highestBit, 7) * 7;
    for (; ; shift -= 7) {
      byte b = (byte) ((val >> shift) & 127);
      if (shift == 0) {
        out.writeByte(b);
        break;
      } else {
        out.writeByte((byte) (b | 128));
      }
    }
  }

  public static long readVLQ(DataInput in) throws IOException {
    return readVLQ(0, in);
  }

  public static long readVLQ(long result, DataInput in) throws IOException {
    for (; ; ) {
      long b = in.readByte() & 0xFFL;
      result |= (b & 127);

      if ((b & 128) == 0) {
        break;
      }
      result <<= 7;
    }

    return result;
  }

  public static long readPrefixedVLQ(int firstByte, int prefixLength, DataInput in) throws IOException {
    int continueOffset = 7 - prefixLength;

    long result = firstByte & Bits.maskBelow(continueOffset);
    return Bits.test(firstByte, continueOffset)
      ? readVLQ(result, in)
      : result;
  }

  public static void writePrefixedVLQ(int prefix, int prefixLength, long n, DataOutput out) throws IOException {
    prefix <<= 8 - prefixLength;

    int continueBit = 1 << (7 - prefixLength);
    if (n < continueBit) {
      out.writeByte(prefix | (int) n);
    } else {
      out.writeByte(prefix | continueBit);
      writeVLQ(n, out);
    }
  }

  public static <V extends Comparable> Iterator<V> mergeSort(IList<Iterator<V>> iterators) {

    if (iterators.size() == 1) {
      return iterators.first();
    }

    PriorityQueue<IEntry<Iterator<V>, V>> heap = new PriorityQueue<IEntry<Iterator<V>, V>>(Comparator.comparing(IEntry::value));
    for (Iterator<V> it : iterators) {
      if (it.hasNext()) {
        heap.add(new Maps.Entry<>(it, it.next()));
      }
    }

    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return heap.size() > 0;
      }

      @Override
      public V next() {
        IEntry<Iterator<V>, V> e = heap.poll();
        if (e == null) {
          throw new NoSuchElementException();
        }

        if (e.key().hasNext()) {
          heap.add(new Maps.Entry<>(e.key(), e.key().next()));
        }
        return e.value();
      }
    };
  }

  public static int transfer(ByteBuffer src, ByteBuffer dst) {
    int n;
    if (dst.remaining() < src.remaining()) {
      n = dst.remaining();
      dst.put((ByteBuffer) src.duplicate().limit(n));
      src.position(src.position() + n);
    } else {
      n = src.remaining();
      dst.put(src);
    }

    return n;
  }


}
