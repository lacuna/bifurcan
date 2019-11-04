package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Bits;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.*;
import java.util.zip.CRC32;

/**
 * @author ztellman
 */
public class Util {

  private static final Object NONE = new Object();

  public final static Charset UTF_16 = Charset.forName("utf-16");
  public static final Charset UTF_8 = Charset.forName("utf-8");
  public static final Charset ASCII = Charset.forName("ascii");

  public static boolean isCollection(Object o) {
    return o instanceof ICollection || o instanceof Collection;
  }

  public static void encodeCollection(Object o, DurableEncoding encoding, DurableOutput out) {
    if (o instanceof IMap) {
      HashMap.encode((IMap) o, encoding, out);
    } else if (o instanceof java.util.Map) {
      HashMap.encode(Maps.from((java.util.Map) o), encoding, out);
    }
  }

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

  public static void writeVLQ(long val, DurableOutput out) {
    assert (val >= 0);

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

  public static long readVLQ(DurableInput in) {
    return readVLQ(0, in);
  }

  public static long readVLQ(long result, DurableInput in) {
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

  public static long readPrefixedVLQ(int firstByte, int prefixLength, DurableInput in) {
    int continueOffset = 7 - prefixLength;

    long result = firstByte & Bits.maskBelow(continueOffset);
    return Bits.test(firstByte, continueOffset)
        ? readVLQ(result, in)
        : result;
  }

  public static void writePrefixedVLQ(int prefix, int prefixLength, long n, DurableOutput out) {
    prefix <<= 8 - prefixLength;

    int continueBit = 1 << (7 - prefixLength);
    if (n < continueBit) {
      out.writeByte(prefix | (int) n);
    } else {
      out.writeByte(prefix | continueBit);
      writeVLQ(n, out);
    }
  }

  public static <V> Iterator<V> mergeSort(IList<Iterator<V>> iterators, Comparator<V> comparator) {

    if (iterators.size() == 1) {
      return iterators.first();
    }

    PriorityQueue<IEntry<Iterator<V>, V>> heap = new PriorityQueue<IEntry<Iterator<V>, V>>(Comparator.comparing(IEntry::value, comparator));
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

  public static class Block<V, E> {
    public final boolean isCollection;
    public final E encoding;
    public final LinearList<V> elements;

    public Block(boolean isCollection, E encoding, V first) {
      this.isCollection = isCollection;
      this.encoding = encoding;
      this.elements = LinearList.of(first);
    }
  }

  public static <V, E> Iterator<Block<V, E>> partitionBy(
      Iterator<V> it,
      Function<V, E> encoding,
      ToIntFunction<E> blockSize,
      BiPredicate<E, E> compatibleEncoding,
      Predicate<V> isCollection) {
    return new Iterator<Block<V, E>>() {
      Block<V, E> next = null;

      @Override
      public boolean hasNext() {
        return next != null || it.hasNext();
      }

      @Override
      public Block<V, E> next() {
        Block<V, E> curr = next;
        next = null;

        if (curr == null) {
          V v = it.next();
          curr = new Block<>(isCollection.test(v), encoding.apply(v), v);
        }

        int maxSize = blockSize.applyAsInt(curr.encoding);
        while (it.hasNext() && curr.elements.size() < maxSize) {
          V v = it.next();
          E nextEncoding = encoding.apply(v);
          boolean nextIsCollection = isCollection.test(v);
          if (nextIsCollection || !compatibleEncoding.test(curr.encoding, nextEncoding)) {
            next = new Block<>(nextIsCollection, nextEncoding, v);
            break;
          } else {
            curr.elements.addLast(v);
          }
        }

        return curr;
      }
    };
  }


}
