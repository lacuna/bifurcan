package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator.SlabBuffer;
import io.lacuna.bifurcan.durable.blocks.HashMap;
import io.lacuna.bifurcan.durable.blocks.List;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.function.Predicate;

/**
 * @author ztellman
 */
public class Util {
  public final static Charset UTF_16 = Charset.forName("utf-16");
  public static final Charset UTF_8 = Charset.forName("utf-8");
  public static final Charset ASCII = Charset.forName("ascii");

  public static String toHexTable(DurableInput in) {
    StringBuffer sb = new StringBuffer();
    ByteBuffer buf = ByteBuffer.allocate(16);
    while (in.remaining() > 0) {
      buf.clear();
      in.read(buf);
      buf.flip();

      for (int i = 0; i < 16; i++) {
        if (i == 8) {
          sb.append(" ");
        }

        if (buf.hasRemaining()) {
          sb.append(String.format("%02X", buf.get())).append(" ");
        } else {
          sb.append("   ");
        }
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  public static String toHexString(ByteBuffer buf) {
    StringBuffer sb = new StringBuffer();
    buf = buf.duplicate();
    while (buf.hasRemaining()) {
      sb.append(Integer.toHexString(buf.get() & 0xFF));
    }
    return sb.toString();
  }

  public static int compareBuffers(ByteBuffer a, ByteBuffer b) {
    a = a.duplicate();
    b = b.duplicate();

    while (a.hasRemaining() && b.hasRemaining()) {
      int d = (a.get() & 0xFF) - (b.get() & 0xFF);
      if (d != 0) {
        return d;
      }
    }

    if (a.hasRemaining()) {
      return 1;
    } else if (b.hasRemaining()) {
      return -1;
    } else {
      return 0;
    }
  }

  public static int compareInputs(DurableInput a, DurableInput b) {
    a = a.duplicate();
    b = b.duplicate();

    while (a.hasRemaining() && b.hasRemaining()) {
      int d = a.readUnsignedByte() - b.readUnsignedByte();
      if (d != 0) {
        return d;
      }
    }

    if (a.hasRemaining()) {
      return 1;
    } else if (b.hasRemaining()) {
      return -1;
    } else {
      return 0;
    }
  }


  public static void encodePrimitives(IList<Object> os, IDurableEncoding.Primitive encoding, DurableOutput out) {
    DurableBuffer.flushTo(out, BlockType.PRIMITIVE, acc -> encoding.encode(os, acc));
  }

  public static void encodeSingleton(Object o, IDurableEncoding encoding, DurableOutput out) {
    if (o instanceof IMap && encoding instanceof IDurableEncoding.Map) {
      HashMap.encodeUnsortedEntries(((IMap) o).entries(), (IDurableEncoding.Map) encoding, out);
    } else if (o instanceof ISet && encoding instanceof IDurableEncoding.Set) {
      throw new IllegalArgumentException();
    } else if (o instanceof IList && encoding instanceof IDurableEncoding.List) {
      List.encode(((IList) o).iterator(), (IDurableEncoding.List) encoding, out);
    } else if (encoding instanceof IDurableEncoding.Primitive) {
      encodePrimitives(LinearList.of(o), (IDurableEncoding.Primitive) encoding, out);
    } else {
      throw new IllegalArgumentException(String.format("cannot encode %s with %s", o.getClass().getName(), encoding.description()));
    }
  }

  public static void encodeBlock(IList<Object> os, IDurableEncoding encoding, DurableOutput out) {
    if (os.size() == 1) {
      encodeSingleton(os.first(), encoding, out);
    } else if (encoding instanceof IDurableEncoding.Primitive){
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
        return HashMap.decode(pool, root, (IDurableEncoding.Map) encoding);
      case LIST:
        if (!(encoding instanceof IDurableEncoding.List)) {
          throw new IllegalArgumentException(String.format("cannot decode list with %s", encoding.description()));
        }
        return List.decode(pool, root, (IDurableEncoding.List) encoding);
      default:
        throw new IllegalArgumentException("Unexpected block type: " + prefix.type.name());
    }
  }

  /**
   * Decodes a block of encoded values, which may or may not be a singleton collection.  This does NOT advance the input.
   */
  public static IDurableEncoding.SkippableIterator decodeBlock(DurableInput in, IDurableCollection.Root root, IDurableEncoding encoding) {
    BlockPrefix prefix = in.peekPrefix();
    if (prefix.type == BlockType.PRIMITIVE) {
      if (!(encoding instanceof IDurableEncoding.Primitive)) {
        throw new IllegalArgumentException(String.format("cannot decode primitive value using %s", encoding.description()));
      }
      return ((IDurableEncoding.Primitive) encoding).decode(in.duplicate().sliceBlock(BlockType.PRIMITIVE), root);
    } else {
      return Iterators.skippable(Iterators.singleton(decodeCollection(prefix, root, encoding, in.pool())));
    }
  }

  public static long size(Iterable<SlabBuffer> bufs) {
    long size = 0;
    for (SlabBuffer b : bufs) {
      size += b.size();
    }
    return size;
  }

  public static void writeVLQ(long val, DurableOutput out) {
    assert (val >= 0);

    int shift = Math.floorDiv(Bits.log2Ceil(val), 7) * 7;
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
      result = (result << 7) | (b & 127);

      if ((b & 128) == 0) {
        break;
      }
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

    PriorityQueue<IEntry<V, Iterator<V>>> heap = new PriorityQueue<>(Comparator.comparing(IEntry::key, comparator));
    for (Iterator<V> it : iterators) {
      if (it.hasNext()) {
        heap.add(IEntry.of(it.next(), it));
      }
    }

    return Iterators.from(
        () -> heap.size() > 0,
        () -> {
          IEntry<V, Iterator<V>> e = heap.poll();
          if (e.value().hasNext()) {
            heap.add(IEntry.of(e.value().next(), e.value()));
          }
          return e.key();
        });
  }

  public static int transfer(ByteBuffer src, ByteBuffer dst) {
    int n;
    if (dst.remaining() < src.remaining()) {
      n = dst.remaining();
      dst.put((ByteBuffer) src.duplicate().limit(src.position() + n));
      src.position(src.position() + n);
    } else {
      n = src.remaining();
      dst.put(src);
    }

    return n;
  }

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


}
