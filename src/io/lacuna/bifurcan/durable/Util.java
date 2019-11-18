package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.DurableEncoding.SkippableIterator;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.blocks.HashMap;
import io.lacuna.bifurcan.durable.blocks.List;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class Util {

  public static ThreadLocal<LinearSet<IDurableCollection.Fingerprint>> DEPENDENCIES = ThreadLocal.withInitial(LinearSet::new);

  public final static Charset UTF_16 = Charset.forName("utf-16");
  public static final Charset UTF_8 = Charset.forName("utf-8");
  public static final Charset ASCII = Charset.forName("ascii");

  public static boolean isCollection(Object o) {
    return o instanceof ICollection || o instanceof Collection;
  }

  public static void encodeBlock(IList<Object> os, DurableEncoding encoding, DurableOutput out) {
    if (os.size() == 1 && isCollection(os.first())) {
      Object o = os.first();
      if (o instanceof IMap) {
        HashMap.encodeUnsortedEntries(((IMap<Object, Object>) o).entries(), encoding, out);

      } else if (o instanceof java.util.Map) {
        IList<IEntry<Object, Object>> entries = ((java.util.Map<Object, Object>) o).entrySet()
            .stream()
            .map(e -> IEntry.of(e.getKey(), e.getValue()))
            .collect(Lists.linearCollector());
        HashMap.encodeUnsortedEntries(entries, encoding, out);

      } else if (o instanceof Iterable) {
        List.encode(((Iterable) o).iterator(), encoding, out);
      }

    } else {
      DurableAccumulator.flushTo(out, BlockType.ENCODED, acc -> encoding.encode(os, acc));
    }
  }

  public static SkippableIterator decodeBlock(DurableInput in, IDurableCollection.Root root, DurableEncoding encoding) {
    BlockPrefix prefix = in.peekPrefix();
    switch (prefix.type) {
      case ENCODED:
        return encoding.decode(in.duplicate().sliceBlock(BlockType.ENCODED));
      case HASH_MAP:
        return SkippableIterator.singleton(HashMap.decode(in.duplicate(), root, encoding));
      case LIST:
        return SkippableIterator.singleton(List.decode(in.duplicate(), root, encoding));
      default:
        throw new IllegalStateException();
    }
  }

  public static long size(Iterable<ByteBuffer> bufs) {
    long size = 0;
    for (ByteBuffer b : bufs) {
      size += b.remaining();
    }
    return size;
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

    PriorityQueue<IEntry<Iterator<V>, V>> heap = new PriorityQueue<IEntry<Iterator<V>, V>>(Comparator.comparing(IEntry::value, comparator));
    for (Iterator<V> it : iterators) {
      if (it.hasNext()) {
        heap.add(IEntry.of(it, it.next()));
      }
    }

    return Iterators.from(
        () -> heap.size() > 0,
        () -> {
          IEntry<Iterator<V>, V> e = heap.poll();
          if (e == null) {
            throw new NoSuchElementException();
          }

          if (e.key().hasNext()) {
            heap.add(IEntry.of(e.key(), e.key().next()));
          }
          return e.value();
        });
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
    public final E encoding;
    public final LinearList<V> elements;

    public Block(E encoding, V first) {
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
          curr = new Block<>(encoding.apply(v), v);
        }

        int maxSize = blockSize.applyAsInt(curr.encoding);
        while (it.hasNext() && curr.elements.size() < maxSize) {
          V v = it.next();
          E nextEncoding = encoding.apply(v);
          if (isCollection.test(v) || !compatibleEncoding.test(curr.encoding, nextEncoding)) {
            next = new Block<>(nextEncoding, v);
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
