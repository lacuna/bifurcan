package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.encodings.Tuple;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.ToIntFunction;

public interface DurableEncoding {

  interface SkippableIterator extends Iterator<Object> {
    static SkippableIterator singleton(Object o) {
      return new SkippableIterator() {
        boolean consumed = false;

        @Override
        public void skip() {
          consumed = true;
        }

        @Override
        public boolean hasNext() {
          return !consumed;
        }

        @Override
        public Object next() {
          consumed = true;
          return o;
        }
      };
    }

    default SkippableIterator skip(long n) {
      for (long i = 0; i < n; i++) {
        skip();
      }
      return this;
    }

    void skip();
  }

  static DurableEncoding list(String description, int blockSize, LongFunction<DurableEncoding> elementEncoding) {
    return new DurableEncoding() {
      @Override
      public String description() {
        return description;
      }

      @Override
      public int blockSize() {
        return blockSize;
      }

      @Override
      public boolean encodesLists() {
        return true;
      }

      @Override
      public DurableEncoding elementEncoding(long index) {
        return elementEncoding.apply(index);
      }
    };
  }

  static DurableEncoding tuple(DurableEncoding... encodings) {
    return new Tuple(encodings);
  }

  String description();
  
  /**
   * Describes whether this encoding can be used to encode maps (and implicitly sets, which are treated as maps without
   * values).
   */
  default boolean encodesMaps() {
    return false;
  }

  /**
   * The hash function used within maps and sets.
   */
  default ToIntFunction<Object> keyHash() {
    return Objects::hashCode;
  }

  /**
   * The key equality used within maps and sets.
   */
  default BiPredicate<Object, Object> keyEquality() {
    return Objects::equals;
  }

  /**
   * The encoding for any key in a map or set.
   */
  default DurableEncoding keyEncoding() {
    throw new UnsupportedOperationException(String.format("Encoding '%s' does not support maps", description()));
  }

  /**
   * The encoding for any value corresponding to `key` within a map.
   */
  default DurableEncoding valueEncoding(Object key) {
    throw new UnsupportedOperationException(String.format("Encoding '%s' does not support maps", description()));
  }

  /**
   * Describes whether this encoding can be used to encode lists.
   */
  default boolean encodesLists() {
    return false;
  }

  /**
   * The encoding for an element at `index` within a list.
   */
  default DurableEncoding elementEncoding(long index) {
    throw new UnsupportedOperationException(String.format("Encoding '%s' does not support lists", description()));
  }

  /**
   * Describes whether this encoding can be used to encode non-collection types.
   */
  default boolean encodesPrimitives() {
    return false;
  }

  /**
   * Describes the number of primitive values which should be encoded as a block.
   */
  default int blockSize() {
    return 32;
  }

  /**
   * Encodes the block of primitive values to `out`.
   */
  default void encode(IList<Object> primitives, DurableOutput out) {
    throw new UnsupportedOperationException(String.format("Encoding '%s' does not support primitives", description()));
  }

  /**
   * Decodes a block of primitive values, returning an iterator of thunks representing each individual value.
   */
  default SkippableIterator decode(DurableInput in) {
    throw new UnsupportedOperationException(String.format("Encoding '%s' does not support primitives", description()));
  }

  /**
   * Describes whether this encoding can be used with sorted collections.
   */
  default boolean hasKeyOrdering() {
    return true;
  }

  default Comparator<Object> keyComparator() {
    return DurableEncoding::defaultComparator;
  }

  static int defaultComparator(Object a, Object b) {
    if (a instanceof Comparable) {
      return ((Comparable) a).compareTo(b);
    } else {
      throw new IllegalArgumentException("No natural order defined for type '" + a.getClass().getSimpleName() + "'");
    }
  }
}
