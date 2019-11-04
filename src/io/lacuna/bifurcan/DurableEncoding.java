package io.lacuna.bifurcan;

import io.lacuna.bifurcan.allocator.SlabAllocator;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.DurableAccumulator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.*;

public interface DurableEncoding {

  class Descriptor {
    public final String id;

    public Descriptor(String id) {
      this.id = id.intern();
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Descriptor) {
        return id == ((Descriptor) obj).id;
      } else {
        return false;
      }
    }
  }

  interface SkippableIterator extends Iterator<Object> {
    default SkippableIterator skip(int n) {
      for (int i = 0; i < n; i++) {
        skip();
      }
      return this;
    }

    void skip();
  }

  /**
   * A plain-text description of the encoding, which is also used as its identity. All encodings sharing a name should
   * be equivalent.
   */
  Descriptor descriptor();

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
    throw new UnsupportedOperationException("Encoding '" + descriptor() + "' does not support maps");
  }

  /**
   * The encoding for any value corresponding to `key` within a map.
   */
  default DurableEncoding valueEncoding(Object key) {
    throw new UnsupportedOperationException("Encoding '" + descriptor() + "' does not support maps");
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
    throw new UnsupportedOperationException("Encoding '" + descriptor() + "' does not support lists");
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
    return 16;
  }

  /**
   * Encodes the block of primitive values to `out`.
   */
  default void encode(IList<Object> primitives, DurableOutput out) {
    throw new UnsupportedOperationException("Encoding '" + descriptor() + "' does not support primitives");
  }

  /**
   * Decodes a block of primitive values, returning an iterator of thunks representing each individual value.
   */
  default SkippableIterator decode(DurableInput in) {
    throw new UnsupportedOperationException("Encoding '" + descriptor() + "' does not support primitives");
  }

  /**
   * Describes whether this encoding can be used with sorted collections.
   */
  default boolean hasOrdering() {
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
