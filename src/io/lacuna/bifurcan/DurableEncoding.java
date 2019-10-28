package io.lacuna.bifurcan;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

public interface DurableEncoding {

  /**
   * A plain-text description of the encoding, which is also used as its identity. All encodings sharing a name should
   * be equivalent.
   */
  String descriptor();

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
  default ToIntFunction<Object> hashFunction() {
    return Objects::hashCode;
  }

  /**
   * The encoding for `key` within a map or set.
   */
  default DurableEncoding keyEncoding(Object key) {
    throw new UnsupportedOperationException("Encoding '" + descriptor() + "' does not support maps");
  }

  /**
   * The encoding for the value corresponding to `key` within a map.
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
   * The encoding for the value stored at `index` within a list.
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
  default Iterator<Supplier<Object>> decode(DurableInput in) {
    throw new UnsupportedOperationException("Encoding '" + descriptor() + "' does not support primitives");
  }

  /**
   * Describes whether this encoding can be used with sorted collections.
   */
  default boolean hasOrdering() {
    return true;
  }

  default Comparator<Object> comparator() {
    return DurableEncoding::defaultComparator;
  }

  static Object defaultCoercion(Object o) {
    if (o instanceof java.util.Map) {
      return Maps.from((java.util.Map) o);
    } else if (o instanceof java.util.Set) {
      return Sets.from((java.util.Set) o);
    } else if (o instanceof java.util.List) {
      return Lists.from((java.util.List) o);
    } else {
      return o;
    }
  }

  static int defaultComparator(Object a, Object b) {
    if (a instanceof Comparable) {
      return ((Comparable) a).compareTo(b);
    } else {
      throw new IllegalArgumentException("No natural order defined for type '" + a.getClass().getSimpleName() + "'");
    }
  }
}
