package io.lacuna.bifurcan;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;

/**
 * A type descriptor for an encoded value.  These may be collections, primitives, or both (see {@link Unityped}).
 * <p>
 * For collections, we need only describe the encoding of the individual values.  For primitives, we must define several things:
 * <ul>
 * <li>{@link Primitive#blockSize()}, which is the number of values which can be typically encoded in ~2kb</li>
 * <li>{@link Primitive#encode(IList, DurableOutput)}, which takes a list of values and writes them as a single block to the output</li>
 * <li>{@link Primitive#decode(DurableInput, IDurableCollection.Root)} which takes an input corresponding to an encoded block, and returns a special iterator that can skip over ignored values</li>
 * </ul>
 * <p>
 * Optionally, encodings may provide custom hash, equality, and comparison semantics.
 * <p>
 * One should not expect a 1:1 correspondence between encodings and in-memory types.  Where an in-memory type only relates
 * to the qualities of an individual value, an encoding must also consider the relationships between neighboring values.
 * Integers which represent incrementing ids, for instance, are much more compressible than arbitrary integers.
 * <p>
 * In general, one should always use the constructors in {@link DurableEncodings} to define a new encoding.
 */
public interface IDurableEncoding {

  /**
   * A description of the encoding.
   */
  String description();

  /**
   * The comparator used for decoded values.
   */
  default Comparator<Object> comparator() {
    return IDurableEncoding::defaultComparator;
  }

  /**
   * The hash function used for decoded values.
   */
  default ToLongFunction<Object> hashFn() {
    return Objects::hashCode;
  }

  /**
   * The equality function used for decoded values.
   */
  default BiPredicate<Object, Object> equalityFn() {
    return Objects::equals;
  }

  /**
   * A predicate that allows us to call out values we want to be stored individually.  This is useful for any value which
   * we do not intend to compress with neighboring values, which enables lazy decoding.  All collections are necessarily
   * singletons.
   */
  boolean isSingleton(Object o);

  /**
   * A map, comprised of a {@link #keyEncoding()} and {@link #valueEncoding()}.
   */
  interface Map extends IDurableEncoding {
    IDurableEncoding keyEncoding();

    IDurableEncoding valueEncoding();

    @Override
    default boolean isSingleton(Object o) {
      return true;
    }
  }

  /**
   * A set, comprised of a single {@link #elementEncoding()}.
   */
  interface Set extends IDurableEncoding {
    IDurableEncoding elementEncoding();

    @Override
    default boolean isSingleton(Object o) {
      return true;
    }
  }

  /**
   * A list comprised of a single {@link #elementEncoding()}.
   */
  interface List extends IDurableEncoding {
    IDurableEncoding elementEncoding();

    @Override
    default boolean isSingleton(Object o) {
      return true;
    }
  }

  /**
   * A primitive, which can encode and decode blocks of values.
   */
  interface Primitive extends IDurableEncoding {
    default int blockSize() {
      return 16;
    }

    void encode(IList<Object> values, DurableOutput out);

    SkippableIterator decode(DurableInput in, IDurableCollection.Root root);

    @Override
    default boolean isSingleton(Object o) {
      return false;
    }
  }

  /**
   * An encoding which is all things to all people.  This is possible when a single encoding can handle all primitive
   * values, either because they are consistently typed or a self-describing encoding such as JSON is used.
   */
  interface Unityped extends Map, Set, List, Primitive {
    @Override
    default boolean isSingleton(Object o) {
      return o instanceof ICollection || o instanceof java.util.Collection;
    }

    @Override
    default IDurableEncoding keyEncoding() {
      return this;
    }

    @Override
    default IDurableEncoding valueEncoding() {
      return this;
    }

    @Override
    default IDurableEncoding elementEncoding() {
      return this;
    }
  }

  /**
   * An extension of {@link Iterator}, which provides for skipping elements without ever providing a value.  This can allow
   * for encoded values being skipped without ever being decoded.
   */
  interface SkippableIterator extends Iterator<Object> {
    void skip();

    default SkippableIterator skip(long n) {
      for (long i = 0; i < n && hasNext(); i++) {
        skip();
      }
      return this;
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
