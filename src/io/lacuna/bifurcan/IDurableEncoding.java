package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public interface IDurableEncoding {

  String description();

  default Comparator<Object> comparator() {
    return IDurableEncoding::defaultComparator;
  }

  default ToIntFunction<Object> hashFn() {
    return Objects::hashCode;
  }

  default BiPredicate<Object, Object> equalityFn() {
    return Objects::equals;
  }

  default int blockSize() {
    return 32;
  }

  boolean isSingleton(Object o);

  interface Map extends IDurableEncoding {
    IDurableEncoding keyEncoding();

    IDurableEncoding valueEncoding();

    @Override
    default boolean isSingleton(Object o) {
      return true;
    }
  }

  interface Set extends IDurableEncoding {
    IDurableEncoding elementEncoding();

    @Override
    default boolean isSingleton(Object o) {
      return true;
    }
  }

  interface List extends IDurableEncoding {
    IDurableEncoding elementEncoding();

    @Override
    default boolean isSingleton(Object o) {
      return true;
    }
  }

  interface Primitive extends IDurableEncoding {
    void encode(IList<Object> values, DurableOutput out);

    SkippableIterator decode(DurableInput in, IDurableCollection.Root root);

    @Override
    default boolean isSingleton(Object o) {
      return false;
    }
  }

  interface Unityped extends Map, Set, List, Primitive {
    @Override
    default boolean isSingleton(Object o) {
      return o instanceof ICollection;
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
