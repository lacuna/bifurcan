package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IList<V> extends
        ILinearizable<IList<V>>,
        IForkable<IList<V>>,
        IPartitionable<IList<V>>,
        Iterable<V> {

  /**
   * @return a new list, with {@code rowValue} appended
   */
  IList<V> append(V value);

  /**
   * @return a new list, with the element at {@code idx} overwritten with {@code rowValue}.  If {@code idx} is equal to {@code size()}, the rowValue is appended.
   * @throws IndexOutOfBoundsException when {@code idx} is not within {@code [0, count]}
   */
  IList<V> set(long idx, V value);

  /**
   * @return the element at {@code idx}
   * @throws IndexOutOfBoundsException when {@code idx} is not within {@code [0, count-1]}
   */
  V nth(long idx);

  /**
   * @return the length of the list
   */
  long size();

  /**
   * @return a {@code java.util.stream.Stream}, representing the elements in the list.
   */
  default Stream<V> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  default Spliterator<V> spliterator() {
    return Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED);
  }

  default Iterator<V> iterator() {
    return new Iterator<V>() {

      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < size();
      }

      @Override
      public V next() {
        if (hasNext()) {
          return nth(idx++);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  /**
   * @return a read-only version of the list, which will throw an {@code UnsupportedOperationException} for any write
   */
  default IList<V> readOnly() {
    return Lists.readOnly(this);
  }

  /**
   * @return the elements of the list, in an array
   */
  default Object[] toArray() {
    Object[] ary = new Object[(int) size()];
    IntStream.range(0, ary.length).forEach(i -> ary[i] = nth(i));
    return ary;
  }

  /**
   * @param klass the component class of the list, which must be specified due to Java's impoverished type system
   * @return the elements of the list, in a typed array
   */
  default V[] toArray(Class<V> klass) {
    V[] ary = (V[]) Array.newInstance(klass, (int) size());
    IntStream.range(0, ary.length).forEach(i -> ary[i] = nth(i));
    return ary;
  }

  /**
   * @return the collection, represented as a normal Java {@code List}, which will throw an {@code UnsupportedOperationException} for any write
   */
  default java.util.List<V> toList() {
    return Lists.toList(this);
  }

  @Override
  default IList<IList<V>> partition(int parts) {
    IList<V>[] ary = new IList[parts];

    long subSize = size() / parts;
    long offset = 0;
    for (int i = 0; i < parts; i++) {
      ary[i] = Lists.subList(this, offset, i == (parts - 1) ? size() : offset + subSize);
    }

    return Lists.from(ary);
  }

  @Override
  default IList<V> merge(IList<V> collection) {
    return null;
  }

  default <U> IList<U> map(Function<V, U> f) {
    return Lists.from(size(), i -> f.apply(nth(i)));
  }

  default <U> U reduce(BiFunction<U, V, U> f, U initVal) {
    U val = initVal;
    for (V e : this) {
      val = f.apply(val, e);
    }
    return val;
  }
}
