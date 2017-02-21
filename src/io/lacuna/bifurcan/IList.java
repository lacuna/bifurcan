package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IList<V> extends
    ISplittable<IList<V>>,
    Iterable<V>,
    IForkable<IList<V>>,
    ILinearizable<IList<V>> {

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
   * @return a new list, with {@code value} appended
   */
  default IList<V> addLast(V value) {
    return null;
  }

  /**
   * @return a new list, with {@code value} prepended
   */
  default IList<V> addFirst(V value) {
    return null;
  }

  /**
   * @return a new list with the last value removed, or the same list if already empty
   */
  default IList<V> removeLast() {
    return null;
  }

  /**
   * @return a new list with the first value removed, or the same value if already empty
   */
  default IList<V> removeFirst() {
    return null;
  }

  /**
   * @return a new list, with the element at {@code idx} overwritten with {@code value}. If {@code idx} is equal to {@code size()}, the value is appended.
   * @throws IndexOutOfBoundsException when {@code idx} is not within {@code [0, count]}
   */
  default IList<V> set(long idx, V value) {
    return null;
  }

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
  default IList<IList<V>> split(int parts) {
    IList<V>[] ary = new IList[parts];

    long subSize = size() / parts;
    long offset = 0;
    for (int i = 0; i < parts; i++) {
      ary[i] = Lists.subList(this, offset, i == (parts - 1) ? size() : offset + subSize);
    }

    return Lists.from(ary);
  }

  /**
   * @param start the inclusive start of the range
   * @param end the exclusive end of the range
   * @return a read-only view into this sub-range of the list
   */
  default IList<V> subList(long start, long end) {
    return Lists.subList(this, start, end);
  }

  /**
   * @param l another list
   * @return the read-only concatenation of the two lists
   */
  default IList<V> concat(IList<V> l) {
    return Lists.concat(this, l);
  }

  /**
   * @return the first element
   * @throws NoSuchElementException if the collection is empty
   */
  default V first() {
    if (size() == 0) {
      throw new NoSuchElementException();
    }
    return nth(0);
  }

  /**
   * @return the last element
   * @throws NoSuchElementException if the collection is empty
   */
  default V last() {
    if (size() == 0) {
      throw new NoSuchElementException();
    }
    return nth(size() - 1);
  }

  @Override
  default IList<V> forked() {
    return null;
  }

  @Override
  default IList<V> linear() {
    return null;
  }
}
