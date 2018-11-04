package io.lacuna.bifurcan;

import io.lacuna.bifurcan.Lists.VirtualList;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IList<V> extends
  ICollection<IList<V>, V>,
  Iterable<V> {

  default IList<V> update(long idx, Function<V, V> updateFn) {
    return set(idx, updateFn.apply(nth(idx)));
  }

  /**
   * @return true, if the list is linear
   */
  default boolean isLinear() {
    return false;
  }

  /**
   * @return a new list, with {@code value} appended
   */
  default IList<V> addLast(V value) {
    return new VirtualList<>(this).addLast(value);
  }

  /**
   * @return a new list, with {@code value} prepended
   */
  default IList<V> addFirst(V value) {
    return new VirtualList<>(this).addFirst(value);
  }

  /**
   * @return a new list with the last value removed, or the same list if already empty
   */
  default IList<V> removeLast() {
    return new VirtualList<V>(this).removeLast();
  }

  /**
   * @return a new list with the first value removed, or the same value if already empty
   */
  default IList<V> removeFirst() {
    return new VirtualList<V>(this).removeFirst();
  }

  /**
   * @return a new list, with the element at {@code idx} overwritten with {@code value}. If {@code idx} is equal to {@code size()}, the value is appended.
   * @throws IndexOutOfBoundsException when {@code idx} is not within {@code [0, count]}
   */
  default IList<V> set(long idx, V value) {
    return new VirtualList<V>(this).set(idx, value);
  }

  /**
   * @return a {@code java.util.stream.Stream}, representing the elements in the list
   */
  default Stream<V> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  default Spliterator<V> spliterator() {
    return Spliterators.spliterator(iterator(), size(), Spliterator.ORDERED);
  }

  default Iterator<V> iterator() {
    return Lists.iterator(this);
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
   * @param allocator a function which creates an array of the specified size
   * @return the elements of the list, in a typed array
   */
  default V[] toArray(IntFunction<V[]> allocator) {
    V[] ary = allocator.apply((int) size());
    for (int i = 0; i < ary.length; i++) {
      ary[i] = nth(i);
    }
    return ary;
  }

  /**
   * @return the collection, represented as a normal Java {@code ListNodes}, which will throw an {@code UnsupportedOperationException} for any write
   */
  default java.util.List<V> toList() {
    return Lists.toList(this);
  }

  @Override
  default IList<IList<V>> split(int parts) {
    parts = Math.max(1, Math.min((int) size(), parts));
    IList<V>[] ary = new IList[parts];

    long subSize = size() / parts;
    long offset = 0;
    for (int i = 0; i < parts; i++) {
      ary[i] = Lists.slice(this, offset, i == (parts - 1) ? size() : offset + subSize);
      offset += subSize;
    }

    return Lists.from(ary);
  }

  /**
   * @param start the inclusive start of the range
   * @param end   the exclusive end of the range
   * @return a read-only view into this sub-range of the list
   */
  default IList<V> slice(long start, long end) {
    return Lists.slice(this, start, end);
  }

  /**
   * @param l another list
   * @return a new collection representing the concatenation of the two lists
   */
  default IList<V> concat(IList<V> l) {
    return Lists.concat(this, l);
  }

  /**
   * @return the first element
   * @throws IndexOutOfBoundsException if the collection is empty
   */
  default V first() {
    if (size() == 0) {
      throw new IndexOutOfBoundsException();
    }
    return nth(0);
  }

  /**
   * @return the last element
   * @throws IndexOutOfBoundsException if the collection is empty
   */
  default V last() {
    if (size() == 0) {
      throw new IndexOutOfBoundsException();
    }
    return nth(size() - 1);
  }

  @Override
  default IList<V> forked() {
    return this;
  }

  @Override
  default IList<V> linear() {
    return new VirtualList<V>(this).linear();
  }

  default boolean equals(Object o, BiPredicate<V, V> equals) {
    if (o instanceof IList) {
      return Lists.equals(this, (IList<V>) o, equals);
    }
    return false;
  }
}
