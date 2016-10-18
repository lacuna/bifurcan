package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author ztellman
 */
public interface IList<V> extends
        ILinearizable<IList<V>>,
        IForkable<IList<V>>,
        IMergeable<IList<V>>,
        Iterable<V> {

  /**
   * @return a new list, with {@code value} appended
   */
  IList<V> append(V value);

  /**
   * @return a new list, with the element at {@code idx} overwritten with {@code value}
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
   * @return the collection, represented as a normal Java {@code List}, without support for writes
   */
  java.util.List<V> toList();

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
          return nth(++idx);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }
}
