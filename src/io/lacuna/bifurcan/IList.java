package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author ztellman
 */
public interface IList<V> {

  IList<V> append(V value);

  IList<V> set(long idx, V value);

  V nth(long idx);

  long size();

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
