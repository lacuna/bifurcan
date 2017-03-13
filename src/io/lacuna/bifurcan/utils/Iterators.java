package io.lacuna.bifurcan.utils;

import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.IMap;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * @author ztellman
 */
public class Iterators {

  public static final Iterator EMPTY = new Iterator() {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Object next() {
      throw new NoSuchElementException();
    }
  };

  public static <U, V> Iterator<V> map(Iterator<U> it, Function<U, V> f) {
    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public V next() {
        return f.apply(it.next());
      }
    };
  }

  public static <U, V> Iterator<V> flatMap(Iterator<U> it, Function<U, Iterator<V>> f) {
    return new Iterator<V>() {

      Iterator<V> curr = EMPTY;

      private void prime() {
        while (!curr.hasNext() && it.hasNext()) {
          curr = f.apply(it.next());
        }
      }

      @Override
      public boolean hasNext() {
        prime();
        return curr.hasNext();
      }

      @Override
      public V next() {
        prime();
        return curr.next();
      }
    };
  }

  public static <V> Iterator<V> range(long min, long max, LongFunction<V> f) {
    return new Iterator<V>() {

      long i = min;

      @Override
      public boolean hasNext() {
        return i < max;
      }

      @Override
      public V next() {
        if (hasNext()) {
          return f.apply(i++);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public static <V> Iterator<V> range(long max, LongFunction<V> f) {
    return range(0, max, f);
  }
}
