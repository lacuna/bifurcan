package io.lacuna.bifurcan.utils;

import clojure.lang.Obj;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.LinearList;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Predicate;

/**
 * @author ztellman
 */
public class Iterators {

  private static final Object NONE = new Object();

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

  public static class IteratorStack<V> implements Iterator<V> {

    LinearList<Iterator<V>> iterators = new LinearList<>();

    public IteratorStack() {
    }

    public IteratorStack(Iterator<V>... its) {
      for (Iterator<V> it : its) {
        iterators.addFirst(it);
      }
    }

    private void primeIterator() {
      while (iterators.size() > 0 && !iterators.first().hasNext()) {
        iterators.removeFirst();
      }
    }

    @Override
    public boolean hasNext() {
      primeIterator();
      return iterators.size() > 0 && iterators.first().hasNext();
    }

    @Override
    public V next() {
      primeIterator();
      if (iterators.size() == 0) {
        throw new NoSuchElementException();
      }
      return iterators.first().next();
    }

    public void addFirst(Iterator<V> it) {
      iterators.addFirst(it);
    }

    public void addLast(Iterator<V> it) {
      iterators.addLast(it);
    }
  }

  public static <V> Iterator<V> filter(Iterator<V> it, Predicate<V> f) {
    return new Iterator<V>() {

      private Object next = NONE;
      private boolean done = false;

      private void prime() {
        if (next == NONE && !done) {
          while (it.hasNext()) {
            next = it.next();
            if (f.test((V) next)) {
              return;
            }
          }
          done = true;
        }
      }

      @Override
      public boolean hasNext() {
        prime();
        return !done;
      }

      @Override
      public V next() {
        prime();
        V val = (V) next;
        next = NONE;
        return val;
      }
    };
  }

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

  public static <V> Iterator<V> concat(Iterator<V>... iterators) {
    if (iterators.length == 1) {
      return iterators[0];
    } else {
      IteratorStack<V> stack = new IteratorStack<V>();
      for (Iterator<V> it : iterators) {
        stack.addLast(it);
      }
      return stack;
    }
  }
}
