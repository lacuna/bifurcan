package io.lacuna.bifurcan.utils;

import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.LinearList;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

  public static <V> Iterator<V> mergeSort(IList<Iterator<V>> iterators, Comparator<V> comparator) {

    if (iterators.size() == 1) {
      return iterators.first();
    }

    PriorityQueue<IEntry<V, Iterator<V>>> heap = new PriorityQueue<>(Comparator.comparing(IEntry::key, comparator));
    for (Iterator<V> it : iterators) {
      if (it.hasNext()) {
        heap.add(IEntry.of(it.next(), it));
      }
    }

    return from(
        () -> heap.size() > 0,
        () -> {
          IEntry<V, Iterator<V>> e = heap.poll();
          if (e.value().hasNext()) {
            heap.add(IEntry.of(e.value().next(), e.value()));
          }
          return e.key();
        }
    );
  }

  /**
   * A utility class for dynamically appending and prepending iterators to a collection, which itself can be iterated
   * over.
   */
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

  public static <V> boolean equals(Iterator<V> a, Iterator<V> b, BiPredicate<V, V> equals) {
    while (a.hasNext()) {
      if (!equals.test(a.next(), b.next())) {
        return false;
      }
    }
    return true;
  }

  public static <V> Iterator<V> from(BooleanSupplier hasNext, Supplier<V> next) {
    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return hasNext.getAsBoolean();
      }

      @Override
      public V next() {
        return next.get();
      }
    };
  }

  public static <V> Iterator<V> onExhaustion(Iterator<V> it, Runnable f) {
    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public V next() {
        V result = it.next();
        if (!it.hasNext()) {
          f.run();
        }
        return result;
      }
    };
  }

  public static <V> Iterator<V> singleton(V val) {
    return new Iterator<V>() {
      boolean consumed = false;

      @Override
      public boolean hasNext() {
        return !consumed;
      }

      @Override
      public V next() {
        if (!consumed) {
          consumed = true;
          return val;
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  /**
   * @param it an iterator
   * @param f  a predicate
   * @return an iterator which only yields values that satisfy the predicate
   */
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
        if (next == NONE) {
          throw new NoSuchElementException();
        }

        V val = (V) next;
        next = NONE;
        return val;
      }
    };
  }

  /**
   * @param it an iterator
   * @param f  a function which transforms values
   * @return an iterator which yields the transformed values
   */
  public static <U, V> Iterator<V> map(Iterator<U> it, Function<U, V> f) {
    return from(it::hasNext, () -> f.apply(it.next()));
  }

  /**
   * @param it an iterator
   * @param f  a function which transforms values into iterators
   * @return an iterator which yields the concatenation of the iterators
   */
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

  /**
   * @param min an inclusive start of the range
   * @param max an exclusive end of the range
   * @param f   a function which transforms a number in the range into a value
   * @return an iterator which yields the values returned by {@code f}
   */
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

  /**
   * Represents a range implicitly starting at 0.
   *
   * @param max an exclusive end of the range.
   * @param f   a function which transforms a number in the range into a value.
   * @return an iterator which yields the values returned by {@code f}
   */
  public static <V> Iterator<V> range(long max, LongFunction<V> f) {
    return range(0, max, f);
  }

  /**
   * @param iterators a list of iterators
   * @return a concatenation of all iterators, in the order provided
   */
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

  /**
   * @param it an iterator
   * @param n  the number of elements to drop, which may be larger than the number of values in the iterator
   * @return an iterator with the first {@code n} values dropped
   */
  public static <V> Iterator<V> drop(Iterator<V> it, long n) {
    for (long i = 0; i < n && it.hasNext(); i++) {
      it.next();
    }
    return it;
  }

  public static <V> Stream<V> toStream(Iterator<V> it) {
    return toStream(it, 0);
  }

  public static <V> Stream<V> toStream(Iterator<V> it, long estimatedSize) {
    return StreamSupport.stream(Spliterators.spliterator(it, estimatedSize, Spliterator.ORDERED), false);
  }

  public static class Indexed<T> {
    public final long index;
    public final T value;

    public Indexed(long index, T value) {
      this.index = index;
      this.value = value;
    }

    @Override
    public String toString() {
      return index + ": " + value;
    }
  }

  public static <V> Iterator<Indexed<V>> indexed(Iterator<V> it) {
    return indexed(it, 0);
  }

  public static <V> Iterator<Indexed<V>> indexed(Iterator<V> it, long offset) {
    AtomicLong counter = new AtomicLong(offset);
    return map(it, v -> new Indexed<>(counter.getAndIncrement(), v));
  }

}
