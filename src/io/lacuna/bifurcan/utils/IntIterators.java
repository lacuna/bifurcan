package io.lacuna.bifurcan.utils;

import io.lacuna.bifurcan.LinearList;

import java.util.*;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ztellman
 */
public class IntIterators {

  public static final OfInt EMPTY = new OfInt() {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public int nextInt() {
      throw new NoSuchElementException();
    }
  };

  public static boolean equals(OfInt a, OfInt b) {
    while (a.hasNext()) {
      if (a.nextInt() != b.nextInt()) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param it an iterator
   * @param f a predicate
   * @return an iterator which only yields values that satisfy the predicate
   */
  public static OfInt filter(OfInt it, IntPredicate f) {
    return new OfInt() {

      private int next = 0;
      private boolean primed = false;
      private boolean done = false;

      private void prime() {
        if (!primed && !done) {
          while (it.hasNext()) {
            next = it.nextInt();
            if (f.test(next)) {
              primed = true;
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
      public int nextInt() {
        prime();
        if (!primed) {
          throw new NoSuchElementException();
        }

        primed = false;
        return next;
      }
    };
  }

  /**
   * @param it an iterator
   * @param f a function which transforms values
   * @return an iterator which yields the transformed values
   */
  public static OfInt map(OfInt it, IntUnaryOperator f) {
    return new OfInt() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public int nextInt() {
        return f.applyAsInt(it.nextInt());
      }
    };
  }

  /**
   * @param it an iterator
   * @param f a function which transforms values into iterators
   * @return an iterator which yields the concatenation of the iterators
   */
  public static <U> OfInt flatMap(Iterator<U> it, Function<U, OfInt> f) {
    return new OfInt() {

      OfInt curr = EMPTY;

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
      public int nextInt() {
        prime();
        return curr.nextInt();
      }
    };
  }

  /**
   * @param min an inclusive start of the range
   * @param max an exclusive end of the range
   * @param f a function which transforms a number in the range into a value
   * @return an iterator which yields the values returned by {@code f}
   */
  public static OfInt range(long min, long max, LongToIntFunction f) {
    return new OfInt() {

      long i = min;

      @Override
      public boolean hasNext() {
        return i < max;
      }

      @Override
      public int nextInt() {
        if (hasNext()) {
          return f.applyAsInt(i++);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public static OfInt deltas(OfInt it) {
    return new OfInt() {
      int curr = it.nextInt();

      @Override
      public int nextInt() {
        int prev = curr;
        this.curr = it.nextInt();
        return curr - prev;
      }

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }
    };
  }

  /**
   * Represents a range implicitly starting at 0.
   *
   * @param max an exclusive end of the range.
   * @param f a function which transforms a number in the range into a value.
   * @return an iterator which yields the values returned by {@code f}
   */
  public static OfInt range(long max, LongToIntFunction f) {
    return range(0, max, f);
  }

  public static IntStream toStream(OfInt it, long estimatedSize) {
    return StreamSupport.intStream(Spliterators.spliterator(it, estimatedSize, Spliterator.ORDERED), false);
  }
}
