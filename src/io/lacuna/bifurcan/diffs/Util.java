package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.ISortedSet;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;

/**
 * @author ztellman
 */
public class Util {

  /**
   * @return the number of removed entries before {@code idx}, unless {@code idx} itself is removed
   */
  public static OptionalLong removedPredecessors(ISortedSet<Long> removedIndices, long idx) {
    Long floor = removedIndices.floor(idx);
    if (floor == null) {
      return OptionalLong.of(0);
    } else if (floor == idx) {
      return OptionalLong.empty();
    } else {
      return OptionalLong.of(removedIndices.indexOf(floor).getAsLong() + 1);
    }
  }

  public static long offsetIndex(ISortedSet<Long> removedIndices, long idx) {
    Long floor = removedIndices.floor(idx);
    if (floor == null) {
      return idx;
    } else {
      long estimate = idx;
      // TODO: this can get linear for long contiguous blocks of indices, is there a better (but still simple) index?
      for (;;) {
        long actual = estimate - (removedIndices.indexOf(floor).getAsLong() + 1);
        if (actual == idx) {
          return estimate;
        } else if (actual < idx) {
          estimate += idx - actual;
          floor = removedIndices.floor(estimate);
        } else {
          throw new IllegalStateException("we overshot, somehow");
        }
      }
    }
  }

  public static <V> Iterator<V> skipIndices(Iterator<V> it, Iterator<Long> skippedIndices) {
    return new Iterator<V>() {

      long nextIndex = 0;
      long nextSkippedIndex = nextSkippedIndex();

      private long nextSkippedIndex() {
        return skippedIndices.hasNext() ? skippedIndices.next() : -1;
      }

      private V nextUnderlying() {
        nextIndex++;
        return it.next();
      }

      void prime() {
        while (nextSkippedIndex == nextIndex && it.hasNext()) {
          nextUnderlying();
          nextSkippedIndex = nextSkippedIndex();
        }
      }

      @Override
      public boolean hasNext() {
        prime();
        return it.hasNext();
      }

      @Override
      public V next() {
        prime();
        return nextUnderlying();
      }
    };
  }
}
