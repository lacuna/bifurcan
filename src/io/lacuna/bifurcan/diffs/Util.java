package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.ISortedSet;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;

public class Util {

  /**
   * @return the number of removed entries before the idx, unless {@code idx} itself is removed
   */
  public static OptionalLong removedPredecessors(ISortedSet<Long> removedIndices, long idx) {
    Long floor = removedIndices.floor(idx);
    if (floor == null) {
      return OptionalLong.of(0);
    } else if (floor == idx) {
      return OptionalLong.empty();
    } else {
      return removedIndices.indexOf(floor);
    }
  }

  public static long offsetIndex(ISortedSet<Long> removedIndices, long idx) {
    Long floor = removedIndices.floor(idx);
    if (floor == null) {
      return idx;
    } else {
      return idx + removedIndices.indexOf(floor).getAsLong();
    }
  }

  public static <V> Iterator<V> skipIndices(Iterator<V> it, Iterator<Long> skippedIndices) {
    return new Iterator<V>() {

      long nextIndex = 0;
      long nextSkippedIndex = nextSkippedIndex();

      private long nextSkippedIndex() {
        return skippedIndices.hasNext() ? skippedIndices.next() : -1;
      }

      void prime() {
        while (nextSkippedIndex == nextIndex && it.hasNext()) {
          nextSkippedIndex = nextSkippedIndex();
          nextIndex++;
          it.next();
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
        return it.next();
      }
    };
  }
}
