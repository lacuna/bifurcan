package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.ISortedSet;

import java.util.Iterator;
import java.util.PrimitiveIterator;

public class Util {

  public static long removedPredecessors(ISortedSet<Long> removedIndices, long idx) {
    Long floor = removedIndices.floor(idx);
    if (floor == null) {
      return 0;
    } else if (floor == idx) {
      return -1;
    } else {
      return removedIndices.indexOf(floor);
    }
  }

  public static long offsetIndex(ISortedSet<Long> removedIndices, long idx) {
    Long floor = removedIndices.floor(idx);
    if (floor == null) {
      return idx;
    } else {
      return idx + removedIndices.indexOf(floor);
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
