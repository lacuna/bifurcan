package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.IDiffMap;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.ISortedSet;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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

  /**
   * Given an index on a collection which is an underlying collection with some indices removed, return the corresponding
   * index within that underlying collection.
   */
  public static long offsetIndex(ISortedSet<Long> removedIndices, long idx) {
    Long floor = removedIndices.floor(idx);
    if (floor == null) {
      return idx;
    } else {
      long estimate = idx;
      // TODO: this can get linear for long contiguous blocks of indices, is there a better (but still simple) index?
      // barring that, use a binary search
      for (; ; ) {
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

  /**
   * Given a sequence and a set of indices to skip, return the sequence with those indices omitted.
   */
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

  /**
   * Given a stack of collections, each removing some number of indices from the previous one, return the combined
   * sequence of removed indices from the collection underlying the first element.
   */
  public static PrimitiveIterator.OfLong mergedRemovedIndices(IList<Iterator<Long>> iteratorStack) {
    int stackSize = (int) iteratorStack.size();
    return new PrimitiveIterator.OfLong() {
      final long[] offsets = new long[stackSize];
      final long[] nextIndices = new long[stackSize];
      boolean hasNext = true;

      {
        for (int i = 0; i < stackSize; i++) {
          Iterator<Long> it = iteratorStack.nth(i);
          nextIndices[i] = it.hasNext() ? it.next() : -1;
        }
        checkHasNext();
      }

      private void checkHasNext() {
        for (int i = 0; i < stackSize; i++) {
          if (nextIndices[i] != -1 || iteratorStack.nth(i).hasNext()) {
            return;
          }
        }
        hasNext = false;
      }

      @Override
      public long nextLong() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }

        int minIndex = -1;
        long minNext = Long.MAX_VALUE;
        for (int i = 0; i < stackSize; i++) {
          if (nextIndices[i] >= 0) {
            long next = offsets[i] + nextIndices[i];
            if (next < minNext) {
              minIndex = i;
              minNext = next;
            }
          }
        }

        if (minIndex >= 0) {
          for (int i = minIndex + 1; i < stackSize; i++) {
            offsets[i]++;
          }

          if (iteratorStack.nth(minIndex).hasNext()) {
            nextIndices[minIndex] = iteratorStack.nth(minIndex).next();
          } else {
            nextIndices[minIndex] = -1;
            checkHasNext();
          }
        }

        return minNext;
      }

      @Override
      public boolean hasNext() {
        return hasNext;
      }
    };
  }
}
