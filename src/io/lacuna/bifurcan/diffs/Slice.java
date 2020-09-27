package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.ISortedSet.Bound;

public class Slice {

  public static class SortedMap<K, V> extends ISortedMap.Mixin<K, V> implements IDiffSortedMap<K, V> {

    private static final IntSet OFFSETS = new IntSet().add(0L);

    private final K min, max;
    private final Bound minBound, maxBound;
    private final ISortedMap<K, V> underlying;

    private final ISortedMap<K, ISortedMap<K, V>> segments;

    public SortedMap(ISortedMap<K, V> underlying, K min, Bound minBound, K max, Bound maxBound) {
      this.min = min;
      this.minBound = minBound;
      this.max = max;
      this.maxBound = maxBound;
      this.underlying = underlying;

      ISortedMap<K, V> sliced = underlying.sliceIndices(
          underlying.ceilIndex(min, minBound).orElse(0),
          underlying.floorIndex(max, maxBound).orElse(underlying.size())
      );
      ISortedMap<K, ISortedMap<K, V>> segments = new io.lacuna.bifurcan.SortedMap<>(underlying.comparator());
      if (sliced.size() > 0) {
        segments = segments.put(sliced.entries().first().key(), sliced);
      }
      this.segments = segments;
    }

    @Override
    public SortedMap<K, V> slice(K min, Bound minBound, K max, Bound maxBound) {
      int minCmp = comparator().compare(this.min, min);
      int maxCmp = comparator().compare(this.max, max);
      if (minCmp == 0) {
        minBound = this.minBound == Bound.EXCLUSIVE || minBound == Bound.EXCLUSIVE ? Bound.EXCLUSIVE : Bound.INCLUSIVE;
      } else if (minCmp > 0) {
        min = this.min;
        minBound = this.minBound;
      }

      if (maxCmp == 0) {
        minBound = this.minBound == Bound.EXCLUSIVE || minBound == Bound.EXCLUSIVE ? Bound.EXCLUSIVE : Bound.INCLUSIVE;
      } else if (maxCmp < 0) {
        max = this.max;
        maxBound = this.maxBound;
      }

      return new SortedMap<>(underlying, min, minBound, max, maxBound);
    }

    @Override
    public ISortedMap<K, V> underlying() {
      return underlying;
    }

    @Override
    public ISortedMap<K, ISortedMap<K, V>> segments() {
      return segments;
    }

    @Override
    public ISortedSet<Long> segmentOffsets() {
      return OFFSETS;
    }

    @Override
    public SortedMap<K, V> rebase(ISortedMap<K, V> newUnderlying) {
      // if we're a slice atop another diff, rebase the underlying diff
      newUnderlying = underlying instanceof IDiffSortedMap ?
          ((IDiffSortedMap<K, V>) underlying).rebase(newUnderlying)
          : newUnderlying;
      return new SortedMap<>(newUnderlying, min, minBound, max, maxBound);
    }
  }

  public static class SortedSet<V> extends ISortedSet.Mixin<V> implements IDiffSortedSet<V> {
    private final SortedMap<V, Void> diffMap;

    public SortedSet(V min, Bound minBound, V max, Bound maxBound, ISortedMap<V, Void> underlying) {
      this(new SortedMap<>(underlying, min, minBound, max, maxBound));
    }

    private SortedSet(SortedMap<V, Void> diffMap) {
      this.diffMap = diffMap;
    }

    @Override
    public IDiffSortedMap<V, Void> diffMap() {
      return diffMap;
    }

    @Override
    public IDiffSortedSet<V> slice(V min, Bound minBound, V max, Bound maxBound) {
      return new SortedSet<>(diffMap.slice(min, minBound, max, maxBound));
    }

    @Override
    public IDiffSortedSet<V> rebase(ISortedSet<V> newUnderlying) {
      return new SortedSet<>(diffMap.rebase(newUnderlying.zip(x -> null)));
    }
  }
}
