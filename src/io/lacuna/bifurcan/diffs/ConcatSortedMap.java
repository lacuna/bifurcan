package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.OptionalLong;
import java.util.function.BinaryOperator;

public class ConcatSortedMap<K, V> extends ISortedMap.Mixin<K, V> implements IDiffSortedMap<K, V> {

  private Comparator<K> comparator;
  private ISortedMap<K, ISortedMap<K, V>> segments;
  private ISortedSet<Long> segmentOffsets;
  private final boolean isLinear;

  private ConcatSortedMap(Comparator<K> comparator, ISortedMap<K, ISortedMap<K, V>> segments, ISortedSet<Long> segmentOffsets, boolean isLinear) {
    this.comparator = comparator;
    this.segments = segments;
    this.segmentOffsets = segmentOffsets;
    this.isLinear = isLinear;
  }

  public static <K, V> ConcatSortedMap<K, V> from(ISortedMap<K, V> m) {
    return from(m.comparator(), LinearList.of(m));
  }

  public static <K, V> ConcatSortedMap<K, V> from(Comparator<K> comparator, ISortedMap<K, V>... segments) {
    return from(comparator, LinearList.of(segments));
  }

  public static <K, V> ConcatSortedMap<K, V> from(Comparator<K> comparator, IList<ISortedMap<K, V>> segments) {
    int n = 0;
    for (ISortedMap<K, V> s : segments) {
      if (s.size() > 0) {
        n++;
      }
    }

    ISortedMap<K, ISortedMap<K, V>> m = new SortedMap<>(comparator);
    long[] o = new long[n];

    int i = 0;
    long size = 0;
    for (ISortedMap<K, V> s : segments) {
      if (s.size() > 0) {
        o[i++] = size;
        size += s.size();
        m = m.put(s.first().key(), s.forked());
      }
    }

    ISortedSet<Long> s = Sets.from(Lists.from(o.length, idx -> o[(int) idx]), Long::compare, idx -> aryFloorIndex(o, idx));
    return new ConcatSortedMap<K, V>(comparator, m, s, false);
  }

  @Override
  public Comparator<K> comparator() {
    return comparator;
  }

  @Override
  public ISortedMap<K, ISortedMap<K, V>> segments() {
    return segments;
  }

  @Override
  public ISortedSet<Long> segmentOffsets() {
    return segmentOffsets;
  }

  @Override
  public ConcatSortedMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    ConcatSortedMap<K, V> result;

    OptionalLong oIdx = floorIndex(key);
    if (!oIdx.isPresent()) {
      result = size() > 0 && segments.first().value() instanceof SortedMap
          ? from(comparator, segments.values().removeFirst().addFirst(segments.values().first().put(key, value)))
          : from(comparator, segments.values().addFirst(new SortedMap<K, V>().put(key, value)));
    } else {
      long idx = oIdx.getAsLong();
      boolean overwrite = keyEquality().test(key, nth(idx).key());
      value = overwrite ? merge.apply(nth(idx).value(), value) : value;

      IList<ISortedMap<K, V>> segments = (overwrite ? belowExclusive(idx) : belowInclusive(idx)).segments().values();
      if (segments.size() > 0 && segments.last() instanceof SortedMap) {
        ISortedMap<K, V> s = segments.last().put(key, value);
        segments = segments.removeLast().addLast(s);
      } else {
        segments = segments.addLast(new SortedMap<K, V>(comparator).put(key, value));
      }
      segments = segments.concat(aboveExclusive(idx).segments().values());

      result = from(comparator, segments);
    }

    if (isLinear) {
      super.hash = -1;
      this.segments = result.segments;
      this.segmentOffsets = result.segmentOffsets;
      return this;
    } else {
      return result;
    }
  }

  @Override
  public ConcatSortedMap<K, V> remove(K key) {
    OptionalLong oIdx = floorIndex(key);
    if (oIdx.isPresent() && keyEquality().test(key, nth(oIdx.getAsLong()).key())) {
      ConcatSortedMap<K, V> result = from(comparator, belowExclusive(oIdx.getAsLong()), aboveExclusive(oIdx.getAsLong()));
      if (isLinear) {
        super.hash = -1;
        this.segments = result.segments;
        this.segmentOffsets = result.segmentOffsets;
      } else {
        return result;
      }
    }

    return this;
  }

  @Override
  public ConcatSortedMap<K, V> slice(K min, K max) {
    OptionalLong oMinIdx = segments.floorIndex(min);
    OptionalLong oMaxIdx = segments.floorIndex(max);

    LinearList<ISortedMap<K, V>> acc = new LinearList<>();
    long minIdx = oMinIdx.isPresent() ? oMinIdx.getAsLong() : -1;
    long maxIdx = oMaxIdx.isPresent() ? oMaxIdx.getAsLong() : segments.size();

    if (minIdx >= 0) {
      acc.addLast(segments.nth(minIdx).value().slice(min, max));
    }

    for (long i = minIdx + 1; i < maxIdx; i++) {
      acc.addLast(segments.nth(i).value());
    }

    if (maxIdx < segments.size() && minIdx < maxIdx) {
      acc.addLast(segments.nth(maxIdx).value().slice(min, max));
    }

    return from(comparator, acc);
  }

  @Override
  public ConcatSortedMap<K, V> forked() {
    return isLinear ? new ConcatSortedMap<>(comparator, segments, segmentOffsets, false) : this;
  }

  @Override
  public ConcatSortedMap<K, V> linear() {
    return isLinear ? this : new ConcatSortedMap<>(comparator, segments, segmentOffsets, true);
  }

  @Override
  public boolean isLinear() {
    return isLinear;
  }

  @Override
  public ConcatSortedMap<K, V> clone() {
    return isLinear() ? forked().linear() : this;
  }

  ///

  private ConcatSortedMap<K, V> belowExclusive(long idx) {
    return idx == 0 ? from(comparator) : slice(first().key(), nth(idx - 1).key());
  }

  private ConcatSortedMap<K, V> aboveExclusive(long idx) {
    return idx == size() - 1 ? from(comparator) : slice(nth(idx + 1).key(), last().key());
  }

  private ConcatSortedMap<K, V> belowInclusive(long idx) {
    return slice(first().key(), nth(idx).key());
  }

  private ConcatSortedMap<K, V> aboveInclusive(long idx) {
    return slice(nth(idx).key(), last().key());
  }

  private static OptionalLong aryFloorIndex(long[] offsets, long index) {
    int idx = Arrays.binarySearch(offsets, index);
    if (idx == -1) {
      return OptionalLong.empty();
    } else {
      return OptionalLong.of(idx < 0 ? -idx - 2 : idx);
    }
  }
}
