package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.OptionalLong;
import java.util.function.BinaryOperator;

public class ConcatSortedMap<K, V> extends ISortedMap.Mixin<K, V> implements IDiffSortedMap<K, V> {

  private ISortedMap<K, V> underlying;
  private ISortedMap<K, ISortedMap<K, V>> segments;
  private ISortedSet<Long> segmentOffsets;
  private final boolean isLinear;

  private ConcatSortedMap(
      ISortedMap<K, V> underlying,
      ISortedMap<K, ISortedMap<K, V>> segments,
      ISortedSet<Long> segmentOffsets,
      boolean isLinear
  ) {
    this.underlying = underlying;
    this.segments = segments;
    this.segmentOffsets = segmentOffsets;
    this.isLinear = isLinear;
  }

  public static <K, V> ConcatSortedMap<K, V> from(ISortedMap<K, V> underlying) {
    return from(underlying, LinearList.of(underlying));
  }

  public static <K, V> ConcatSortedMap<K, V> from(ISortedMap<K, V> underlying, ISortedMap<K, V>... segments) {
    return from(underlying, LinearList.of(segments));
  }

  public static <K, V> ConcatSortedMap<K, V> from(ISortedMap<K, V> underlying, IList<ISortedMap<K, V>> segments) {
    int n = 0;
    for (ISortedMap<K, V> s : segments) {
      if (s.size() > 0) {
        n++;
      }
    }

    ISortedMap<K, ISortedMap<K, V>> m = new SortedMap<>(underlying.comparator());
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

    ISortedSet<Long> s = Sets.from(
        Lists.from(o.length, idx -> o[(int) idx]),
        Long::compare,
        idx -> aryFloorIndex(o, idx)
    );
    return new ConcatSortedMap<>(underlying, m, s, false);
  }

  @Override
  public ISortedMap<K, V> underlying() {
    return underlying;
  }

  @Override
  public Comparator<K> comparator() {
    return underlying.comparator();
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
  public ConcatSortedMap<K, V> rebase(ISortedMap<K, V> newUnderlying) {
    // TODO: implement
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcatSortedMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    ConcatSortedMap<K, V> result;

    OptionalLong oIdx = inclusiveFloorIndex(key);
    if (!oIdx.isPresent()) {
      result = size() > 0 && segments.first().value() instanceof SortedMap
          ? from(underlying, segments.values().removeFirst().addFirst(segments.values().first().put(key, value)))
          : from(underlying, segments.values().addFirst(new SortedMap<K, V>().put(key, value)));
    } else {
      long idx = oIdx.getAsLong();
      boolean overwrite = keyEquality().test(key, nth(idx).key());
      value = overwrite ? merge.apply(nth(idx).value(), value) : value;

      IList<ISortedMap<K, V>> segments = (overwrite ? belowExclusive(idx) : belowInclusive(idx)).segments().values();
      if (segments.size() > 0 && segments.last() instanceof SortedMap) {
        ISortedMap<K, V> s = segments.last().put(key, value);
        segments = segments.removeLast().addLast(s);
      } else {
        segments = segments.addLast(new SortedMap<K, V>(comparator()).put(key, value));
      }
      segments = segments.concat(aboveExclusive(idx).segments().values());

      result = from(underlying, segments);
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
    OptionalLong oIdx = inclusiveFloorIndex(key);
    if (oIdx.isPresent() && keyEquality().test(key, nth(oIdx.getAsLong()).key())) {
      ConcatSortedMap<K, V> result = from(
          underlying,
          belowExclusive(oIdx.getAsLong()),
          aboveExclusive(oIdx.getAsLong())
      );
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
    OptionalLong oMinIdx = segments.inclusiveFloorIndex(min);
    OptionalLong oMaxIdx = segments.inclusiveFloorIndex(max);

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

    return from(underlying, acc);
  }

  @Override
  public ConcatSortedMap<K, V> forked() {
    return isLinear ? new ConcatSortedMap<>(underlying, segments, segmentOffsets, false) : this;
  }

  @Override
  public ConcatSortedMap<K, V> linear() {
    return isLinear ? this : new ConcatSortedMap<>(underlying, segments, segmentOffsets, true);
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
    return idx == 0 ? from(underlying, List.empty()) : slice(first().key(), nth(idx - 1).key());
  }

  private ConcatSortedMap<K, V> aboveExclusive(long idx) {
    return idx == size() - 1 ? from(underlying, List.empty()) : slice(nth(idx + 1).key(), last().key());
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
