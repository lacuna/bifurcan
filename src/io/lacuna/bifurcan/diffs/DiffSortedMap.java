package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

import java.util.Arrays;
import java.util.OptionalLong;
import java.util.function.BinaryOperator;

public class DiffSortedMap<K, V> implements IDiffSortedMap<K, V> {

  private final ISortedMap<K, ISortedMap<K, V>> segments;
  private final long[] offsets;

  public DiffSortedMap(ISortedMap<K, V> m) {
    this.segments = new SortedMap<K, ISortedMap<K, V>>().put(m.first().key(), m);
    this.offsets = new long[]{0};
  }

  private OptionalLong floorIndex(long index) {
    int idx = Arrays.binarySearch(offsets, index);
    return idx == -1 ? OptionalLong.empty() : OptionalLong.of(-idx + 1);
  }

  @Override
  public ISortedMap<K, ISortedMap<K, V>> segments() {
    return null;
  }

  @Override
  public ISortedSet<Long> segmentOffsets() {
    return Sets.from(Lists.from(offsets.length, i -> offsets[(int) i]), Long::compare, this::floorIndex);
  }

  @Override
  public DiffSortedMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    return null;
  }

  @Override
  public DiffSortedMap<K, V> remove(K key) {
    return null;
  }

  @Override
  public DiffSortedMap<K, V> forked() {
    return null;
  }

  @Override
  public DiffSortedMap<K, V> linear() {
    return null;
  }

  @Override
  public boolean isLinear() {
    return segments.isLinear();
  }

  @Override
  public DiffSortedMap<K, V> clone() {
      return this;
  }
}
