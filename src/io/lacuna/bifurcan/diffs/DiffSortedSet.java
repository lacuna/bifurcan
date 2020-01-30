package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

public class DiffSortedSet<V> implements IDiffSortedSet<V> {

  private final DiffSortedMap<V, Void> diffMap;

  private DiffSortedSet(DiffSortedMap<V, Void> diffMap) {
    this.diffMap = diffMap;
  }

  public DiffSortedSet(ISortedMap<V, Void> diffMap) {
    this(new DiffSortedMap<>(diffMap));
  }

  public DiffSortedSet(ISortedSet<V> underlying) {
    this(Maps.from(underlying, x -> null));
  }

  @Override
  public IDiffSortedMap<V, Void> diffMap() {
    return diffMap;
  }

  @Override
  public ISortedSet<V> add(V value) {
    DiffSortedMap<V, Void> diffPrime = diffMap.put(value, null, Maps.MERGE_LAST_WRITE_WINS);
    return isLinear() ? this : new DiffSortedSet<>(diffPrime);
  }

  @Override
  public ISortedSet<V> remove(V value) {
    DiffSortedMap<V, Void> diffPrime = diffMap.remove(value);
    return isLinear() ? this : new DiffSortedSet<>(diffPrime);
  }

  @Override
  public ISortedSet<V> forked() {
    return isLinear() ? new DiffSortedSet<>(diffMap.forked()) : this;
  }

  @Override
  public ISortedSet<V> linear() {
    return isLinear() ? this : new DiffSortedSet<>(diffMap.linear());
  }

  @Override
  public boolean isLinear() {
    return diffMap.isLinear();
  }

  @Override
  public DiffSortedSet<V> clone() {
    return this;
  }
}
