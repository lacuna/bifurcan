package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

public class DiffSortedSet<V> extends ISortedSet.Mixin<V> implements IDiffSortedSet<V> {

  private final ConcatSortedMap<V, Void> diffMap;

  private DiffSortedSet(ConcatSortedMap<V, Void> diffMap) {
    this.diffMap = diffMap;
  }

  public DiffSortedSet(ISortedMap<V, Void> diffMap) {
    this(ConcatSortedMap.from(diffMap));
  }

  public DiffSortedSet(ISortedSet<V> underlying) {
    this(underlying.zip(x -> null));
  }

  @Override
  public ISortedSet<V> underlying() {
    return diffMap.underlying().keys();
  }

  @Override
  public IDiffSortedMap<V, Void> diffMap() {
    return diffMap;
  }

  @Override
  public IDiffSortedSet<V> rebase(ISortedSet<V> newUnderlying) {
    return new DiffSortedSet<>(diffMap.rebase(newUnderlying.zip(x -> null)));
  }

  @Override
  public DiffSortedSet<V> add(V value) {
    ConcatSortedMap<V, Void> diffPrime = diffMap.put(value, null, Maps.MERGE_LAST_WRITE_WINS);
    if (isLinear()) {
      super.hash = -1;
      return this;
    } else {
      return new DiffSortedSet<>(diffPrime);
    }
  }

  @Override
  public DiffSortedSet<V> remove(V value) {
    ConcatSortedMap<V, Void> diffPrime = diffMap.remove(value);
    if (isLinear()) {
      super.hash = -1;
      return this;
    } else {
      return new DiffSortedSet<V>(diffPrime);
    }
  }

  @Override
  public DiffSortedSet<V> forked() {
    return isLinear() ? new DiffSortedSet<>(diffMap.forked()) : this;
  }

  @Override
  public DiffSortedSet<V> linear() {
    return isLinear() ? this : new DiffSortedSet<>(diffMap.linear());
  }

  @Override
  public boolean isLinear() {
    return diffMap.isLinear();
  }

  @Override
  public DiffSortedSet<V> clone() {
    return isLinear() ? forked().linear() : this;
  }
}
