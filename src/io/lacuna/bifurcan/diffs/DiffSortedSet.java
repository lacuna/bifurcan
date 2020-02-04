package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

public class DiffSortedSet<V> implements IDiffSortedSet<V> {

  private final ConcatSortedMap<V, Void> diffMap;

  private DiffSortedSet(ConcatSortedMap<V, Void> diffMap) {
    this.diffMap = diffMap;
  }

  public DiffSortedSet(ISortedMap<V, Void> diffMap) {
    this(ConcatSortedMap.from(diffMap.comparator(), LinearList.of(diffMap)));
  }

  public DiffSortedSet(ISortedSet<V> underlying) {
    this(Maps.from(underlying, x -> null));
  }

  @Override
  public IDiffSortedMap<V, Void> diffMap() {
    return diffMap;
  }

  @Override
  public DiffSortedSet<V> add(V value) {
    ConcatSortedMap<V, Void> diffPrime = diffMap.put(value, null, Maps.MERGE_LAST_WRITE_WINS);
    return isLinear() ? this : new DiffSortedSet<>(diffPrime);
  }

  @Override
  public DiffSortedSet<V> remove(V value) {
    ConcatSortedMap<V, Void> diffPrime = diffMap.remove(value);
    return isLinear() ? this : new DiffSortedSet<>(diffPrime);
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

  @Override
  public int hashCode() {
    return (int) Sets.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ISet) {
      return Sets.equals(this, (ISet) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Sets.toString(this);
  }
}
