package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

public class DiffSet<V> implements IDiffSet<V> {

  private final ISet<V> underlying;
  private final ISet<V> added;
  private final ISortedSet<Long> removedIndices;

  public DiffSet(ISet<V> underlying) {
    this(underlying, new Set<>(underlying.valueHash(), underlying.valueEquality()), new IntSet());
  }

  public DiffSet(ISet<V> underlying, ISet<V> added, ISortedSet<Long> removedIndices) {
    this.underlying = underlying;
    this.added = added;
    this.removedIndices = removedIndices;
  }

  @Override
  public ISet<V> underlying() {
    return underlying;
  }

  @Override
  public ISet<V> added() {
    return added;
  }

  @Override
  public ISortedSet<Long> removedIndices() {
    return removedIndices;
  }

  @Override
  public ISet<V> add(V value) {
    if (!underlying.contains(value)) {
      ISet<V> addedPrime = added.add(value);
      return isLinear() ? this : new DiffSet<>(underlying, addedPrime, removedIndices);
    } else {
      return this;
    }
  }

  @Override
  public ISet<V> remove(V value) {
    if (added.contains(value)) {
      ISet<V> addedPrime = added.remove(value);
      return isLinear() ? this : new DiffSet<>(underlying, addedPrime, removedIndices);
    } else {
      long idx = underlying.indexOf(value);
      ISortedSet<Long> removedIndicesPrime = idx >= 0 ? removedIndices.add(idx) : removedIndices;
      return isLinear() ? this : new DiffSet<>(underlying, added, removedIndicesPrime);
    }
  }

  @Override
  public boolean isLinear() {
    return added.isLinear();
  }

  @Override
  public ISet<V> forked() {
    return isLinear() ? new DiffSet<>(underlying, added.forked(), removedIndices.forked()) : this;
  }

  @Override
  public ISet<V> linear() {
    return isLinear() ? this : new DiffSet<>(underlying, added.linear(), removedIndices.linear());
  }

  @Override
  public int hashCode() {
    return (int) Sets.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ISet) {
      return Sets.equals(this, (ISet<V>) obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return Sets.toString(this);
  }

  @Override
  public DiffSet<V> clone() {
    return this;
  }
}
