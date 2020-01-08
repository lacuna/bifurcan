package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

import java.util.OptionalLong;
import java.util.function.BinaryOperator;

/**
 *
 * @author ztellman
 */
public class DiffMap<K, V> implements IDiffMap<K, V> {

  private final IMap<K, V> underlying, added;
  private final ISortedSet<Long> removedIndices;

  public DiffMap(IMap<K, V> underlying) {
    this(underlying, new Map<>(underlying.keyHash(), underlying.keyEquality()), new IntSet());
  }

  private DiffMap(IMap<K, V> underlying, IMap<K, V> added, ISortedSet<Long> removedIndices) {
    this.underlying = underlying;
    this.added = added;
    this.removedIndices = removedIndices;
  }

  @Override
  public DiffMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    long addedSize = added.size();
    OptionalLong idx = underlying.indexOf(key);
    IMap<K, V> addedPrime = idx.isPresent() && !added.contains(key)
        ? added.put(key, merge.apply(underlying.apply(key), value))
        : added.put(key, value, merge);

    if (addedPrime.size() != addedSize) {
      ISortedSet<Long> removedIndicesPrime = idx.isPresent() ? removedIndices.add(idx.getAsLong()) : removedIndices;
      return isLinear() ? this : new DiffMap<>(underlying, addedPrime, removedIndicesPrime);
    } else {
      return isLinear() ? this : new DiffMap<>(underlying, addedPrime, removedIndices);
    }
  }

  @Override
  public DiffMap<K, V> remove(K key) {
    IMap<K, V> addedPrime = added.remove(key);
    OptionalLong idx = underlying.indexOf(key);
    ISortedSet<Long> removedIndicesPrime = idx.isPresent() ? removedIndices.add(idx.getAsLong()) : removedIndices;
    return isLinear() ? this : new DiffMap<>(underlying, addedPrime, removedIndicesPrime);
  }

  @Override
  public boolean isLinear() {
    return added.isLinear();
  }

  @Override
  public DiffMap<K, V> forked() {
    return isLinear() ? new DiffMap<>(underlying, added.forked(), removedIndices.forked()) : this;
  }

  @Override
  public DiffMap<K, V> linear() {
    return isLinear() ? this : new DiffMap<>(underlying, added.linear(), removedIndices.linear());
  }

  @Override
  public IMap<K, V> underlying() {
    return underlying;
  }

  @Override
  public IMap<K, V> added() {
    return added;
  }

  @Override
  public ISortedSet<Long> removedIndices() {
    return removedIndices;
  }

  @Override
  public DiffMap<K, V> clone() {
    return this;
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return Maps.equals(this, (IMap) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }
}
