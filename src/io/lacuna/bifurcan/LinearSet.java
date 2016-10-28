package io.lacuna.bifurcan;

import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class LinearSet<V> implements ISet<V>, IPartitionable<LinearSet<V>> {

  private LinearMap<V, Void> map;

  public LinearSet() {
    this(8);
  }

  public LinearSet(int initialCapacity) {
    this(initialCapacity, Objects::hashCode, Objects::equals);
  }

  public LinearSet(IList<V> elements) {
    this((int) elements.size());
    for (V e : elements) {
      map = (LinearMap<V, Void>) map.put(e, null);
    }
  }

  public LinearSet(java.util.Collection<V> elements) {
    this(elements.size());
    for (V e : elements) {
      map = (LinearMap<V, Void>) map.put(e, null);
    }
  }

  public LinearSet(int initialCapacity, ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    map = new LinearMap<>(initialCapacity, hashFn, equalsFn);
  }

  private LinearSet(LinearMap<V, Void> map) {
    this.map = map;
  }

  @Override
  public ISet<V> add(V value) {
    map = (LinearMap<V, Void>) map.put(value, null);
    return this;
  }

  @Override
  public ISet<V> remove(V value) {
    map = (LinearMap<V, Void>) map.remove(value);
    return this;
  }

  @Override
  public boolean contains(V value) {
    return map.contains(value);
  }

  @Override
  public long size() {
    return map.size();
  }

  @Override
  public IList<V> elements() {
    IList<IMap.IEntry<V, Void>> entries = map.entries();
    return Lists.from(entries.size(), i -> entries.nth(i).key());
  }

  @Override
  public ISet<V> union(ISet<V> s) {
    if (s instanceof LinearSet) {
      return new LinearSet<V>(map.merge(((LinearSet<V>) s).map));
    } else {
      return Sets.union(this, s);
    }
  }

  @Override
  public ISet<V> difference(ISet<V> s) {
    if (s instanceof LinearSet) {
      return new LinearSet<V>(map.difference(((LinearSet<V>) s).map));
    } else {
      return Sets.difference(this, s);
    }
  }

  @Override
  public ISet<V> intersection(ISet<V> s) {
    if (s instanceof LinearSet) {
      return new LinearSet<V>(map.intersection(((LinearSet<V>) s).map));
    } else {
      return Sets.intersection(this, s);
    }
  }

  @Override
  public ISet<V> forked() {
    throw new UnsupportedOperationException("A LinearSet cannot be efficiently transformed into a forked representation");
  }

  @Override
  public ISet<V> linear() {
    return this;
  }

  @Override
  public IList<LinearSet<V>> partition(int parts) {
    return map.partition(parts).stream().map(m -> new LinearSet<>(m)).collect(Lists.linearCollector());
  }

  @Override
  public LinearSet<V> merge(LinearSet<V> set) {
    return new LinearSet<V>(map.merge(set.map));
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


}
