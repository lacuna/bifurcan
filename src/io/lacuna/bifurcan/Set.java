package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class Set<V> implements ISet<V> {

  Map<V, Void> map;

  public Set() {
    this(Objects::hashCode, Objects::equals);
  }

  public Set(ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    map = new Map<V, Void>(hashFn, equalsFn);
  }

  private Set(Map<V, Void> map) {
    this.map = map;
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
    IList<IMap.IEntry> entries = ((IMap) map).entries();
    return Lists.from(size(), i -> (V) entries.nth(i).key(), () -> iterator());
  }

  @Override
  public Set<V> add(V value) {
    Map<V, Void> mapPrime = map.put(value, null);
    if (map.isLinear()) {
      map = mapPrime;
      return this;
    } else {
      return new Set<V>(mapPrime);
    }
  }

  @Override
  public Set<V> remove(V value) {
    Map<V, Void> mapPrime = map.remove(value);
    if (map.isLinear()) {
      map = mapPrime;
      return this;
    } else {
      return new Set<V>(mapPrime);
    }
  }

  @Override
  public Iterator<V> iterator() {
    return Iterators.map(map.iterator(), e -> e.key());
  }

  @Override
  public Set<V> union(ISet<V> s) {
    if (s instanceof Set) {
      return new Set<V>(map.union(((Set<V>) s).map));
    } else {
      return (Set<V>) Sets.union(this, s);
    }
  }

  @Override
  public Set<V> difference(ISet<V> s) {
    if (s instanceof Set) {
      return new Set<V>(map.difference(((Set<V>) s).map));
    } else {
      return (Set<V>) Sets.difference(this, s);
    }
  }

  @Override
  public Set<V> intersection(ISet<V> s) {
    if (s instanceof Set) {
      return new Set<V>(map.intersection(((Set<V>) s).map));
    } else {
      return (Set<V>) Sets.intersection(new Set<V>().linear(), this, s).forked();
    }
  }

  @Override
  public Set<V> forked() {
    return map.isLinear() ? new Set<V>(map.forked()) : this;
  }

  @Override
  public Set<V> linear() {
    return map.isLinear() ? this : new Set<V>(map.linear());
  }

  @Override
  public int hashCode() {
    return (int) Sets.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Set) {
      return map.equals(((Set<V>) obj).map, (a, b) -> true);
    } else if (obj instanceof ISet) {
      return Sets.equals(this, (ISet<V>) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Sets.toString(this);
  }
}
