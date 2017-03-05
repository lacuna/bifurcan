package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class Set<V> implements ISet<V> {

  IMap<V, ?> map;

  public Set() {
    this(Objects::hashCode, Objects::equals);
  }

  public Set(ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    map = new Map<V, Void>(hashFn, equalsFn);
  }

  private Set(IMap<V, ?> map) {
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
    return Lists.from(size(), i -> (V) entries.nth(i).key(), l -> iterator());
  }

  @Override
  public ISet<V> add(V value) {
    IMap<V, ?> mapPrime = map.put(value, null);
    if (map.isLinear()) {
      map = mapPrime;
      return this;
    } else {
      return new Set<V>(mapPrime);
    }
  }

  @Override
  public ISet<V> remove(V value) {
    IMap<V, ?> mapPrime = map.remove(value);
    if (map.isLinear()) {
      map = mapPrime;
      return this;
    } else {
      return new Set<V>(mapPrime);
    }
  }

  @Override
  public Iterator<V> iterator() {
    Iterator<IMap.IEntry> entries = ((IMap) map).iterator();
    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return entries.hasNext();
      }

      @Override
      public V next() {
        return (V) entries.next().key();
      }
    };
  }

  @Override
  public ISet<V> union(ISet<V> s) {
    return null;
  }

  @Override
  public ISet<V> difference(ISet<V> s) {
    return null;
  }

  @Override
  public ISet<V> intersection(ISet<V> s) {
    return null;
  }
}
