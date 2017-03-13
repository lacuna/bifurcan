package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class LinearSet<V> implements ISet<V> {

  private LinearMap<V, Void> map;

  public LinearSet() {
    this(8);
  }

  public LinearSet(int initialCapacity) {
    this(initialCapacity, Objects::hashCode, Objects::equals);
  }

  public LinearSet(int initialCapacity, ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    map = new LinearMap<>(initialCapacity, hashFn, equalsFn);
  }

  private LinearSet(LinearMap<V, Void> map) {
    this.map = map;
  }

  public static <V> LinearSet<V> from(IList<V> elements) {
    return from(elements.toList());
  }

  public static <V> LinearSet<V> from(java.util.Collection<V> elements) {
    LinearSet<V> set = new LinearSet<>((int) elements.size());
    for (V e : elements) {
      set = set.add(e);
    }
    return set;
  }

  public static <V> LinearSet<V> from(ISet<V> set) {
    if (set instanceof LinearSet) {
      return ((LinearSet<V>) set).clone();
    } else {
      return from(set.toSet());
    }
  }

  @Override
  public LinearSet<V> add(V value) {
    map.put(value, null);
    return this;
  }

  @Override
  public LinearSet<V> remove(V value) {
    map.remove(value);
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
    return Lists.from(entries.size(), i -> entries.nth(i).key(), () -> iterator());
  }

  @Override
  public Iterator<V> iterator() {
    return Iterators.map(map.iterator(), IMap.IEntry::key);
  }

  @Override
  public LinearSet<V> union(ISet<V> s) {
    if (s instanceof LinearSet) {
      map.union(((LinearSet<V>) s).map);
    } else {
      for (V e : s) {
        map.put(e, null);
      }
    }
    return this;
  }

  @Override
  public LinearSet<V> difference(ISet<V> s) {
    if (s instanceof LinearSet) {
      map.difference(((LinearSet<V>) s).map);
    } else {
      for (V e : s) {
        map.remove(e);
      }
    }
    return this;
  }

  @Override
  public LinearSet<V> intersection(ISet<V> s) {
    if (s instanceof LinearSet) {
      map.intersection(((LinearSet<V>) s).map);
    } else {
      for (V e : s) {
        if (!map.contains(e)) {
          map.remove(e);
        }
      }
    }
    return this;
  }

  @Override
  public ISet<V> forked() {
    throw new UnsupportedOperationException("A LinearSet cannot be efficiently transformed into a forked representation");
  }

  @Override
  public LinearSet<V> linear() {
    return this;
  }

  @Override
  public IList<ISet<V>> split(int parts) {
    return map.split(parts).stream().map(m -> new LinearSet<>((LinearMap<V, Void>) m)).collect(Lists.collector());
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (long row : map.table) {
      if (LinearMap.Row.populated(row)) {
        hash += LinearMap.Row.hash(row);
      }
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ISet) {
      return Sets.equals(this, (ISet<V>) obj);
    }
    return false;
  }

  @Override
  protected LinearSet<V> clone() {
    return new LinearSet<>(map.clone());
  }

  @Override
  public String toString() {
    return Sets.toString(this);
  }


}
