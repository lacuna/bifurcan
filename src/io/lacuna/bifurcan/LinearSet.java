package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class LinearSet<V> implements ISet<V>, Cloneable {

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
    final Object[] entries = map.entries;
    return Iterators.range(size(), i -> (V) entries[(int) i << 1]);
  }

  @Override
  public LinearSet<V> union(ISet<V> s) {
    if (s instanceof LinearSet) {
      return new LinearSet<V>(map.union(((LinearSet<V>) s).map));
    } else {
      LinearMap<V, Void> m = map.clone();
      s.forEach(e -> map.put(e, null));
      return new LinearSet<V>(m);
    }
  }

  @Override
  public LinearSet<V> difference(ISet<V> s) {
    if (s instanceof LinearSet) {
      return new LinearSet<V>(map.difference(((LinearSet<V>) s).map));
    } else {
      LinearMap<V, Void> m = map.clone();
      s.forEach(m::remove);
      return new LinearSet<V>(m);
    }
  }

  @Override
  public LinearSet<V> intersection(ISet<V> s) {
    if (s instanceof LinearSet) {
      return new LinearSet<V>(map.intersection(((LinearSet<V>) s).map));
    } else {
      LinearMap<V, Void> m = map.clone();
      for (V e : this) {
        if (s.contains(e)) {
          m.remove(e);
        }
      }
      return new LinearSet<V>(m);
    }
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
  public List<LinearSet<V>> split(int parts) {
    return map.split(parts).stream().map(m -> new LinearSet<>(m)).collect(Lists.collector());
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
  public LinearSet<V> clone() {
    return new LinearSet<>(map.clone());
  }

  @Override
  public String toString() {
    return Sets.toString(this);
  }


}
