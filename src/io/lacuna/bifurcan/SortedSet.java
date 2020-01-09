package io.lacuna.bifurcan;

import java.util.Comparator;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public class SortedSet<V> implements ISortedSet<V> {

  final SortedMap<V, Void> m;
  int hash = -1;

  public SortedSet() {
    this.m = new SortedMap<>();
  }

  private SortedSet(SortedMap<V, Void> m) {
    this.m = m;
  }

  @Override
  public Comparator<V> comparator() {
    return m.comparator();
  }

  @Override
  public V floor(V val) {
    return Optional.of(m.floor(val)).map(IEntry::key).orElse(null);
  }

  @Override
  public V ceil(V val) {
    return Optional.of(m.ceil(val)).map(IEntry::key).orElse(null);
  }

  @Override
  public SortedSet<V> slice(V min, V max) {
    return new SortedSet<>(m.slice(min, max));
  }

  @Override
  public SortedSet<V> add(V value) {
    SortedMap<V, Void> mPrime = m.put(value, null);
    if (m == mPrime) {
      hash = -1;
      return this;
    } else {
      return new SortedSet<>(mPrime);
    }
  }

  @Override
  public SortedSet<V> remove(V value) {
    SortedMap<V, Void> mPrime = m.remove(value);
    if (m == mPrime) {
      hash = -1;
      return this;
    } else {
      return new SortedSet<>(mPrime);
    }
  }

  @Override
  public SortedSet<V> union(ISet<V> s) {
    if (s instanceof SortedSet) {
      return new SortedSet<>(m.union(((SortedSet<V>) s).m));
    } else {
      return new SortedSet<>(m.union(Maps.from(s, k -> null)));
    }
  }

  @Override
  public <U> SortedMap<V, U> zip(Function<V, U> f) {
    return m.mapValues((k, v) -> f.apply(k));
  }

  @Override
  public SortedSet<V> difference(ISet<V> s) {
    return new SortedSet<>(m.difference(s));
  }

  @Override
  public SortedSet<V> intersection(ISet<V> s) {
    return new SortedSet<>(m.intersection(s));
  }

  @Override
  public ToLongFunction<V> valueHash() {
    return m.keyHash();
  }

  @Override
  public BiPredicate<V, V> valueEquality() {
    return m.keyEquality();
  }

  @Override
  public boolean contains(V value) {
    return m.contains(value);
  }

  @Override
  public OptionalLong indexOf(V element) {
    return m.indexOf(element);
  }

  @Override
  public long size() {
    return m.size();
  }

  @Override
  public V nth(long index) {
    return m.nth(index).key();
  }

  @Override
  public IList<V> elements() {
    return Lists.lazyMap(m.entries(), IEntry::key);
  }

  @Override
  public SortedSet<V> forked() {
    return isLinear() ? new SortedSet<>(m.forked()) : this;
  }

  @Override
  public SortedSet<V> linear() {
    return isLinear() ? this : new SortedSet<>(m.linear());
  }

  @Override
  public int hashCode() {
    if (hash == -1) {
      hash = (int) Sets.hash(this);
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ISortedSet) {
      return Sets.equals(this, (ISortedSet<V>) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Sets.toString(this);
  }

  @Override
  public ISet<V> clone() {
    return this;
  }
}
