package io.lacuna.bifurcan;

import java.util.Comparator;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public class IntSet extends ISet.Mixin<Long> implements ISortedSet<Long> {

  final IntMap<Void> m;
  int hash = -1;

  public IntSet() {
    m = new IntMap<Void>();
  }

  IntSet(IntMap<Void> m) {
    this.m = m;
  }

  @Override
  public OptionalLong floorIndex(Long val) {
    return m.floorIndex(val);
  }

  @Override
  public OptionalLong ceilIndex(Long val) {
    return m.ceilIndex(val);
  }

  @Override
  public IntSet slice(Long min, Long max) {
    return new IntSet(m.slice(min, max));
  }

  @Override
  public IntSet add(Long value) {
    IntMap<Void> mPrime = m.put(value, null);
    if (m == mPrime) {
      hash = -1;
      return this;
    } else {
      return new IntSet(mPrime);
    }
  }

  @Override
  public IntSet remove(Long value) {
    IntMap<Void> mPrime = m.remove(value);
    if (m == mPrime) {
      hash = -1;
      return this;
    } else {
      return new IntSet(mPrime);
    }
  }

  @Override
  public IntSet union(ISet<Long> s) {
    if (s instanceof IntSet) {
      return new IntSet(m.union(((IntSet) s).m));
    } else {
      return (IntSet) Sets.union(this, s);
    }
  }

  @Override
  public IntSet difference(ISet<Long> s) {
    if (s instanceof IntSet) {
      return new IntSet(m.difference(((IntSet) s).m));
    } else {
      return (IntSet) Sets.difference(this, s);
    }
  }

  @Override
  public IntSet intersection(ISet<Long> s) {
    if (s instanceof IntSet) {
      return new IntSet(m.intersection(((IntSet) s).m));
    } else {
      return (IntSet) Sets.intersection(new IntSet().linear(), this, s);
    }
  }

  @Override
  public Comparator<Long> comparator() {
    return Comparator.naturalOrder();
  }

  @Override
  public ToLongFunction<Long> valueHash() {
    return m.keyHash();
  }

  @Override
  public BiPredicate<Long, Long> valueEquality() {
    return m.keyEquality();
  }

  @Override
  public boolean contains(Long value) {
    return m.contains(value);
  }

  @Override
  public OptionalLong indexOf(Long element) {
    return m.indexOf(element);
  }

  @Override
  public long size() {
    return m.size();
  }

  @Override
  public Long nth(long idx) {
    return m.nth(idx).key();
  }

  @Override
  public IList<Long> elements() {
    return Lists.lazyMap(m.entries(), IEntry::key);
  }

  @Override
  public IntSet forked() {
    return isLinear() ? new IntSet(m.forked()) : this;
  }

  @Override
  public IntSet linear() {
    return isLinear() ? this : new IntSet(m.linear());
  }
}
