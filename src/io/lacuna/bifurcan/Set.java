package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IMap.IEntry;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;

/**
 * A set which builds atop {@code Map}, and shares the same performance characteristics.
 *
 * @author ztellman
 */
public class Set<V> implements ISet<V>, Cloneable {

  public Map<V, Void> map;
  private int hash = -1;

  public Set() {
    this(Objects::hashCode, Objects::equals);
  }

  /**
   * @param hashFn the hash function used by the set
   * @param equalsFn the equality semantics used by the set
   */
  public Set(ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    map = new Map<>(hashFn, equalsFn);
  }

  Set(Map<V, Void> map) {
    this.map = map;
  }

  /**
   * @param s a set
   * @return an equivalent set, with the same equality semantics
   */
  public static <V> Set<V> from(ISet<V> s) {
    if (s instanceof Set) {
      return ((Set<V>) s).forked();
    } else {
      Set<V> result = new Set<V>(s.valueHash(), s.valueEquality()).linear();
      s.forEach(result::add);
      return result.forked();
    }
  }

  /**
   * @param iterator an iterator
   * @return a set containing the remaining values in the iterator
   */
  public static <V> Set<V> from(Iterator<V> iterator) {
    Set<V> set = new Set<V>().linear();
    iterator.forEachRemaining(set::add);
    return set.forked();
  }

  /**
   * @param iterable an {@code Iterable} object
   * @return a set containing the values in the iterator
   */
  public static <V> Set<V> from(Iterable<V> iterable) {
    return from(iterable.iterator());
  }

  public static <V> Set<V> of(V... elements) {
    Set<V> set = new Set<V>().linear();
    for (V e : elements) {
      set.add(e);
    }
    return set.forked();
  }

  ///

  @Override
  public boolean isLinear() {
    return map.isLinear();
  }

  @Override
  public ToIntFunction<V> valueHash() {
    return map.keyHash();
  }

  @Override
  public BiPredicate<V, V> valueEquality() {
    return map.keyEquality();
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
  public long indexOf(V element) {
    return map.indexOf(element);
  }

  @Override
  public V nth(long index) {
    return map.nth(index).key();
  }

  @Override
  public Set<V> add(V value) {
    return add(value, map.editor);
  }

  public Set<V> add(V value, Object editor) {
    Map<V, Void> mapPrime = map.put(value, null, (BinaryOperator<Void>) Maps.MERGE_LAST_WRITE_WINS, editor);
    if (isLinear() || editor != map.editor) {
      hash = -1;
    }

    return map == mapPrime ? this : new Set<>(mapPrime);
  }

  @Override
  public Set<V> remove(V value) {
    return remove(value, map.editor);
  }

  public Set<V> remove(V value, Object editor) {
    Map<V, Void> mapPrime = map.remove(value, editor);
    if (isLinear() || editor != map.editor) {
      hash = -1;
    }

    return map == mapPrime ? this : new Set<>(mapPrime);
  }

  @Override
  public List<Set<V>> split(int parts) {
    return map.split(parts).stream().map(m -> new Set<V>(m)).collect(Lists.collector());
  }

  @Override
  public Iterator<V> iterator() {
    return Iterators.map(map.iterator(), IEntry::key);
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
    if (hash == -1) {
      hash = (int) Sets.hash(this);
    }
    return hash;
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

  @Override
  public Set<V> clone() {
    return map.isLinear() ? forked().linear() : this;
  }
}
