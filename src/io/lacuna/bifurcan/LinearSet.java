package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.DiffSet;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * A set which builds atop {@code LinearMap}, and shares the same performance characteristics.
 *
 * @author ztellman
 */
public class LinearSet<V> implements ISet<V>, Cloneable {

  LinearMap<V, Void> map;

  ///

  /**
   * @return a set containing the elements in {@code list}
   */
  public static <V> LinearSet<V> from(IList<V> list) {
    return from(list.toList());
  }

  /**
   * @return a set containing the elements in {@code collection}
   */
  public static <V> LinearSet<V> from(java.util.Collection<V> collection) {
    return collection.stream().collect(Sets.linearCollector(collection.size()));
  }

  /**
   * @return a set containing the remaining elements in {@code iterator}
   */
  public static <V> LinearSet<V> from(Iterator<V> iterator) {
    LinearSet<V> set = new LinearSet<V>();
    iterator.forEachRemaining(set::add);
    return set;
  }

  /**
   * @return a set containing the elements in {@code iterable}
   */
  public static <V> LinearSet<V> from(Iterable<V> iterable) {
    return from(iterable.iterator());
  }

  /**
   * @return a set containing the same elements as {@code set}, with the same equality semantics
   */
  public static <V> LinearSet<V> from(ISet<V> set) {
    if (set instanceof LinearSet) {
      return ((LinearSet<V>) set).clone();
    } else {
      LinearSet<V> result = new LinearSet<V>((int) set.size(), set.valueHash(), set.valueEquality());
      set.forEach(result::add);
      return result;
    }
  }

  /**
   * @return a set containing {@code elements}
   */
  public static <V> LinearSet<V> of(V... elements) {
    LinearSet<V> set = new LinearSet<>(elements.length);
    for (V e : elements) {
      set.add(e);
    }
    return set;
  }

  public LinearSet() {
    this(8);
  }

  /**
   * @param initialCapacity the initial capacity of the set
   */
  public LinearSet(int initialCapacity) {
    this(initialCapacity, Maps.DEFAULT_HASH_CODE, Maps.DEFAULT_EQUALS);
  }

  /**
   * @param initialCapacity the initial capacity of the set
   * @param hashFn          the hash function used by the set
   * @param equalsFn        the equality semantics used by the set
   */
  public LinearSet(int initialCapacity, ToLongFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    map = new LinearMap<>(initialCapacity, hashFn, equalsFn);
  }

  /**
   * @param hashFn   the hash function used by the set
   * @param equalsFn the equality semantics used by the set
   */
  public LinearSet(ToLongFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    map = new LinearMap<>(8, hashFn, equalsFn);
  }

  LinearSet(LinearMap<V, Void> map) {
    this.map = map;
  }

  ///

  @Override
  public boolean isLinear() {
    return true;
  }

  @Override
  public ToLongFunction<V> valueHash() {
    return map.keyHash();
  }

  @Override
  public BiPredicate<V, V> valueEquality() {
    return map.keyEquality();
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

  public LinearSet<V> clear() {
    map.clear();

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
  public OptionalLong indexOf(V element) {
    return map.indexOf(element);
  }

  @Override
  public V nth(long idx) {
    return map.nth(idx).key();
  }

  @Override
  public Iterator<V> iterator() {
    final Object[] entries = map.entries;
    return Iterators.range(size(), i -> (V) entries[(int) i << 1]);
  }

  @Override
  public <U> LinearMap<V, U> zip(Function<V, U> f) {
    return map.mapValues((k, v) -> f.apply(k));
  }

  @Override
  public LinearSet<V> union(ISet<V> s) {
    if (s instanceof LinearSet) {
      return new LinearSet<V>(map.union(((LinearSet<V>) s).map));
    } else {
      LinearMap<V, Void> m = map.clone();
      s.forEach(e -> m.put(e, null));
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
        if (!s.contains(e)) {
          m.remove(e);
        }
      }
      return new LinearSet<V>(m);
    }
  }

  @Override
  public ISet<V> forked() {
    return new DiffSet<>(this);
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
