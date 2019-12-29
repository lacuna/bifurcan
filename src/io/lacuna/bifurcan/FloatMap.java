package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Encodings;

import java.util.*;
import java.util.function.*;

import static io.lacuna.bifurcan.utils.Encodings.doubleToLong;
import static io.lacuna.bifurcan.utils.Encodings.longToDouble;

/**
 * A map which has floating-point keys, built atop {@code IntMap}, with which it shares performance characteristics.
 *
 * Since this is intended foremost as a sorted data structure, it does not allow {@code NaN} and treats {@code -0.0} as
 * equivalent to {@code 0.0}.  Anyone looking for identity-based semantics should use a normal {@code Map} instead.
 *
 * @author ztellman
 */
public class FloatMap<V> implements ISortedMap<Double, V>, Cloneable {

  private static final ToIntFunction<Double> HASH = n -> IntMap.HASH.applyAsInt(Encodings.doubleToLong(n));

  public IntMap<V> map;

  public static <V> FloatMap<V> from(IMap<Number, V> m) {
    if (m instanceof FloatMap) {
      return (FloatMap) m.forked();
    } else {
      return from(m.entries());
    }
  }

  /**
   * @param m a {@code java.util.Map}
   * @return a forked copy of the map
   */
  public static <V> FloatMap<V> from(java.util.Map<Number, V> m) {
    return from(m.entrySet());
  }

  /**
   * @param collection a collection of {@code java.util.map.Entry} objects
   * @return an {@code IntMap} representing the entries in the collection
   */
  public static <V> FloatMap<V> from(Collection<java.util.Map.Entry<Number, V>> collection) {
    FloatMap<V> map = new FloatMap<V>().linear();
    for (java.util.Map.Entry<Number, V> entry : collection) {
      map = map.put(entry.getKey().doubleValue(), entry.getValue());
    }
    return map.forked();
  }

  /**
   * @param list a list of {@code IEntry} objects
   * @return an {@code IntMap} representing the entries in the list
   */
  public static <V> FloatMap<V> from(IList<IEntry<Number, V>> list) {
    FloatMap<V> map = new FloatMap<V>().linear();
    for (IEntry<Number, V> entry : list) {
      map = map.put(entry.key().doubleValue(), entry.value());
    }
    return map.forked();
  }


  public FloatMap() {
    this.map = new IntMap<V>();
  }

  private FloatMap(IntMap<V> map) {
    this.map = map;
  }

  ///


  @Override
  public Comparator<Double> comparator() {
    return Comparator.naturalOrder();
  }

  @Override
  public ToIntFunction<Double> keyHash() {
    return HASH;
  }

  @Override
  public BiPredicate<Double, Double> keyEquality() {
    return Double::equals;
  }

  public boolean contains(double key) {
    return map.contains(doubleToLong(key));
  }

  @Override
  public IList<IEntry<Double, V>> entries() {
    return Lists.lazyMap(map.entries(), FloatMap::convertEntry);
  }

  public OptionalLong indexOf(double key) {
    return map.indexOf(doubleToLong(key));
  }

  @Override
  public OptionalLong indexOf(Double key) {
    return indexOf((double) key);
  }

  @Override
  public IEntry<Double, V> nth(long index) {
    return convertEntry(map.nth(index));
  }

  @Override
  public long size() {
    return map.size();
  }

  public IEntry<Double, V> floor(double key) {
    return convertEntry(map.floor(doubleToLong(key)));
  }

  @Override
  public IEntry<Double, V> floor(Double key) {
    return floor((double) key);
  }

  public IEntry<Double, V> ceil(double key) {
    return convertEntry(map.ceil(doubleToLong(key)));
  }

  @Override
  public IEntry<Double, V> ceil(Double key) {
    return ceil((double) key);
  }

  public FloatMap<V> slice(double min, double max) {
    return new FloatMap<>(map.slice(doubleToLong(min), doubleToLong(max)));
  }

  @Override
  public FloatMap<V> slice(Double min, Double max) {
    return slice((double) min, (double) max);
  }

  @Override
  public FloatMap<V> merge(IMap<Double, V> b, BinaryOperator<V> mergeFn) {
    if (b instanceof FloatMap) {
      FloatMap<V> m = (FloatMap<V>) b;
      return new FloatMap<>(map.merge(m.map, mergeFn));
    } else {
      return (FloatMap<V>) Maps.merge(this.clone(), b, mergeFn);
    }
  }

  @Override
  public FloatMap<V> difference(ISet<Double> keys) {
    return (FloatMap<V>) Maps.difference(this, keys);
  }

  @Override
  public FloatMap<V> intersection(ISet<Double> keys) {
    return this.intersection(Maps.from(keys, n -> null));
  }

  @Override
  public FloatMap<V> union(IMap<Double, V> m) {
    return this.merge(m, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public FloatMap<V> difference(IMap<Double, ?> b) {
    if (b instanceof FloatMap) {
      FloatMap<V> m = (FloatMap<V>) b;
      return new FloatMap<>(map.difference(m.map));
    } else {
      return (FloatMap<V>) Maps.difference(this.clone(), b.keys());
    }
  }

  @Override
  public FloatMap<V> intersection(IMap<Double, ?> b) {
    if (b instanceof FloatMap) {
      FloatMap<V> m = (FloatMap<V>) b;
      return new FloatMap<>(map.intersection(m.map));
    } else {
      FloatMap<V> result = (FloatMap<V>) Maps.intersection(new FloatMap<V>().linear(), this, b.keys());
      return isLinear() ? result : result.forked();
    }
  }

  /**
   * @param key   a primitive {@code long} key
   * @param value a value
   * @return an updated {@code FloatMap} with {@code value} under {@code key}
   */
  public FloatMap<V> put(double key, V value) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  public FloatMap<V> put(double key, V value, Object editor) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS, editor);
  }

  /**
   * @param key   a primitive {@code long} key
   * @param value a value
   * @param merge a function which will be invoked if there is a pre-existing value under {@code key}, with the current
   *              value as the first argument and new value as the second, to determine the combined result
   * @return an updated map
   */
  public FloatMap<V> put(double key, V value, BinaryOperator<V> merge) {
    return put(key, value, merge, isLinear() ? map.editor : new Object());
  }

  public FloatMap<V> put(double key, V value, BinaryOperator<V> merge, Object editor) {

    if (Double.isNaN(key)) {
      throw new IllegalArgumentException("FloatMap does not support NaN");
    }

    IntMap<V> mapPrime = map.put(doubleToLong(key), value, merge, editor);
    if (isLinear()) {
      map = mapPrime;
      return this;
    } else {
      return new FloatMap<>(mapPrime);
    }
  }

  @Override
  public FloatMap<V> put(Double key, V value) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public FloatMap<V> put(Double key, V value, BinaryOperator<V> merge) {
    return put((double) key, value, merge);
  }

  /**
   * @return an updated map that does not contain {@code key}
   */
  public FloatMap<V> remove(double key) {
    return remove(key, isLinear() ? map.editor : new Object());
  }

  public FloatMap<V> remove(double key, Object editor) {
    IntMap<V> mapPrime = map.remove(doubleToLong(key), editor);

    if (isLinear()) {
      map = mapPrime;
      return this;
    } else {
      return new FloatMap<>(mapPrime);
    }
  }

  @Override
  public FloatMap<V> remove(Double key) {
    return remove((double) key);
  }

  @Override
  public <U> FloatMap<U> mapValues(BiFunction<Double, V, U> f) {
    return new FloatMap<>(map.mapValues((k, v) -> f.apply(longToDouble(k), v)));
  }

  public Optional<V> get(double key) {
    return map.get(doubleToLong(key));
  }

  public V get(double key, V defaultValue) {
    return map.get(doubleToLong(key), defaultValue);
  }

  @Override
  public V get(Double key, V defaultValue) {
    return get((double) key, defaultValue);
  }

  @Override
  public FloatMap<V> update(Double key, UnaryOperator<V> update) {
    return update((double) key, update);
  }

  public FloatMap<V> update(double key, UnaryOperator<V> update) {
    return put(key, update.apply(get(key, null)), isLinear() ? isLinear() : new Object());
  }

  public FloatMap<V> update(double key, UnaryOperator<V> update, Object editor) {
    return put(key, update.apply(get(key, null)), editor);
  }

  @Override
  public List<FloatMap<V>> split(int parts) {
    return map.split(parts).stream().map(m -> new FloatMap<>(m)).collect(Lists.collector());
  }

  @Override
  public boolean isLinear() {
    return map.isLinear();
  }

  @Override
  public FloatMap<V> forked() {
    return isLinear() ? new FloatMap<>(map.forked()) : this;
  }

  @Override
  public FloatMap<V> linear() {
    return isLinear() ? this : new FloatMap<>(map.linear());
  }

  @Override
  public FloatMap<V> clone() {
    return new FloatMap<>(map.clone());
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
  }

  @Override
  public boolean equals(IMap<Double, V> o, BiPredicate<V, V> valEquals) {
    if (o instanceof FloatMap) {
      FloatMap<V> m = (FloatMap<V>) o;
      return map.equals(m.map, valEquals);
    } else {
      return Maps.equals(this, o, valEquals);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return equals((IMap<Double, V>) obj, Objects::equals);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }

  ///

  private static <V> IEntry<Double, V> convertEntry(IEntry<Long, V> e) {
    return e != null ? IEntry.of(longToDouble(e.key()), e.value()) : null;
  }

}
