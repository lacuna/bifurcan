package io.lacuna.bifurcan;

import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

import static io.lacuna.bifurcan.utils.Encodings.doubleToLong;
import static io.lacuna.bifurcan.utils.Encodings.longToDouble;

/**
 * @author ztellman
 */
public class FloatMap<V> implements ISortedMap<Double, V>, Cloneable {

  private static final ToIntFunction<Double> HASH = n -> IntMap.HASH.applyAsInt(Double.doubleToLongBits(n));

  private final IntMap<V> map;

  private FloatMap(IntMap<V> map) {
    this.map = map;
  }

  @Override
  public ToIntFunction<Double> keyHash() {
    return HASH;
  }

  @Override
  public BiPredicate<Double, Double> keyEquality() {
    return Double::equals;
  }

  public V get(double key, V defaultValue) {
    return map.get(doubleToLong(key), defaultValue);
  }

  @Override
  public V get(Double key, V defaultValue) {
    return get((double) key, defaultValue);
  }

  public boolean contains(double key) {
    return map.contains(doubleToLong(key));
  }

  @Override
  public boolean contains(Double key) {
    return contains((double) key);
  }

  @Override
  public IList<IEntry<Double, V>> entries() {
    return Lists.lazyMap(map.entries(), FloatMap::convertEntry);
  }

  public long indexOf(double key) {
    return map.indexOf(doubleToLong(key));
  }

  @Override
  public long indexOf(Double key) {
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
  public ISortedMap<Double, V> slice(Double min, Double max) {
    return slice((double) min, (double) max);
  }

  @Override
  public boolean isLinear() {
    return map.isLinear();
  }

  @Override
  public IMap<Double, V> forked() {
    return isLinear() ? new FloatMap<>(map.forked()) : this;
  }

  @Override
  public IMap<Double, V> linear() {
    return isLinear() ? this : new FloatMap<>(map.linear());
  }

  ///

  private static <V> IEntry<Double, V> convertEntry(IEntry<Long, V> e) {
    return new Maps.Entry<>(longToDouble(e.key()), e.value());
  }

}
