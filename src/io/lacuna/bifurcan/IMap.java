package io.lacuna.bifurcan;

import io.lacuna.bifurcan.Maps.VirtualMap;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IMap<K, V> extends
    Iterable<IMap.IEntry<K, V>>,
    ISplittable<IMap<K, V>>,
    ILinearizable<IMap<K, V>>,
    IForkable<IMap<K, V>> {

  interface IEntry<K, V> {
    K key();

    V value();

    default boolean equals(IEntry<K, V> o, BiPredicate<K, K> keyEquals, BiPredicate<V, V> valEquals) {
      return keyEquals.test(key(), o.key()) && valEquals.test(value(), o.value());
    }
  }

  /**
   * @return the hash function used by the map
   */
  default ToIntFunction<K> keyHash() {
    return Objects::hashCode;
  }

  /**
   * @return the key equality semantics used by the map
   */
  default BiPredicate<K, K> keyEquality() {
    return Objects::equals;
  }

  /**
   * @return the value under {@code key}, or {@code defaultValue} if there is no such key
   */
  V get(K key, V defaultValue);

  /**
   * @return an {@code Optional} containing the value under {@code key}, or nothing if the value is {@code null} or
   * is not contained within the map.
   */
  default Optional<V> get(K key) {
    return Optional.ofNullable(get(key, null));
  }

  /**
   * @return true if {@code key} is in the map, false otherwise
   */
  boolean contains(K key);

  /**
   * @return a list containing all the entries within the map
   */
  IList<IEntry<K, V>> entries();

  /**
   * @return a set representing all keys in the map
   */
  default ISet<K> keys() {
    return Sets.from(Lists.lazyMap(entries(), IEntry::key), this::contains);
  }

  /**
   * @return a list representing all values in the map
   */
  default IList<V> values() {
    return Lists.lazyMap(entries(), IEntry::value);
  }

  /**
   * @return the number of entries in the map
   */
  long size();

  /**
   * @return true, if the collection is linear
   */
  default boolean isLinear() {
    return false;
  }

  /**
   * @return the collection, represented as a normal Java {@code io.lacuna.bifurcan.MapNodes}, which will throw
   * {@code UnsupportedOperationException} on writes
   */
  default java.util.Map<K, V> toMap() {
    return Maps.toMap(this);
  }

  /**
   * @return an iterator over all entries in the map
   */
  default Iterator<IEntry<K, V>> iterator() {
    return entries().iterator();
  }

  /**
   * @return a {@code java.util.stream.Stream}, representing the entries in the map
   */
  default Stream<IEntry<K, V>> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  default Spliterator<IEntry<K, V>> spliterator() {
    return Spliterators.spliterator(iterator(), size(), Spliterator.DISTINCT);
  }

  /**
   * @param b       another map
   * @param mergeFn a function which, in the case of key collisions, takes two values and returns the merged result
   * @return a new map representing the merger of the two maps
   */
  default IMap<K, V> merge(IMap<K, V> b, BinaryOperator<V> mergeFn) {
    return Maps.merge(this, b, mergeFn);
  }

  /**
   * @param keys a set of keys
   * @return a new map representing the current map, less the keys in {@code keys}
   */
  default IMap<K, V> difference(ISet<K> keys) {
    return Maps.difference(this, keys);
  }

  /**
   * @param keys a set of keys
   * @return a new map representing the current map, but only with the keys in {@code keys}
   */
  default IMap<K, V> intersection(ISet<K> keys) {
    IMap<K, V> result = Maps.intersection(new Map<K, V>(keyHash(), keyEquality()).linear(), this, keys);
    return isLinear() ? result : result.forked();
  }

  /**
   * @param m another map
   * @return a new map combining the entries of both, with the values from the second map shadowing those of the first
   */
  default IMap<K, V> union(IMap<K, V> m) {
    return merge(m, Maps.MERGE_LAST_WRITE_WINS);
  }

  /**
   * @param m another map
   * @return a new map representing the current map, less the keys in {@code m}
   */
  default IMap<K, V> difference(IMap<K, ?> m) {
    return difference(m.keys());
  }

  /**
   * @param m another map
   * @return a new map representing the current map, but only with the keys in {@code m}
   */
  default IMap<K, V> intersection(IMap<K, ?> m) {
    return intersection(m.keys());
  }

  /**
   * @param merge a function which will be invoked if there is a pre-existing value under {@code key}, with the current
   *              value as the first argument and new value as the second, to determine the combined result
   * @return an updated map with {@code value} under {@code key}
   */
  default IMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    return new VirtualMap<>(this).put(key, value, merge);
  }

  /**
   * @return an updated map with {@code value} stored under {@code key}
   */
  default IMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  /**
   * @return an updated map that does not contain {@code key}
   */
  default IMap<K, V> remove(K key) {
    return new VirtualMap<>(this).remove(key);
  }

  @Override
  default IMap<K, V> forked() {
    return null;
  }

  @Override
  default IMap<K, V> linear() {
    return null;
  }

  @Override
  default IList<? extends IMap<K, V>> split(int parts) {
    return keys()
        .split(parts)
        .stream()
        .map(ks -> Maps.from(ks, k -> get(k, null)))
        .collect(Lists.collector());
  }

  /**
   * @param m      another map
   * @param equals a predicate which checks value equalities
   * @return true, if the maps are equivalent
   */
  default boolean equals(IMap<K, V> m, BiPredicate<V, V> equals) {
    return Maps.equals(this, m, equals);
  }
}
