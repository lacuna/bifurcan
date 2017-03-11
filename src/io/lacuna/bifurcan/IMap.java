package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.Optional;

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
  }

  interface ValueMerger<V> {
    V merge(V current, V updated);
  }

  V get(K key, V defaultValue);

  /**
   * @return an {@code Optional} containing the value under {@code key}, or nothing if the value is {@code null} or
   * is not contained within the map.  To differentiate between these cases, use {@code contains()}.
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
   * @return the collection, represented as a normal Java {@code io.lacuna.bifurcan.MapNodes}, which will throw {@code UnsupportedOperationException} on writes
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
   * @param b       another map
   * @param mergeFn a function which, in the case of key collisions, returns the resulting
   * @return
   */
  default IMap<K, V> merge(IMap<K, V> b, ValueMerger<V> mergeFn) {
    return Maps.merge(this, b, mergeFn);
  }

  default IMap<K, V> difference(ISet<K> keys) {
    IMap<K, V> m = this.linear();
    for (K key : keys) {
      m = m.remove(key);
    }
    return m.forked();
  }

  default IMap<K, V> intersection(ISet<K> keys) {
    return Maps.intersection(new Map<K, V>().linear(), this, keys).forked();
  }

  default IMap<K, V> union(IMap<K, V> m) {
    return Maps.merge(this, m, Maps.MERGE_LAST_WRITE_WINS);
  }

  default IMap<K, V> difference(IMap<K, ?> m) {
    return difference(m.keys());
  }

  default IMap<K, V> intersection(IMap<K, ?> m) {
    return intersection(m.keys());
  }

  /**
   * @param key   the key
   * @param value the new value under the key
   * @param merge a function which will be invoked if there is a pre-existing value under {@code key}, with both the
   *              old and new value, to determine what should be stored in the map
   * @return an updated map
   */
  default IMap<K, V> put(K key, V value, ValueMerger<V> merge) {
    return null;
  }

  /**
   * @return an updated map with {@code value} stored under {@code key}
   */
  default IMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  /**
   * @return the map, without anything stored under {@code key}
   */
  default IMap<K, V> remove(K key) {
    return null;
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
  default public IList<IMap<K, V>> split(int parts) {
    return keys()
        .split(parts)
        .stream()
        .map(ks -> Maps.from(ks, k -> get(k, null)))
        .collect(Lists.collector());
  }

}
