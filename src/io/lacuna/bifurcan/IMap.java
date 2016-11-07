package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.Optional;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IMap<K, V> extends Iterable<IMap.IEntry<K, V>>, ISplittable<IMap<K, V>> {

  interface IEntry<K, V> {
    K key();

    V value();
  }

  interface ValueMerger<K, V> {
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
  ISet<K> keys();

  /**
   * @return the number of entries in the map
   */
  long size();

  /**
   * @return the collection, represented as a normal Java {@code io.lacuna.bifurcan.Map}, which will throw {@code UnsupportedOperationException} on writes
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
   * @param b another map
   * @param mergeFn a function which, in the case of key collisions, returns the resulting
   * @return
     */
  default IMap<K, V> merge(IMap<K, V> b, ValueMerger<K, V> mergeFn) {
    return Maps.merge(this, b, mergeFn);
  }

  default IMap<K, V> merge(IMap<K, V> b) {
    return this.merge(b, Maps.MERGE_LAST_WRITE_WINS);
  }
}
