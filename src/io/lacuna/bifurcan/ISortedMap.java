package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IMap;

/**
 * @author ztellman
 */
public interface ISortedMap<K, V> extends IMap<K, V> {

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  IEntry<K, V> floor(K key);

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  IEntry<K, V> ceil(K key);

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within [{@code} min, {@code} max]
   */
  ISortedMap<K, V> slice(K min, K max);

  default IEntry<K, V> first() {
    return nth(0);
  }

  default IEntry<K, V> last() {
    return nth(size() - 1);
  }
}
