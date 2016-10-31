package io.lacuna.bifurcan;

import java.util.function.BiFunction;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IMap<K, V> extends
        IReadMap<K, V>,
        ILinearizable<IMap<K, V>>,
        IForkable<IMap<K, V>> {

  /**
   * @param key   the key
   * @param value the new value under the key
   * @param mergeFn a function which will be invoked if there is a pre-existing value under {@code key}, with both the
   *                old and new value, to determine what should be stored in the map
   * @return an updated map
   */
  IMap<K, V> put(K key, V value, EntryMerger<K, V> mergeFn);

  /**
   * @return an updated map with {@code value} stored under {@code key}
   */
  default IMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  /**
   * @return the map, without anything stored under {@code key}
   */
  IMap<K, V> remove(K key);
}
