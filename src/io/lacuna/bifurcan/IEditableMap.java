package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IEditableMap<K, V> extends
        IMap<K, V>,
        ILinearizable<IEditableMap<K, V>>,
        IForkable<IEditableMap<K, V>> {

  /**
   * @param key   the key
   * @param value the new value under the key
   * @param mergeFn a function which will be invoked if there is a pre-existing value under {@code key}, with both the
   *                old and new value, to determine what should be stored in the map
   * @return an updated map
   */
  IEditableMap<K, V> put(K key, V value, ValueMerger<K, V> mergeFn);

  /**
   * @return an updated map with {@code value} stored under {@code key}
   */
  default IEditableMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  /**
   * @return the map, without anything stored under {@code key}
   */
  IEditableMap<K, V> remove(K key);
}
