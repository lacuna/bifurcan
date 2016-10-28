package io.lacuna.bifurcan;

import java.util.Optional;

/**
 * @author ztellman
 */
public interface IMap<K, V> extends
        ILinearizable<IMap<K, V>>,
        IForkable<IMap<K, V>> {

  interface IEntry<K, V> {
    K key();

    V value();
  }

  /**
   * @return the map, with {@code rowValue} stored under {@code key}
   */
  IMap<K, V> put(K key, V value);

  /**
   * @return the map, without anything stored under {@code key}
   */
  IMap<K, V> remove(K key);

  /**
   * @return an {@code Optional} containing the rowValue under {@code key}, or nothing if none exists
   */
  Optional<V> get(K key);

  boolean contains(K key);

  /**
   * @return an {@code IList} containing all the entries within the map
   */
  IList<IEntry<K, V>> entries();

  //ISet<K> keys();

  /**
   * @return the number of entries in the map
   */
  long size();

  /**
   * @return the collection, represented as a normal Java {@code Map}, which will throw {@code UnsupportedOperationException} on writes
   */
  default java.util.Map<K, V> toMap() {
    return Maps.toMap(this);
  }
}
