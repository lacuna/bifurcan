package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface IMap<K,V> {

  interface Entry<K,V> {
    K key();
    V value();
  }

  IMap<K,V> put(K key, V value);

  IMap<K,V> remove(K key);

  V get(K key);

  boolean contains(K key);

  IList<Entry<K,V>> entries();

  long size();
}
