package io.lacuna.bifurcan;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ztellman
 */
public interface IMap<K, V> extends
        ILinearizable<IMap<K, V>>,
        IForkable<IMap<K, V>> {

  interface Entry<K, V>  {
    K key();

    V value();
  }

  /**
   * @return the map, with {@code value} stored under {@code key}
   */
  IMap<K, V> put(K key, V value);

  /**
   * @return the map, without anything stored under {@code key}
   */
  IMap<K, V> remove(K key);

  /**
   * @return an {@code Optional} containing the value under {@code key}, or nothing if none exists
   */
  Optional<V> get(K key);

  /**
   * @return an {@code IList} containing all the entries within the map
   */
  IList<Entry<K, V>> entries();

  /**
   * @return the number of entries in the map
   */
  long size();

  /**
   * @return the collection, represented as a normal Java {@code Map}, without support for writes
   */
  default java.util.Map<K, V> toMap() {
    final IMap<K, V> map = this;
    return new java.util.Map<K, V>() {

      @Override
      public int size() {
        return (int) map.size();
      }

      @Override
      public boolean isEmpty() {
        return map.size() == 0;
      }

      @Override
      public boolean containsKey(Object key) {
        return map.get((K) key).isPresent();
      }

      @Override
      public boolean containsValue(Object value) {
        return map.entries().stream().anyMatch(e -> Objects.equals(value, e.value()));
      }

      @Override
      public V get(Object key) {
        return map.get((K) key).orElse(null);
      }

      @Override
      public V put(K key, V value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public V remove(Object key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<K> keySet() {
        return map.entries().stream().map(e -> e.key()).collect(Collectors.toSet());
      }

      @Override
      public Collection<V> values() {
        return map.entries().stream().map(e -> e.value()).collect(Collectors.toList());
      }

      @Override
      public Set<Entry<K, V>> entrySet() {
        return map.entries().stream()
                .map(e ->
                        new Map.Entry<K, V>() {
                          @Override
                          public K getKey() {
                            return e.key();
                          }

                          @Override
                          public V getValue() {
                            return e.value();
                          }

                          @Override
                          public V setValue(V value) {
                            throw new UnsupportedOperationException();
                          }
                        }
                ).collect(Collectors.toSet());
      }
    };
  }
}
