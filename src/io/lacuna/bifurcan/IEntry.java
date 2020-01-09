package io.lacuna.bifurcan;

import java.util.function.BiPredicate;

/**
 * @author ztellman
 */
public interface IEntry<K, V> {

  interface WithHash<K, V> extends IEntry<K, V>, Comparable<WithHash<K, V>> {
    long keyHash();

    @Override
    default int compareTo(WithHash<K, V> o) {
      return Long.compare(keyHash(), o.keyHash());
    }
  }

  static <K, V> IEntry<K, V> of(K key, V value) {
    return new Maps.Entry<>(key, value);
  }

  static <K, V> IEntry.WithHash<K, V> of(long keyHash, K key, V value) {
    return new Maps.HashEntry<>(keyHash, key, value);
  }

  K key();

  V value();

  default boolean equals(IEntry<K, V> o, BiPredicate<K, K> keyEquals, BiPredicate<V, V> valEquals) {
    return keyEquals.test(key(), o.key()) && valEquals.test(value(), o.value());
  }
}
