package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IMap;

import java.util.function.BiPredicate;

/**
 * @author ztellman
 */
public interface IMapNode<K, V> extends Iterable<IMap.IEntry<K, V>> {

  IMapNode<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge);

  Object get(int shift, int hash, K key, BiPredicate<K, K> equals, Object defaultValue);

  IMapNode<K, V> remove(int shift, Object editor, int hash, K key, BiPredicate<K, K> equals);

  long size();

  IMap.IEntry<K, V> nth(long idx);
}
