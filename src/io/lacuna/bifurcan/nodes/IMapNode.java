package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IMap;

import java.util.function.BiPredicate;

/**
 * @author ztellman
 */
public interface IMapNode<K, V> extends Iterable<IMap.IEntry<K, V>> {

  class PutCommand<K, V> {
    public final Object editor;
    public final int hash;
    public final K key;
    public final V value;
    public final BiPredicate<K, K> equals;
    public final IMap.ValueMerger<V> merge;

    public PutCommand(Object editor, int hash, K key, V value, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge) {
      this.editor = editor;
      this.hash = hash;
      this.key = key;
      this.value = value;
      this.equals = equals;
      this.merge = merge;
    }

    public PutCommand(PutCommand<K, V> c, int hash, K key, V value) {
      this.editor = c.editor;
      this.hash = hash;
      this.key = key;
      this.value = value;
      this.equals = c.equals;
      this.merge = c.merge;
    }
  }

  class RemoveCommand<K, V> {
    public final Object editor;
    public final int hash;
    public final K key;
    public final BiPredicate<K, K> equals;

    public RemoveCommand(Object editor, int hash, K key, BiPredicate<K, K> equals) {
      this.editor = editor;
      this.hash = hash;
      this.key = key;
      this.equals = equals;
    }
  }

  IMapNode<K, V> put(int shift, PutCommand<K, V> command);

  IMapNode<K, V> remove(int shift, RemoveCommand<K, V> command);

  Object get(int shift, int hash, K key, BiPredicate<K, K> equals, Object defaultValue);

  long size();

  IMap.IEntry<K, V> nth(long idx);
}
