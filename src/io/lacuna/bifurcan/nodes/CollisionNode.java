package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.ArrayVector;

import java.util.Iterator;
import java.util.function.BiPredicate;

/**
 * @author ztellman
 */
public class CollisionNode<K, V> implements IMapNode<K, V> {

  public final Object[] entries;

  public CollisionNode(K k1, V v1, K k2, V v2) {
    this(new Object[] {k1, v1, k2, v2});
  }

  private CollisionNode(Object[] entries) {
    this.entries = entries;
  }

  private int indexOf(K key, BiPredicate<K, K> equals) {
    for (int i = 0; i < entries.length; i += 2) {
      if (equals.test(key, (K) entries[i])) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public IMapNode<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge) {
    int idx = indexOf(key, equals);
    if (idx < 0) {
      return new CollisionNode<K, V>(ArrayVector.append(entries, key, value));
    } else {
      return new CollisionNode<K, V>(ArrayVector.set(entries, idx, key, merge.merge((V) entries[idx + 1], value)));
    }
  }

  @Override
  public Object get(int shift, int hash, K key, BiPredicate<K, K> equals, Object defaultValue) {
    int idx = indexOf(key, equals);
    if (idx < 0) {
      return defaultValue;
    } else {
      return entries[idx + 1];
    }
  }

  @Override
  public IMapNode<K, V> remove(int shift, Object editor, int hash, K key, BiPredicate<K, K> equals) {
    int idx = indexOf(key, equals);
    if (idx < 0) {
      return this;
    } else {
      return new CollisionNode<K, V>(ArrayVector.remove(entries, idx, 2));
    }
  }

  public long size() {
    return entries.length >> 1;
  }

  @Override
  public IMap.IEntry<K, V> nth(long idx) {
    int i = (int) idx << 1;
    return new Maps.Entry<K, V>((K) entries[i], (V) entries[i + 1]);
  }

  @Override
  public Iterator<IMap.IEntry<K, V>> iterator() {
    return new Iterator<IMap.IEntry<K, V>>() {
      int idx = 0;
      @Override
      public boolean hasNext() {
        return idx < entries.length;
      }

      @Override
      public IMap.IEntry<K, V> next() {
        idx += 2;
        return new Maps.Entry<K, V>((K) entries[idx - 2], (V) entries[idx - 1]);
      }
    };
  }
}
