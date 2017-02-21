package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.ChampNode;

import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class Map<K, V>  implements IMap<K, V> {

  private static final Object DEFAULT_VALUE = new Object();

  private final BiPredicate<K, K> equalsFn;
  private final ToIntFunction<K> hashFn;
  public final ChampNode<K, V> root;

  public Map(ToIntFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    this(ChampNode.EMPTY, hashFn, equalsFn);
  }

  public Map() {
    this(ChampNode.EMPTY, Objects::hashCode, Objects::equals);
  }

  private Map(ChampNode<K, V> root, ToIntFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    this.root = root;
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
  }

  @Override
  public V get(K key, V defaultValue) {
    Object val = root.get(0, hashFn.applyAsInt(key), key, equalsFn, DEFAULT_VALUE);
    if (val == DEFAULT_VALUE) {
      return defaultValue;
    } else {
      return (V) val;
    }
  }

  @Override
  public IMap<K, V> put(K key, V value, ValueMerger<V> merge) {
    ChampNode<K, V> rootPrime = root.put(0, this, hashFn.applyAsInt(key), key, value, equalsFn, merge);
    if (rootPrime == root) {
      return this;
    } else {
      return new Map<K, V>(rootPrime, hashFn, equalsFn);
    }
  }

  @Override
  public IMap<K, V> remove(K key) {
    ChampNode<K, V> rootPrime = root.remove(0, this, hashFn.applyAsInt(key), key, equalsFn);
    if (rootPrime == root) {
      return this;
    } else {
      return new Map<K, V>(rootPrime, hashFn, equalsFn);
    }
  }

  @Override
  public boolean contains(K key) {
    return root.get(0, hashFn.applyAsInt(key), key, equalsFn, DEFAULT_VALUE) != DEFAULT_VALUE;
  }

  @Override
  public IList<IEntry<K, V>> entries() {
    return Lists.from(size(), i -> root.nth(i));
  }

  @Override
  public ISet<K> keys() {
    IList<IEntry<K, V>> entries = entries();
    return Sets.from(
        Lists.from(size(), i -> entries.nth(i).key()),
        this::contains);
  }

  @Override
  public long size() {
    return root.size();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return Maps.equals(this, (IMap<K, V>) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }
}
