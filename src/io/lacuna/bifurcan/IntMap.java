package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.IntMapNodes;
import io.lacuna.bifurcan.nodes.IntMapNodes.INode;

import java.util.Optional;

/**
 * @author ztellman
 */
public class IntMap<V> implements IMap<Long, V> {

  private static final Object DEFAULT_VALUE = new Object();

  private final Object editor = new Object();
  private final boolean linear;
  public INode<V> root;

  public IntMap() {
    this.root = IntMapNodes.Empty.EMPTY;
    this.linear = false;
  }

  public IntMap(INode<V> root, boolean linear) {
    this.root = root;
    this.linear = linear;
  }

  @Override
  public IMap<Long, V> put(Long key, V value, ValueMerger<V> merge) {
    INode<V> rootPrime = root.put(editor, key, value, merge);
    if (root == rootPrime) {
      return this;
    } else if (linear) {
      root = rootPrime;
      return this;
    } else {
      return new IntMap<V>(rootPrime, false);
    }
  }

  @Override
  public IMap<Long, V> remove(Long key) {
    INode<V> rootPrime = root.remove(editor, key);
    if (root == rootPrime) {
      return this;
    } else if (linear) {
      root = rootPrime;
      return this;
    } else {
      return new IntMap<V>(rootPrime, false);
    }
  }

  @Override
  public V get(Long key, V defaultValue) {
    return (V) root.get(key, defaultValue);
  }

  @Override
  public boolean contains(Long key) {
    return root.get(key, DEFAULT_VALUE) != DEFAULT_VALUE;
  }

  @Override
  public IList<IEntry<Long, V>> entries() {
    return Lists.from(size(), i -> root.nth((int) i), l -> root.iterator());
  }

  @Override
  public long size() {
    return root.size();
  }

  @Override
  public boolean isLinear() {
    return linear;
  }

  @Override
  public IMap<Long, V> forked() {
    return linear ? new IntMap<V>(root, false) : this;
  }

  @Override
  public IMap<Long, V> linear() {
    return linear ? this : new IntMap<V>(root, true);
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return Maps.equals(this, (IMap<Long, V>) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }
}
