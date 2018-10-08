package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.SortedMapNodes;
import io.lacuna.bifurcan.nodes.SortedMapNodes.Node;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BinaryOperator;

/**
 * A red-black tree based on Germane 2004 (http://matt.might.net/papers/germane2014deletion.pdf)
 */
public class SortedMap<K, V> implements ISortedMap<K, V> {

  private final Comparator<K> comparator;
  public Node<K, V> root;
  private final Object editor;

  public SortedMap() {
    this.root = SortedMapNodes.EMPTY_NODE;
    this.comparator = (Comparator<K>) Comparator.naturalOrder();
    this.editor = null;
  }

  public static <K, V> SortedMap<K, V> from(java.util.Map<K, V> m) {
    SortedMap<K, V> result = new SortedMap<K, V>().linear();
    m.entrySet().forEach(e -> result.put(e.getKey(), e.getValue()));
    return result.forked();
  }

  private SortedMap(Node<K, V> root, boolean linear, Comparator<K> comparator) {
    this.root = root;
    this.comparator = comparator;
    this.editor = linear ? new Object() : null;
  }

  @Override
  public IEntry<K, V> floor(K key) {
    Node<K, V> n = SortedMapNodes.floor(root, key, comparator);
    return n == null
      ? null
      : new Maps.Entry<>(n.k, n.v);
  }

  @Override
  public IEntry<K, V> ceil(K key) {
    Node<K, V> n = SortedMapNodes.ceil(root, key, comparator);
    return n == null
      ? null
      : new Maps.Entry<>(n.k, n.v);
  }

  @Override
  public SortedMap<K, V> slice(K min, K max) {
    return new SortedMap<>(SortedMapNodes.slice(root, min, max, comparator), isLinear(), comparator);
  }

  @Override
  public IList<? extends IMap<K, V>> split(int parts) {
    IList<Node<K, V>> acc = new LinearList<>();
    root.split(Math.max(1, (int) Math.ceil((double) root.size / parts)), acc);

    return acc.stream()
      .map(n -> new SortedMap<>(n, isLinear(), comparator))
      .collect(Lists.collector());
  }

  @Override
  public IMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    Node<K, V> rootPrime = root.put(key, value, merge, comparator);
    //rootPrime.checkInvariant();
    if (isLinear()) {
      root = rootPrime;
      return this;
    } else {
      return new SortedMap<>(rootPrime, false, comparator);
    }
  }

  @Override
  public IMap<K, V> remove(K key) {
    Node<K, V> rootPrime = root.remove(key, comparator);
    //rootPrime.checkInvariant();
    if (isLinear()) {
      root = rootPrime;
      return this;
    } else {
      return new SortedMap<>(rootPrime, false, comparator);
    }
  }

  @Override
  public V get(K key, V defaultValue) {
    Node<K, V> n = SortedMapNodes.find(root, key, comparator);
    return n == null ? defaultValue : n.v;
  }

  @Override
  public boolean contains(K key) {
    return SortedMapNodes.find(root, key, comparator) != null;
  }

  @Override
  public long indexOf(K key) {
    return SortedMapNodes.indexOf(root, key, comparator);
  }

  @Override
  public Iterator<IEntry<K, V>> iterator() {
    return SortedMapNodes.iterator(root);
  }

  @Override
  public IEntry<K, V> nth(long index) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException();
    }
    Node<K, V> n = SortedMapNodes.nth(root, (int) index);
    return new Maps.Entry<>(n.k, n.v);
  }

  @Override
  public long size() {
    return root.size;
  }

  @Override
  public IMap<K, V> clone() {
    return this;
  }

  @Override
  public boolean isLinear() {
    return editor != null;
  }

  @Override
  public SortedMap<K, V> forked() {
    return isLinear() ? new SortedMap<>(root, false, comparator) : this;
  }

  @Override
  public SortedMap<K, V> linear() {
    return isLinear() ? this : new SortedMap<>(root, true, comparator);
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof IMap) {
      return Maps.equals(this, (IMap) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }
}
