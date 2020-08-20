package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.SortedMapNodes;
import io.lacuna.bifurcan.nodes.SortedMapNodes.Node;

import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.*;

/**
 * A red-black tree based on <a href="http://matt.might.net/papers/germane2014deletion.pdf">Germane 2014</a>.
 */
public class SortedMap<K, V> implements ISortedMap<K, V> {

  private static final SortedMap EMPTY = new SortedMap();

  private final Comparator<K> comparator;
  public Node<K, V> root;
  private final Object editor;
  private int hash = -1;

  public SortedMap() {
    this(SortedMapNodes.EMPTY_NODE, false, (Comparator<K>) Comparator.naturalOrder());
  }

  public SortedMap(Comparator<K> comparator) {
    this(SortedMapNodes.EMPTY_NODE, false, comparator);
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

  public static <K, V> SortedMap<K, V> empty() {
    return EMPTY;
  }

  @Override
  public Comparator<K> comparator() {
    return comparator;
  }

  @Override
  public OptionalLong floorIndex(K key) {
    long idx = root.floorIndex(key, comparator, 0);
    return idx < 0 ? OptionalLong.empty() : OptionalLong.of(idx);
  }

  @Override
  public OptionalLong ceilIndex(K key) {
    long idx = root.ceilIndex(key, comparator, 0);
    return idx < 0 ? OptionalLong.empty() : OptionalLong.of(idx);
  }

  @Override
  public SortedMap<K, V> update(K key, UnaryOperator<V> update) {
    return put(key, update.apply(this.get(key, null)));
  }

  @Override
  public SortedMap<K, V> put(K key, V value) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public SortedMap<K, V> slice(K min, K max) {
    return new SortedMap<>(root.slice(min, max, comparator), isLinear(), comparator);
  }

  @Override
  public List<SortedMap<K, V>> split(int parts) {
    IList<Node<K, V>> acc = new LinearList<>();
    root.split(Math.max(1, (int) Math.ceil((double) root.size / parts)), acc);

    return acc.stream()
      .map(n -> new SortedMap<>(n, isLinear(), comparator))
      .collect(Lists.collector());
  }

  @Override
  public SortedMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    Node<K, V> rootPrime = root.put(key, value, merge, comparator);
    //rootPrime.checkInvariant();
    if (isLinear()) {
      hash = -1;
      root = rootPrime;
      return this;
    } else {
      return new SortedMap<>(rootPrime, false, comparator);
    }
  }

  @Override
  public SortedMap<K, V> remove(K key) {
    Node<K, V> rootPrime = root.remove(key, comparator);
    //rootPrime.checkInvariant();
    if (isLinear()) {
      hash = -1;
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
  public OptionalLong indexOf(K key) {
    long idx = SortedMapNodes.indexOf(root, key, comparator);
    return idx < 0 ? OptionalLong.empty() : OptionalLong.of(idx);
  }

  @Override
  public <U> SortedMap<K, U> mapValues(BiFunction<K, V, U> f) {
    return new SortedMap<>(root.mapValues(f), isLinear(), comparator);
  }

  @Override
  public Iterator<IEntry<K, V>> iterator() {
    return SortedMapNodes.iterator(root);
  }

  @Override
  public IEntry<K, V> nth(long idx) {
    if (idx < 0 || idx >= size()) {
      throw new IndexOutOfBoundsException(String.format("%d must be within [0,%d)", idx, size()));
    }
    Node<K, V> n = SortedMapNodes.nth(root, (int) idx);
    return IEntry.of(n.k, n.v);
  }

  public long size() {
    return root.size;
  }

  @Override
  public SortedMap<K, V> clone() {
    return isLinear() ? forked().linear() : this;
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
  public ToLongFunction<K> keyHash() {
    return Maps.DEFAULT_HASH_CODE;
  }

  @Override
  public BiPredicate<K, K> keyEquality() {
    return Maps.DEFAULT_EQUALS;
  }

  @Override
  public int hashCode() {
    if (hash == -1) {
      hash = (int) Maps.hash(this);
    }
    return hash;
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
