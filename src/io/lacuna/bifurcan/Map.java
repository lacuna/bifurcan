package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.MapNodes;
import io.lacuna.bifurcan.nodes.MapNodes.Node;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.*;

/**
 * An implementation of an immutable hash-map based on the general approach described by Steindorfer and Vinju in
 * <a href=https://michael.steindorfer.name/publications/oopsla15.pdf>this paper</a>.  It allows for customized hashing
 * and equality semantics, and due to its default reliance on Java's semantics, it is significantly faster than
 * Clojure's {@code PersistentHashMap} for lookups and construction for collections smaller than 100k entries, and has
 * equivalent performance for larger collections.
 * <p>
 * By ensuring that equivalent maps always have equivalent layout in memory, it can perform equality checks and set
 * operations (union, difference, intersection) significantly faster than a more naive implementation.  By keeping the
 * memory layout of each node more compact, iteration is at least 2x faster than Clojure's implementation.
 *
 * @author ztellman
 */
public class Map<K, V> extends IMap.Mixin<K, V> {

  public static final Map EMPTY = new Map();

  private static final Object DEFAULT_VALUE = new Object();

  private final BiPredicate<K, K> equalsFn;
  private final ToLongFunction<K> hashFn;
  private Node<K, V> root;
  final Object editor;

  ///

  /**
   * @param map another map
   * @return an equivalent forked map, with the same equality semantics
   */
  public static <K, V> Map<K, V> from(IMap<K, V> map) {
    if (map instanceof Map) {
      return (Map<K, V>) map.forked();
    } else {
      Map<K, V> result = new Map<K, V>(map.keyHash(), map.keyEquality()).linear();
      map.forEach(e -> result.put(e.key(), e.value()));
      return result.forked();
    }
  }

  /**
   * @return a forked map with the same contents as {@code map}
   */
  public static <K, V> Map<K, V> from(java.util.Map<K, V> map) {
    return map.entrySet().stream().collect(Maps.collector(java.util.Map.Entry::getKey, java.util.Map.Entry::getValue));
  }

  /**
   * @param entries an iterator of {@code IEntry} objects
   * @return a forked map containing the remaining entries
   */
  public static <K, V> Map<K, V> from(Iterator<IEntry<K, V>> entries) {
    Map<K, V> m = new Map<K, V>().linear();
    entries.forEachRemaining(e -> m.put(e.key(), e.value()));
    return m.forked();
  }

  /**
   * @param entries a list of {@code IEntry} objects
   * @return a forked map containing these entries
   */
  public static <K, V> Map<K, V> from(IList<IEntry<K, V>> entries) {
    return entries.stream().collect(Maps.collector(IEntry::key, IEntry::value));
  }

  /**
   * Creates a map.
   *
   * @param hashFn   a function which yields the hash value of keys
   * @param equalsFn a function which checks equality of keys
   */
  public Map(ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    this(Node.EMPTY, hashFn, equalsFn, false);
  }

  public Map() {
    this(Node.EMPTY, Maps.DEFAULT_HASH_CODE, Maps.DEFAULT_EQUALS, false);
  }

  private Map(Node<K, V> root, ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn, boolean linear) {
    this.root = root;
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
    this.editor = linear ? new Object() : null;
  }

  public static <K, V> Map<K, V> empty() {
    return (Map<K, V>) EMPTY;
  }

  ///

  @Override
  public Set<K> keys() {
    return new Set<K>((Map<K, Void>) this);
  }

  @Override
  public ToLongFunction<K> keyHash() {
    return hashFn;
  }

  @Override
  public BiPredicate<K, K> keyEquality() {
    return equalsFn;
  }

  @Override
  public V get(K key, V defaultValue) {
    Object val = MapNodes.get(root, 0, keyHash(key), key, equalsFn, DEFAULT_VALUE);
    return val == DEFAULT_VALUE ? defaultValue : (V) val;
  }

  @Override
  public Map<K, V> put(K key, V value, BinaryOperator<V> merge) {
    return put(key, value, merge, isLinear() ? editor : new Object());
  }

  public Map<K, V> put(K key, V value, BinaryOperator<V> merge, Object editor) {
    Node<K, V> rootPrime = root.put(0, editor, keyHash(key), key, value, equalsFn, merge);

    if (isLinear() && editor == this.editor) {
      root = rootPrime;
      super.hash = -1;
      return this;
    } else {
      return new Map<K, V>(rootPrime, hashFn, equalsFn, false);
    }
  }

  @Override
  public Map<K, V> update(K key, UnaryOperator<V> update) {
    return update(key, update, isLinear() ? editor : new Object());
  }

  public Map<K, V> update(K key, UnaryOperator<V> update, Object editor) {
    return put(key, update.apply(get(key, null)), (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS, editor);
  }

  @Override
  public Map<K, V> remove(K key) {
    return remove(key, isLinear() ? editor : new Object());
  }

  public Map<K, V> remove(K key, Object editor) {
    Node<K, V> rootPrime = (Node<K, V>) root.remove(0, editor, keyHash(key), key, equalsFn);

    if (isLinear() && editor == this.editor) {
      root = rootPrime;
      super.hash = -1;
      return this;
    } else {
      return new Map<K, V>(rootPrime, hashFn, equalsFn, false);
    }
  }

  @Override
  public boolean contains(K key) {
    return MapNodes.contains(root, 0, keyHash(key), key, equalsFn);
  }

  @Override
  public OptionalLong indexOf(K key) {
    long idx = root.indexOf(0, keyHash(key), key, keyEquality());
    return idx < 0 ? OptionalLong.empty() : OptionalLong.of(idx);
  }

  @Override
  public IEntry<K, V> nth(long idx) {
    if (idx < 0 || idx >= size()) {
      throw new IndexOutOfBoundsException();
    }
    return root.nth(idx);
  }

  @Override
  public Map<K, V> forked() {
    if (isLinear()) {
      return new Map<>(root, hashFn, equalsFn, false);
    } else {
      return this;
    }
  }

  @Override
  public Map<K, V> linear() {
    if (isLinear()) {
      return this;
    } else {
      return new Map<>(root, hashFn, equalsFn, true);
    }
  }

  @Override
  public <U> Map<K, U> mapValues(BiFunction<K, V, U> f) {
    return new Map<K, U>(root.mapVals(new Object(), f), hashFn, equalsFn, isLinear());
  }

  @Override
  public List<Map<K, V>> split(int parts) {
    List<Map<K, V>> list = new List<Map<K, V>>().linear();
    MapNodes.split(new Object(), root, (int) Math.ceil(size() / (float) parts))
        .stream()
        .map(n -> new Map<K, V>(n, hashFn, equalsFn, false))
        .forEach(list::addLast);
    return list.forked();
  }

  @Override
  public Map<K, V> union(IMap<K, V> m) {
    return merge(m, Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public Map<K, V> merge(IMap<K, V> m, BinaryOperator<V> mergeFn) {
    if (m instanceof Map && Maps.equivEquality(this, m)) {
      Node<K, V> rootPrime = MapNodes.merge(0, editor, root, ((Map) m).root, equalsFn, mergeFn);
      return new Map<>(rootPrime, hashFn, equalsFn, isLinear());
    } else {
      return (Map<K, V>) Maps.merge(this.clone(), m, mergeFn);
    }
  }

  @Override
  public Map<K, V> difference(ISet<K> keys) {
    if (keys instanceof Set && Maps.equivEquality(this, keys)) {
      return difference(((Set<K>) keys).map);
    } else {
      return (Map<K, V>) Maps.difference(this.clone(), keys);
    }
  }

  @Override
  public Map<K, V> intersection(ISet<K> keys) {
    if (keys instanceof Set && Maps.equivEquality(this, keys)) {
      return intersection(((Set<K>) keys).map);
    } else {
      Map<K, V> map = (Map<K, V>) Maps.intersection(new Map<K, V>(hashFn, equalsFn).linear(), this, keys);
      return isLinear() ? map : map.forked();
    }
  }

  @Override
  public Map<K, V> difference(IMap<K, ?> m) {
    if (m instanceof Map && Maps.equivEquality(this, m)) {
      Node<K, V> rootPrime = MapNodes.difference(0, editor, root, ((Map) m).root, equalsFn);
      return new Map<>(rootPrime == null ? Node.EMPTY : rootPrime, hashFn, equalsFn, isLinear());
    } else {
      return difference(m.keys());
    }
  }

  @Override
  public Map<K, V> intersection(IMap<K, ?> m) {
    if (m instanceof Map && Maps.equivEquality(this, m)) {
      Node<K, V> rootPrime = MapNodes.intersection(0, editor, root, ((Map) m).root, equalsFn);
      return new Map<>(rootPrime == null ? Node.EMPTY : rootPrime, hashFn, equalsFn, isLinear());
    } else {
      return intersection(m.keys());
    }
  }

  @Override
  public long size() {
    return root.size();
  }

  @Override
  public boolean isLinear() {
    return editor != null;
  }

  @Override
  public Iterator<IEntry<K, V>> iterator() {
    return root.iterator();
  }

  @Override
  public boolean equals(IMap<K, V> m, BiPredicate<V, V> valEquals) {
    if (m instanceof Map && keyHash() == m.keyHash()) {
      return root.equals(((Map<K, V>) m).root, equalsFn, valEquals);
    } else {
      return Maps.equals(this, m, valEquals);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return equals((IMap<K, V>) obj, Objects::equals);
    }
    return false;
  }

  @Override
  public Map<K, V> clone() {
    return isLinear() ? forked().linear() : this;
  }

  private int keyHash(K key) {
    long hash64 = hashFn.applyAsLong(key);
    int hash = (int) ((hash64 >> 32) ^ hash64);

    // make sure we don't have too many collisions in the lower bits
    hash ^= (hash >>> 20) ^ (hash >>> 12);
    hash ^= (hash >>> 7) ^ (hash >>> 4);
    return hash;
  }
}
