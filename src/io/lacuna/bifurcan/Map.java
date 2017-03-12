package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.MapNodes;
import io.lacuna.bifurcan.nodes.MapNodes.Node;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class Map<K, V> implements IMap<K, V> {

  private static final Object DEFAULT_VALUE = new Object();

  private final BiPredicate<K, K> equalsFn;
  private final ToIntFunction<K> hashFn;
  public Node<K, V> root;
  public final boolean linear;
  private final Object editor = new Object();

  public Map(ToIntFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    this(Node.EMPTY, hashFn, equalsFn, false);
  }

  public Map() {
    this(Node.EMPTY, Objects::hashCode, Objects::equals, false);
  }

  private Map(Node<K, V> root, ToIntFunction<K> hashFn, BiPredicate<K, K> equalsFn, boolean linear) {
    this.root = root;
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
    this.linear = linear;
  }

  @Override
  public V get(K key, V defaultValue) {
    Object val = root.get(0, keyHash(key), key, equalsFn, DEFAULT_VALUE);

    if (val == DEFAULT_VALUE) {
      return defaultValue;
    } else {
      return (V) val;
    }
  }

  @Override
  public Map<K, V> put(K key, V value) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public Map<K, V> put(K key, V value, BinaryOperator<V> merge) {
    Node<K, V> rootPrime = (Node<K, V>) root.put(0, editor, keyHash(key), key, value, equalsFn, merge);

    if (rootPrime == root) {
      return this;
    } else if (linear) {
      root = rootPrime;
      return this;
    } else {
      return new Map<K, V>(rootPrime, hashFn, equalsFn, false);
    }
  }

  @Override
  public Map<K, V> remove(K key) {
    Node<K, V> rootPrime = (Node<K, V>) root.remove(0, editor, keyHash(key), key, equalsFn);

    if (rootPrime == root) {
      return this;
    } else if (linear) {
      root = rootPrime;
      return this;
    } else {
      return new Map<K, V>(rootPrime, hashFn, equalsFn, false);
    }
  }

  @Override
  public boolean contains(K key) {
    return root.get(0, keyHash(key), key, equalsFn, DEFAULT_VALUE) != DEFAULT_VALUE;
  }

  @Override
  public IList<IEntry<K, V>> entries() {
    return Lists.from(size(), i -> root.nth(i), () -> iterator());
  }

  @Override
  public Map<K, V> forked() {
    if (linear) {
      return new Map<>(root, hashFn, equalsFn, false);
    } else {
      return this;
    }
  }

  @Override
  public Map<K, V> linear() {
    if (linear) {
      return this;
    } else {
      return new Map<>(root, hashFn, equalsFn, true);
    }
  }

  @Override
  public IList<IMap<K, V>> split(int parts) {
    IList<IMap<K, V>> list = new List<IMap<K, V>>().linear();
    MapNodes.split(new Object(), root, (int) Math.ceil(size() / (float) parts))
        .stream()
        .map(n -> new Map<K, V>(n, hashFn, equalsFn, false))
        .forEach(list::addLast);
    return list.forked();
  }

  @Override
  public Map<K, V> union(IMap<K, V> m) {
    return merge(m, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public Map<K, V> merge(IMap<K, V> b, BinaryOperator<V> mergeFn) {
    // TODO
    if (false) /*(b instanceof Map)*/ {
      Node<K, V> rootPrime = MapNodes.merge(0, editor, root, ((Map) b).root, equalsFn, mergeFn);
      return linear ? this : new Map<>(rootPrime, hashFn, equalsFn, false);
    } else {
      return (Map<K, V>) Maps.merge(this, b, mergeFn);
    }
  }

  @Override
  public Map<K, V> difference(ISet<K> keys) {
    if (keys instanceof Set) {
      return difference(((Set<K>) keys).map);
    } else {
      return (Map<K, V>) Maps.difference(this, keys);
    }
  }

  @Override
  public Map<K, V> intersection(ISet<K> keys) {
    if (keys instanceof Set) {
      return intersection(((Set<K>) keys).map);
    } else {
      return (Map<K, V>) Maps.intersection(new Map<K, V>().linear(), this, keys).forked();
    }
  }

  @Override
  public Map<K, V> difference(IMap<K, ?> m) {
    if (m instanceof Map) {
      Node<K, V> rootPrime = MapNodes.difference(0, editor, root, ((Map) m).root, equalsFn);
      return linear ? this : new Map<>(rootPrime, hashFn, equalsFn, false);
    } else {
      return difference(m.keys());
    }
  }

  @Override
  public Map<K, V> intersection(IMap<K, ?> m) {
    if (m instanceof Map) {
      Node<K, V> rootPrime = MapNodes.intersection(0, editor, root, ((Map) m).root, equalsFn);
      return linear ? this : new Map<>(rootPrime, hashFn, equalsFn, false);
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
    return linear;
  }

  @Override
  public Iterator<IEntry<K, V>> iterator() {
    return root.iterator();
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
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

  private int keyHash(K key) {
    int hash = hashFn.applyAsInt(key);

    // make sure we don't have too many collisions in the lower bits
    hash ^= (hash >>> 20) ^ (hash >>> 12);
    hash ^= (hash >>> 7) ^ (hash >>> 4);
    return hash;
  }
}
