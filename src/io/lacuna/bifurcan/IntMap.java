package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.IntMapNodes;
import io.lacuna.bifurcan.nodes.IntMapNodes.Node;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.*;
import java.util.Map;
import java.util.function.*;

/**
 * A map which has integer keys, which is an combination of Okasaki and Gill's
 * <a href="http://ittc.ku.edu/~andygill/papers/IntMap98.pdf">Fast Mergeable Integer Maps</a> with the memory layout
 * suggested by Steindorfer and Vinju used in {@link io.lacuna.bifurcan.Map}, with which it shares the same broad performance
 * characteristics.
 * <p>
 * This collection keeps the keys in sorted order, and can thought of as either a map of integers or a sparse vector.
 * It provides {@link IntMap#slice(Long, Long)}, {@link IntMap#floor(Long)}, and {@link IntMap#ceil(Long)} methods which
 * allow for lookups and filtering on its keys.
 *
 * @author ztellman
 */
public class IntMap<V> implements ISortedMap<Long, V>, Cloneable {

  static final ToLongFunction<Long> HASH = n -> n;
  private static final Object DEFAULT_VALUE = new Object();

  final Object editor;
  private Node<V> neg, pos;
  private int hash = -1;

  /**
   * @param m another map
   * @return a forked copy of the map
   */
  public static <V> IntMap<V> from(IMap<Number, V> m) {
    if (m instanceof IntMap) {
      return (IntMap) m.forked();
    } else {
      return from(m.entries());
    }
  }

  /**
   * @return a forked copy of {@code m}
   */
  public static <V> IntMap<V> from(java.util.Map<Number, V> m) {
    return from(m.entrySet());
  }

  /**
   * @param collection a collection of {@link java.util.Map.Entry} objects
   * @return an {@link IntMap} representing the entries in the collection
   */
  public static <V> IntMap<V> from(Collection<Map.Entry<Number, V>> collection) {
    IntMap<V> map = new IntMap<V>().linear();
    for (Map.Entry<Number, V> entry : collection) {
      map = map.put((long) entry.getKey(), entry.getValue());
    }
    return map.forked();
  }

  /**
   * @param list a list of {@link IEntry} objects
   * @return an {@link IntMap} representing the entries in the list
   */
  public static <V> IntMap<V> from(IList<IEntry<Number, V>> list) {
    IntMap<V> map = new IntMap<V>().linear();
    for (IEntry<Number, V> entry : list) {
      map = map.put((long) entry.key(), entry.value());
    }
    return map.forked();
  }

  public IntMap() {
    this.neg = Node.NEG_EMPTY;
    this.pos = Node.POS_EMPTY;
    this.editor = null;
  }

  private IntMap(Node<V> neg, Node<V> pos, boolean linear) {
    this.neg = neg;
    this.pos = pos;
    this.editor = linear ? new Object() : null;
  }

  ///


  @Override
  public Comparator<Long> comparator() {
    return Comparator.naturalOrder();
  }

  @Override
  public ToLongFunction<Long> keyHash() {
    return HASH;
  }

  @Override
  public BiPredicate<Long, Long> keyEquality() {
    return Long::equals;
  }

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within {@code [min, max]}
   */
  public IntMap<V> slice(long min, long max) {
    Node<V> negPrime = neg.slice(editor, min, max);
    Node<V> posPrime = pos.slice(editor, min, max);
    return new IntMap<V>(
      negPrime == null ? Node.NEG_EMPTY : negPrime,
      posPrime == null ? Node.POS_EMPTY : posPrime,
      isLinear());
  }

  @Override
  public IntMap<V> slice(Long min, Long max) {
    return slice((long) min, (long) max);
  }

  @Override
  public IntMap<V> merge(IMap<Long, V> b, BinaryOperator<V> mergeFn) {
    if (b instanceof IntMap) {
      IntMap<V> m = (IntMap<V>) b;
      return new IntMap<V>(
        IntMapNodes.merge(new Object(), neg, m.neg, mergeFn),
        IntMapNodes.merge(new Object(), pos, m.pos, mergeFn),
        isLinear());
    } else {
      return (IntMap<V>) Maps.merge(this.clone(), b, mergeFn);
    }
  }

  @Override
  public IntMap<V> difference(ISet<Long> keys) {
    if (keys instanceof IntSet) {
      return difference(((IntSet) keys).m);
    } else {
      return (IntMap<V>) Maps.difference(this, keys);
    }
  }

  @Override
  public IntMap<V> intersection(ISet<Long> keys) {
    if (keys instanceof IntSet) {
      return intersection(((IntSet) keys).m);
    } else {
      return (IntMap<V>) Maps.intersection(new IntMap<V>().linear(), this, keys);
    }
  }

  @Override
  public IntMap<V> union(IMap<Long, V> m) {
    return merge(m, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public IntMap<V> difference(IMap<Long, ?> b) {
    if (b instanceof IntMap) {
      IntMap<V> m = (IntMap<V>) b;
      Node<V> negPrime = IntMapNodes.difference(new Object(), neg, m.neg);
      Node<V> posPrime = IntMapNodes.difference(new Object(), pos, m.pos);
      return new IntMap<V>(negPrime == null ? Node.NEG_EMPTY : negPrime, posPrime == null ? Node.POS_EMPTY : posPrime, isLinear());
    } else {
      return (IntMap<V>) Maps.difference(this.clone(), b.keys());
    }
  }

  @Override
  public IntMap<V> intersection(IMap<Long, ?> b) {
    if (b instanceof IntMap) {
      IntMap<V> m = (IntMap<V>) b;
      Node<V> negPrime = IntMapNodes.intersection(new Object(), neg, m.neg);
      Node<V> posPrime = IntMapNodes.intersection(new Object(), pos, m.pos);
      return new IntMap<V>(negPrime == null ? Node.NEG_EMPTY : negPrime, posPrime == null ? Node.POS_EMPTY : posPrime, isLinear());
    } else {
      IntMap<V> result = (IntMap<V>) Maps.intersection(new IntMap<V>().linear(), this, b.keys());
      return isLinear() ? result : result.forked();
    }
  }

  /**
   * @param key   a primitive {@code long} key
   * @param value a value
   * @return an updated {@link IntMap} with {@code value} under {@code key}
   */
  public IntMap<V> put(long key, V value) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  public IntMap<V> put(long key, V value, Object editor) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS, editor);
  }

  /**
   * @param key   a primitive {@code long} key
   * @param value a value
   * @param merge a function which will be invoked if there is a pre-existing value under {@code key}, with the current
   *              value as the first argument and new value as the second, to determine the combined result
   * @return an updated map
   */
  public IntMap<V> put(long key, V value, BinaryOperator<V> merge) {
    return put(key, value, merge, isLinear() ? editor : new Object());
  }

  public IntMap<V> put(long key, V value, BinaryOperator<V> merge, Object editor) {
    if (key < 0) {
      Node<V> negPrime = neg.put(editor, key, value, merge);
      if (neg == negPrime) {
        return this;
      } else if (isLinear()) {
        hash = -1;
        neg = negPrime;
        return this;
      } else {
        return new IntMap<V>(negPrime, pos, false);
      }
    } else {
      Node<V> posPrime = pos.put(editor, key, value, merge);
      if (pos == posPrime) {
        return this;
      } else if (isLinear()) {
        hash = -1;
        pos = posPrime;
        return this;
      } else {
        return new IntMap<>(neg, posPrime, false);
      }
    }
  }

  @Override
  public IntMap<V> put(Long key, V value) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public IntMap<V> put(Long key, V value, BinaryOperator<V> merge) {
    return put((long) key, value, merge);
  }

  /**
   * @return an updated map that does not contain {@code key}
   */
  public IntMap<V> remove(long key) {
    return remove(key, isLinear() ? editor : new Object());
  }

  public IntMap<V> remove(long key, Object editor) {
    if (key < 0) {
      Node<V> negPrime = neg.remove(editor, key);
      if (neg == negPrime) {
        return this;
      } else if (isLinear()) {
        hash = -1;
        neg = negPrime;
        return this;
      } else {
        return new IntMap<V>(negPrime, pos, false);
      }
    } else {
      Node<V> posPrime = pos.remove(editor, key);
      if (pos == posPrime) {
        return this;
      } else if (isLinear()) {
        hash = -1;
        pos = posPrime;
        return this;
      } else {
        return new IntMap<>(neg, posPrime, false);
      }
    }
  }

  @Override
  public IntMap<V> remove(Long key) {
    return remove((long) key);
  }

  @Override
  public <U> IntMap<U> mapValues(BiFunction<Long, V, U> f) {
    Object editor = new Object();
    return new IntMap<>(neg.mapVals(editor, f), pos.mapVals(editor, f), isLinear());
  }

  public Optional<V> get(long key) {
    Object o = (key < 0 ? neg : pos).get(key, DEFAULT_VALUE);
    return o == DEFAULT_VALUE ? Optional.empty() : Optional.of((V) o);
  }

  public V get(long key, V defaultValue) {
    return (V) (key < 0 ? neg : pos).get(key, defaultValue);
  }

  @Override
  public V get(Long key, V defaultValue) {
    return get((long) key, defaultValue);
  }

  @Override
  public IntMap<V> update(Long key, UnaryOperator<V> update) {
    return update((long) key, update);
  }

  public IntMap<V> update(long key, UnaryOperator<V> update) {
    return put(key, update.apply(get(key, null)), isLinear() ? editor : new Object());
  }

  public IntMap<V> update(long key, UnaryOperator<V> update, Object editor) {
    return put(key, update.apply(get(key, null)), editor);
  }

  public boolean contains(long key) {
    return (key < 0 ? neg : pos).get(key, DEFAULT_VALUE) != DEFAULT_VALUE;
  }

  @Override
  public boolean contains(Long key) {
    return contains((long) key);
  }

  @Override
  public OptionalLong indexOf(Long key) {
    return indexOf((long) key);
  }

  public OptionalLong indexOf(long key) {
    if (key < 0) {
      return neg.indexOf(key);
    } else {
      OptionalLong index = pos.indexOf(key);
      return index.isPresent() ? OptionalLong.of(index.getAsLong() + neg.size) : index;
    }
  }

  @Override
  public IEntry<Long, V> nth(long idx) {
    return idx < neg.size() ? neg.nth(idx) : pos.nth(idx - neg.size());
  }

  @Override
  public Iterator<IEntry<Long, V>> iterator() {
    return Iterators.concat(neg.iterator(), pos.iterator());
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  public IEntry<Long, V> floor(long key) {
    if (key < 0) {
      return neg.floor(key);
    } else {
      IEntry<Long, V> entry = pos.floor(key);
      if (entry != null) {
        return entry;
      } else {
        return neg.size() > 0 ? neg.nth(neg.size() - 1) : null;
      }
    }
  }

  @Override
  public IEntry<Long, V> floor(Long key) {
    return floor((long) key);
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  public IEntry<Long, V> ceil(long key) {
    if (key >= 0) {
      return pos.ceil(key);
    } else {
      IEntry<Long, V> entry = neg.ceil(key);
      if (entry != null) {
        return entry;
      } else {
        return pos.size() > 0 ? pos.nth(0) : null;
      }
    }
  }

  @Override
  public IEntry<Long, V> ceil(Long key) {
    return ceil((long) key);
  }

  @Override
  public long size() {
    return neg.size() + pos.size();
  }

  @Override
  public boolean isLinear() {
    return editor != null;
  }

  @Override
  public IntMap<V> forked() {
    return isLinear() ? new IntMap<V>(neg, pos, false) : this;
  }

  @Override
  public IntMap<V> linear() {
    return isLinear() ? this : new IntMap<V>(neg, pos, true);
  }

  @Override
  public List<IntMap<V>> split(int parts) {
    List<IntMap<V>> result = new List<IntMap<V>>().linear();

    parts = Math.max(1, Math.min((int) size(), parts));
    if (parts == 1 || size() == 0) {
      return result.addLast(this).forked();
    }

    int estParts = (int) Math.min(parts - 1, Math.max(1, (neg.size() / (double) size()) * parts));
    int negParts = pos.size() == 0 ? parts : (neg.size == 0 ? 0 : estParts);
    int posParts = parts - negParts;

    if (negParts > 0) {
      IntMapNodes.split(new Object(), neg, neg.size() / negParts)
        .stream()
        .map(n -> new IntMap<V>(n, Node.POS_EMPTY, isLinear()))
        .forEach(m -> result.addLast((IntMap<V>) m));
    }

    if (posParts > 0) {
      IntMapNodes.split(new Object(), pos, pos.size() / posParts)
        .stream()
        .map(n -> new IntMap<V>(Node.NEG_EMPTY, n, false))
        .forEach(m -> result.addLast((IntMap<V>) m));
    }

    return result.forked();
  }



  @Override
  public int hashCode() {
    if (hash == -1) {
      hash = (int) Maps.hash(this);
    }
    return hash;
  }

  @Override
  public boolean equals(IMap<Long, V> o, BiPredicate<V, V> valEquals) {
    if (o instanceof IntMap) {
      IntMap<V> m = (IntMap<V>) o;
      return neg.equals(m.neg, valEquals) && pos.equals(m.pos, valEquals);
    } else {
      return Maps.equals(this, o, valEquals);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return equals((IMap<Long, V>) obj, Objects::equals);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }

  @Override
  public IntMap<V> clone() {
    return new IntMap<>(neg, pos, isLinear());
  }
}
