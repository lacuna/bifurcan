package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.IntMapNodes;
import io.lacuna.bifurcan.nodes.IntMapNodes.Node;
import io.lacuna.bifurcan.utils.IteratorStack;

import java.util.*;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

/**
 * @author ztellman
 */
public class IntMap<V> implements IMap<Long, V> {

  private static final Object DEFAULT_VALUE = new Object();

  private final Object editor = new Object();
  private final boolean linear;
  public Node<V> neg, pos;

  public IntMap() {
    this.neg = Node.NEG_EMPTY;
    this.pos = Node.POS_EMPTY;
    this.linear = false;
  }

  private IntMap(Node<V> neg, Node<V> pos, boolean linear) {
    this.neg = neg;
    this.pos = pos;
    this.linear = linear;
  }

  public static <V> IntMap<V> from(IMap<Number, V> m) {
    IntMap<V> map = new IntMap<V>().linear();
    for (IEntry<Number, V> e : m) {
      map = map.put((long) e.key(), e.value());
    }
    return map.forked();
  }

  public static <V> IntMap<V> from(java.util.Map<Number, V> m) {
    IntMap<V> map = new IntMap<V>().linear();
    for (Map.Entry<Number, V> e : m.entrySet()) {
      map = map.put((long) e.getKey(), e.getValue());
    }
    return map.forked();
  }

  public IntMap<V> slice(long min, long max) {
    Node<V> negPrime = neg.slice(editor, min, max);
    Node<V> posPrime = pos.slice(editor, min, max);
    return new IntMap<V>(
        negPrime == null ? Node.NEG_EMPTY : negPrime,
        posPrime == null ? Node.POS_EMPTY : posPrime,
        linear);
  }

  @Override
  public IntMap<V> merge(IMap<Long, V> b, BinaryOperator<V> mergeFn) {
    if (b instanceof IntMap) {
      IntMap<V> m = (IntMap<V>) b;
      return new IntMap<V>(neg.merge(new Object(), m.neg, mergeFn), pos.merge(new Object(), m.pos, mergeFn), linear);
    } else {
      return (IntMap<V>) Maps.merge(this, b, mergeFn);
    }
  }

  @Override
  public IntMap<V> difference(IMap<Long, ?> b) {
    if (b instanceof IntMap) {
      IntMap<V> m = (IntMap<V>) b;
      Node<V> negPrime = neg.difference(new Object(), m.neg);
      Node<V> posPrime = pos.difference(new Object(), m.pos);
      return new IntMap<V>(negPrime == null ? Node.NEG_EMPTY : negPrime, posPrime == null ? Node.POS_EMPTY : posPrime, linear);
    } else {
      return (IntMap<V>) Maps.difference(this, b.keys());
    }
  }

  @Override
  public IntMap<V> intersection(IMap<Long, ?> b) {
    if (b instanceof IntMap) {
      IntMap<V> m = (IntMap<V>) b;
      Node<V> negPrime = neg.intersection(new Object(), m.neg);
      Node<V> posPrime = pos.intersection(new Object(), m.pos);
      return new IntMap<V>(negPrime == null ? Node.NEG_EMPTY : negPrime, posPrime == null ? Node.POS_EMPTY : posPrime, linear);
    } else {
      return (IntMap<V>) Maps.intersection(new IntMap<V>().linear(), this, b.keys()).forked();
    }
  }

  public IntMap<V> put(long key, V value) {
    return put(key, value, (BinaryOperator<V>) Maps.MERGE_LAST_WRITE_WINS);
  }

  public IntMap<V> put(long key, V value, BinaryOperator<V> merge) {
    if (key < 0) {
      Node<V> negPrime = neg.put(editor, key, value, merge);
      if (neg == negPrime) {
        return this;
      } else if (linear) {
        neg = negPrime;
        return this;
      } else {
        return new IntMap<V>(negPrime, pos, false);
      }
    } else {
      Node<V> posPrime = pos.put(editor, key, value, merge);
      if (pos == posPrime) {
        return this;
      } else if (linear) {
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

  public IntMap<V> remove(long key) {
    if (key < 0) {
      Node<V> negPrime = neg.remove(editor, key);
      if (neg == negPrime) {
        return this;
      } else if (linear) {
        neg = negPrime;
        return this;
      } else {
        return new IntMap<V>(negPrime, pos, false);
      }
    } else {
      Node<V> posPrime = pos.remove(editor, key);
      if (pos == posPrime) {
        return this;
      } else if (linear) {
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

  public V get(long key, V defaultValue) {
    return (V) (key < 0 ? neg : pos).get(key, defaultValue);
  }

  @Override
  public V get(Long key, V defaultValue) {
    return get((long) key, defaultValue);
  }

  public boolean contains(long key) {
    return (key < 0 ? neg : pos).get(key, DEFAULT_VALUE) != DEFAULT_VALUE;
  }

  @Override
  public boolean contains(Long key) {
    return contains((long) key);
  }

  @Override
  public IList<IEntry<Long, V>> entries() {
    return Lists.from(
        size(),
        i -> (i < neg.size()) ? neg.nth((int) i) : pos.nth((int) (i - neg.size())),
        this::iterator);
  }

  @Override
  public Iterator<IEntry<Long, V>> iterator() {
    return new IteratorStack<>(neg.iterator(), pos.iterator());
  }

  public IEntry<Long, V> floor(long key) {
    if (key < 0) {
      return neg.floor(key);
    } else {
      IEntry<Long, V> entry = pos.floor(key);
      if (entry != null) {
        return entry;
      } else {
        return neg.size() > 0 ? neg.nth(pos.size() - 1) : null;
      }
    }
  }

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
  public long size() {
    return neg.size() + pos.size();
  }

  @Override
  public boolean isLinear() {
    return linear;
  }

  @Override
  public IntMap<V> forked() {
    return linear ? new IntMap<V>(neg, pos, false) : this;
  }

  @Override
  public IntMap<V> linear() {
    return linear ? this : new IntMap<V>(neg, pos, true);
  }

  @Override
  public IList<IMap<Long, V>> split(int parts) {
    // Java's generics are such a trash fire
    List<IMap<Long, V>> result = new List<IMap<Long, V>>().linear();

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
          .map(n -> new IntMap<V>(n, Node.POS_EMPTY, linear))
          .forEach(m -> result.addLast((IMap<Long, V>) m));
    }

    if (posParts > 0) {
      IntMapNodes.split(new Object(), pos, pos.size() / posParts)
          .stream()
          .map(n -> new IntMap<V>(Node.NEG_EMPTY, n, false))
          .forEach(m -> result.addLast((IMap<Long, V>) m));
    }

    return result.forked();
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
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
}
