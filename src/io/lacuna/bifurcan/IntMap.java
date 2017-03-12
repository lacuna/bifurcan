package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.IntMapNodes;
import io.lacuna.bifurcan.nodes.IntMapNodes.Node;
import io.lacuna.bifurcan.utils.IteratorStack;

import java.util.Objects;
import java.util.Optional;
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

  public IntMap(Node<V> neg, Node<V> pos, boolean linear) {
    this.neg = neg;
    this.pos = pos;
    this.linear = linear;
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
        () -> new IteratorStack<>(neg.iterator(), pos.iterator()));
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
