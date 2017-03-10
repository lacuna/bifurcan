package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.IntMapNodes;
import io.lacuna.bifurcan.nodes.IntMapNodes.Node;
import io.lacuna.bifurcan.utils.IteratorStack;

import java.util.Optional;

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

  public IntMap<V> put(long key, V value, ValueMerger<V> merge) {
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
  public IntMap<V> put(Long key, V value, ValueMerger<V> merge) {
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
    return Lists.from(size(),
        i -> (i < neg.size()) ? neg.nth((int) i) : pos.nth((int) (i - neg.size())),
        l -> new IteratorStack<>(neg.iterator(), pos.iterator()));
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
  public IMap<Long, V> forked() {
    return linear ? new IntMap<V>(neg, pos, false) : this;
  }

  @Override
  public IMap<Long, V> linear() {
    return linear ? this : new IntMap<V>(neg, pos, true);
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
