package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.utils.IteratorStack;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static io.lacuna.bifurcan.utils.Bits.bitOffset;
import static io.lacuna.bifurcan.utils.Bits.highestBit;

/**
 * Created by zach on 3/3/17.
 */
public class IntMapNodes {

  public static int offset(long a, long b) {
    return bitOffset(highestBit(a ^ b, 1)) & ~0x3;
  }

  interface INode<V> {

    long size();

    Iterator<V> iterator();

    INode<V> merge(Object editor, INode<V> node, IMap.ValueMerger<V> f);

    INode<V> difference(Object editor, INode<V> node);

    INode<V> intersection(Object editor, INode<V> node);

    INode<V> range(Object editor, long min, long max);

    INode<V> put(Object editor, long k, V v, IMap.ValueMerger<V> mergeFn);

    INode<V> remove(Object editor, long k);

    V get(long k, V defaultVal);
  }

  // 2-way top-level branch
  public static class BinaryBranch<V> implements INode<V> {

    public Object editor;
    public INode<V> a, b;

    public BinaryBranch(INode<V> a, INode<V> b) {
      this.a = a;
      this.b = b;
    }

    public long size() {
      return a.size() + b.size();
    }

    public Iterator<V> iterator() {
      return new IteratorStack<V>(a.iterator(), b.iterator());
    }

    public INode<V> range(Object editor, long min, long max) {
      if (max < 0) {
        return a.range(editor, min, max);
      } else if (min >= 0) {
        return b.range(editor, min, max);
      } else {
        INode<V> aPrime = a.range(editor, min, max);
        INode<V> bPrime = b.range(editor, min, max);

        if (aPrime == null && bPrime == null) {
          return Empty.EMPTY;
        } else if (aPrime == null) {
          return bPrime;
        } else if (bPrime == null) {
          return aPrime;
        } else {
          return new BinaryBranch<>(aPrime, bPrime);
        }
      }
    }

    public INode<V> merge(Object editor, INode<V> node, IMap.ValueMerger<V> mergeFn) {
      if (node instanceof BinaryBranch) {
        BinaryBranch<V> bin = (BinaryBranch<V>) node;
        return new BinaryBranch<>(a.merge(editor, bin.a, mergeFn), b.merge(editor, bin.b, mergeFn));
      } else if (node instanceof Branch) {
        Branch<V> branch = (Branch<V>) node;
        return branch.prefix < 0
            ? new BinaryBranch<>(a.merge(editor, node, mergeFn), b)
            : new BinaryBranch<>(a, b.merge(editor, node, mergeFn));
      } else {
        return node.merge(editor, this, (x, y) -> mergeFn.merge(y, x));
      }
    }

    @Override
    public INode<V> difference(Object editor, INode<V> node) {
      return null;
    }

    @Override
    public INode<V> intersection(Object editor, INode<V> node) {
      return null;
    }

    public INode<V> put(Object editor, long k, V v, IMap.ValueMerger<V> mergeFn) {
      if (k < 0) {
        INode<V> aPrime = a.put(editor, k, v, mergeFn);
        return a == aPrime ? this : new BinaryBranch<>(aPrime, b);
      } else {
        INode<V> bPrime = b.put(editor, k, v, mergeFn);
        return b == bPrime ? this : new BinaryBranch<>(a, bPrime);
      }
    }

    public INode<V> remove(Object editor, long k) {
      if (k < 0) {
        INode<V> aPrime = a.remove(editor, k);
        return aPrime == null
            ? b
            : (a == aPrime)
            ? this
            : new BinaryBranch<>(aPrime, b);
      } else {
        INode<V> bPrime = b.remove(editor, k);
        return bPrime == null
            ? a
            : (b == bPrime)
            ? this
            : new BinaryBranch<>(a, bPrime);
      }
    }

    public V get(long k, V defaultVal) {
      return k < 0 ? a.get(k, defaultVal) : b.get(k, defaultVal);
    }
  }

  // 16-way branch node

  public static class Branch<V> implements INode<V> {

    public Object editor;
    public final long prefix, mask;
    public final int offset;
    long size;
    public final INode[] children;

    public Branch(Object editor, long prefix, int offset, long size, INode[] children) {
      this.editor = editor;
      this.prefix = prefix;
      this.offset = offset;
      this.mask = 0xfL << offset;
      this.size = size;
      this.children = children;
    }

    public Branch(Object editor, long prefix, int offset, INode[] children) {
      this.editor = editor;
      this.prefix = prefix;
      this.offset = offset;
      this.mask = 0xfL << offset;
      this.size = -1;
      this.children = children;
    }

    public int indexOf(long key) {
      return (int) ((key & mask) >>> offset);
    }

    private INode[] arraycopy() {
      INode[] copy = new INode[16];
      System.arraycopy(children, 0, copy, 0, 16);
      return copy;
    }

    private static boolean overlap(long min0, long max0, long min1, long max1) {
      return (max1 - min0) >= 0 && (max0 - min1) >= 0;
    }

    public INode<V> range(Object editor, long min, long max) {
      if (offset < 60) {
        long nodeMask = ((1L << (offset + 4)) - 1);
        long nodeMin = prefix & ~nodeMask;
        long nodeMax = prefix | nodeMask;
        if (!overlap(min, max, nodeMin, nodeMax)) {
          return null;
        }
      }

      INode[] children = new INode[16];
      long lowerBits = (1L << offset) - 1;
      for (long i = 0; i < 16; i++) {
        INode c = this.children[(int) i];
        if (c != null) {
          long childMin = ((prefix & ~mask) | (i << offset)) & ~lowerBits;
          long childMax = childMin | lowerBits;
          if (overlap(min, max, childMin, childMax)) {
            children[(int) i] = c.range(editor, min, max);
          }
        }
      }
      return new Branch<>(editor, prefix, offset, children);
    }

    public Iterator<V> iterator() {
      return null;
    }

    public V get(long k, V defaultVal) {
      INode<V> n = children[indexOf(k)];
      return n == null ? defaultVal : n.get(k, defaultVal);
    }

    public long size() {
      int size = 0;
      for (int i = 0; i < 16; i++) {
        INode n = children[i];
        if (n != null) size += n.size();
      }
      this.size = size;
      return size;
    }

    public INode<V> merge(Object editor, INode<V> node, IMap.ValueMerger<V> mergeFn) {
      if (node instanceof Branch) {
        Branch<V> branch = (Branch) node;
        int offsetPrime = offset(prefix, branch.prefix);

        if (branch.prefix < 0 && this.prefix >= 0) {
          return new BinaryBranch<>(branch, this);
        } else if (branch.prefix >= 0 && this.prefix < 0) {
          return new BinaryBranch<>(this, branch);
        }

        if (offsetPrime > offset && offsetPrime > branch.offset) {
          return new Branch<V>(editor, prefix, offset(prefix, branch.prefix), new INode[16])
              .merge(editor, this, mergeFn)
              .merge(editor, node, mergeFn);
        }

        // we contain the other node
        if (offset > branch.offset) {
          int idx = indexOf(branch.prefix);
          INode[] children = arraycopy();
          INode n = children[idx];
          children[idx] = n != null ? n.merge(editor, node, mergeFn) : node;
          return new Branch<>(editor, prefix, offset, children);

        }

        if (offset < branch.offset) {
          return branch.merge(editor, this, (x, y) -> mergeFn.merge(y, x));
        }

        INode[] children = new INode[16];
        INode[] branchChildren = branch.children;
        int offset = this.offset;

        for (int i = 0; i < 16; i++) {
          INode n = this.children[i];
          INode nPrime = branchChildren[i];
          if (n == null) {
            children[i] = nPrime;
          } else if (nPrime == null) {
            children[i] = n;
          } else {
            children[i] = n.merge(editor, nPrime, mergeFn);
          }
        }
        return new Branch<>(editor, prefix, offset, children);

      } else {
        return node.merge(editor, this, (x, y) -> mergeFn.merge(y, x));
      }
    }

    @Override
    public INode<V> difference(Object editor, INode<V> node) {
      return null;
    }

    @Override
    public INode<V> intersection(Object editor, INode<V> node) {
      return null;
    }

    public INode<V> put(Object editor, long k, V v, IMap.ValueMerger<V> mergeFn) {
      int offsetPrime = offset(k, prefix);

      // need a new branch above us both
      if (prefix < 0 && k >= 0) {
        return new BinaryBranch<>(this, new Leaf<>(k, v));
      } else if (k < 0 && prefix >= 0) {
        return new BinaryBranch<>(new Leaf<>(k, v), this);
      } else if (offsetPrime > this.offset) {
        return new Branch<V>(editor, k, offsetPrime, new INode[16])
            .merge(editor, this, null)
            .put(editor, k, v, mergeFn);

        // somewhere at or below our level
      } else {
        int idx = indexOf(k);
        INode<V> n = children[idx];
        if (n == null) {
          if (editor == this.editor) {
            children[idx] = new Leaf<V>(k, v);
            size = -1;
            return this;
          } else {
            INode[] children = arraycopy();
            children[idx] = new Leaf<V>(k, v);
            return new Branch<V>(editor, prefix, offset, size, children);
          }
        } else {
          INode<V> nPrime = n.put(editor, k, v, mergeFn);
          if (nPrime == n) {
            size = -1;
            return this;
          } else {
            INode[] children = arraycopy();
            children[idx] = nPrime;
            return new Branch<>(editor, prefix, offset, size, children);
          }
        }
      }
    }

    public INode remove(Object editor, long k) {
      int idx = indexOf(k);
      INode n = children[idx];
      if (n == null) {
        return this;
      } else {
        INode nPrime = n.remove(editor, k);
        if (nPrime == n) {
          size = -1;
          return this;
        } else {
          INode[] children = arraycopy();
          children[idx] = nPrime;
          for (int i = 0; i < 16; i++) {
            if (children[i] != null) {
              return new Branch(editor, prefix, offset, children);
            }
          }
          return null;
        }
      }
    }
  }

  // leaf node
  public static class Leaf<V> implements INode<V> {
    public final long key;
    public final V value;

    public Leaf(long key, V value) {
      this.key = key;
      this.value = value;
    }

    public Iterator<V> iterator() {
      return new Iterator<V>() {

        boolean iterated = false;

        public boolean hasNext() {
          return !iterated;
        }

        public V next() {
          if (iterated) {
            throw new NoSuchElementException();
          } else {
            iterated = true;
            return value;
          }
        }
      };
    }

    public INode<V> range(Object editor, long min, long max) {
      return (min <= key && key <= max) ? this : null;
    }

    public long size() {
      return 1;
    }

    public INode<V> merge(Object editor, INode<V> node, IMap.ValueMerger<V> mergeFn) {
      return node.put(editor, key, value, (x, y) -> mergeFn.merge(y, x));
    }

    @Override
    public INode<V> difference(Object editor, INode<V> node) {
      return null;
    }

    @Override
    public INode<V> intersection(Object editor, INode<V> node) {
      return null;
    }

    public INode<V> put(Object editor, long k, V v, IMap.ValueMerger<V> mergeFn) {
      if (k == key) {
        v = mergeFn.merge(value, v);
        return new Leaf<>(k, v);
      } else if (key < 0 && k >= 0) {
        return new BinaryBranch<>(this, new Leaf<>(k, v));
      } else if (k < 0 && key >= 0) {
        return new BinaryBranch<>(new Leaf<>(k, v), this);
      } else {
        return new Branch<V>(editor, k, offset(k, key), new INode[16])
            .put(editor, key, value, mergeFn)
            .put(editor, k, v, mergeFn);
      }
    }

    public INode<V> remove(Object editor, long k) {
      if (key == k) {
        return null;
      } else {
        return this;
      }
    }

    public V get(long k, V defaultVal) {
      return k == key ? value : defaultVal;
    }
  }

  // empty node
  public static class Empty<V> implements INode<V> {

    public static Empty EMPTY = new Empty();

    Empty() {
    }

    @Override
    public INode<V> range(Object editor, long min, long max) {
      return this;
    }

    @Override
    public INode<V> put(Object editor, long k, V v, IMap.ValueMerger<V> mergeFn) {
      return new Leaf<>(k, v);
    }

    @Override
    public INode<V> remove(Object editor, long k) {
      return this;
    }

    @Override
    public V get(long k, V defaultVal) {
      return defaultVal;
    }

    @Override
    public Iterator<V> iterator() {
      return new Iterator<V>() {

        public boolean hasNext() {
          return false;
        }

        public V next() {
          throw new NoSuchElementException();
        }
      };
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public INode<V> merge(Object editor, INode<V> node, IMap.ValueMerger<V> mergeFn) {
      return node;
    }

    @Override
    public INode<V> difference(Object editor, INode<V> node) {
      return this;
    }

    @Override
    public INode<V> intersection(Object editor, INode<V> node) {
      return this;
    }
  }

}
