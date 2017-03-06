package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.IMap.IEntry;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.IteratorStack;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static io.lacuna.bifurcan.utils.Bits.bitOffset;
import static io.lacuna.bifurcan.utils.Bits.highestBit;

/**
 * @author ztellman
 */
public class IntMapNodes {

  public static int offset(long a, long b) {
    return bitOffset(highestBit(a ^ b, 1)) & ~0x3;
  }

  private static boolean overlap(long min0, long max0, long min1, long max1) {
    return (max1 - min0) >= 0 && (max0 - min1) >= 0;
  }

  public interface INode<V> {

    int size();

    IEntry<Long, V> nth(int idx);

    Iterator<IEntry<Long, V>> iterator();

    INode<V> merge(Object editor, INode<V> node, IMap.ValueMerger<V> f);

    INode<V> difference(Object editor, INode<V> node);

    INode<V> intersection(Object editor, INode<V> node);

    INode<V> range(Object editor, long min, long max);

    INode<V> put(Object editor, long k, V v, IMap.ValueMerger<V> mergeFn);

    INode<V> remove(Object editor, long k);

    Object get(long k, Object defaultVal);
  }

  // 2-way top-level branch
  public static class BinaryBranch<V> implements INode<V> {

    public INode<V> a, b;

    public BinaryBranch(INode<V> a, INode<V> b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public IEntry<Long, V> nth(int idx) {
      return idx < a.size() ? a.nth(idx) : b.nth(idx - a.size());
    }

    public int size() {
      return a.size() + b.size();
    }

    public Iterator<IEntry<Long, V>> iterator() {
      return new IteratorStack<>(a.iterator(), b.iterator());
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

    public Object get(long k, Object defaultVal) {
      return k < 0 ? a.get(k, defaultVal) : b.get(k, defaultVal);
    }
  }

  // 16-way branch node

  public static class Branch<V> implements INode<V> {

    public Object editor;
    public final long prefix, mask;
    public final int offset;
    int size;
    public final INode[] children;

    public Branch(Object editor, long prefix, int offset, int size, INode[] children) {
      this.editor = editor;
      this.prefix = prefix;
      this.offset = offset;
      this.mask = 0xfL << offset;
      this.size = size;
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

    @Override
    public IEntry<Long, V> nth(int idx) {
      for (int i = 0; i < 16; i++) {
        INode<V> c = this.children[i];
        if (c != null) {
          if (idx < c.size()) {
            return c.nth(idx);
          } else {
            idx -= c.size();
          }
        }
      }

      throw new IndexOutOfBoundsException();
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
      int size = 0;
      for (long i = 0; i < 16; i++) {
        INode c = this.children[(int) i];
        if (c != null) {
          long childMin = ((prefix & ~mask) | (i << offset)) & ~lowerBits;
          long childMax = childMin | lowerBits;
          if (overlap(min, max, childMin, childMax)) {
            INode<V> cPrime = c.range(editor, min, max);
            size += cPrime.size();
            children[(int) i] = cPrime;
          }
        }
      }
      return new Branch<>(editor, prefix, offset, size, children);
    }

    public Iterator<IEntry<Long, V>> iterator() {
      IteratorStack<IEntry<Long, V>> stack = new IteratorStack<>();
      for (int i = 0; i < children.length; i++) {
        INode<V> n = children[i];
        if (n != null) {
          stack.addLast(n.iterator());
        }
      }
      return stack;
    }

    public Object get(long k, Object defaultVal) {
      INode<V> n = children[indexOf(k)];
      return n == null ? defaultVal : n.get(k, defaultVal);
    }

    public int size() {
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
          return new Branch<V>(editor, prefix, offset(prefix, branch.prefix), 0, new INode[16])
              .merge(editor, this, mergeFn)
              .merge(editor, node, mergeFn);
        }

        // we contain the other node
        if (offset > branch.offset) {
          int idx = indexOf(branch.prefix);
          INode[] children = arraycopy();
          INode n = children[idx];
          int prevSize = n == null ? 0 : n.size();
          children[idx] = n != null ? n.merge(editor, node, mergeFn) : node;
          return new Branch<>(editor, prefix, offset, size + (children[idx].size() - prevSize), children);
        }

        if (offset < branch.offset) {
          return branch.merge(editor, this, (x, y) -> mergeFn.merge(y, x));
        }

        INode[] children = new INode[16];
        INode[] branchChildren = branch.children;
        int offset = this.offset;
        int size = 0;
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
          size += children[i].size();
        }
        return new Branch<>(editor, prefix, offset, size, children);

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
        return new Branch<V>(editor, k, offsetPrime, 0, new INode[16])
            .merge(editor, this, null)
            .put(editor, k, v, mergeFn);

        // somewhere at or below our level
      } else {
        int idx = indexOf(k);
        INode<V> n = children[idx];
        if (n == null) {
          if (editor == this.editor) {
            children[idx] = new Leaf<V>(k, v);
            size++;
            return this;
          } else {
            INode[] children = arraycopy();
            children[idx] = new Leaf<V>(k, v);
            return new Branch<V>(editor, prefix, offset, size + 1, children);
          }
        } else {
          int prevSize = n.size();
          INode<V> nPrime = n.put(editor, k, v, mergeFn);
          if (nPrime == n) {
            size += nPrime.size() - prevSize;
            return this;
          } else {
            INode[] children = arraycopy();
            children[idx] = nPrime;
            return new Branch<>(editor, prefix, offset, size + (nPrime.size() - prevSize), children);
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
        int prevSize = n.size();
        INode nPrime = n.remove(editor, k);
        if (nPrime == n) {
          size += n.size() - prevSize;
          return this;
        } else {
          INode[] children = arraycopy();
          children[idx] = nPrime;
          for (int i = 0; i < 16; i++) {
            if (children[i] != null) {
              return new Branch(editor, prefix, offset, size + (nPrime.size() - prevSize), children);
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

    public Iterator<IEntry<Long, V>> iterator() {
      return new Iterator<IEntry<Long, V>>() {

        boolean iterated = false;

        public boolean hasNext() {
          return !iterated;
        }

        public IEntry<Long, V> next() {
          if (iterated) {
            throw new NoSuchElementException();
          } else {
            iterated = true;
            return new Maps.Entry<>(key, value);
          }
        }
      };
    }

    @Override
    public IEntry<Long, V> nth(int idx) {
      if (idx == 0) {
        return new Maps.Entry<>(key, value);
      }
      throw new IndexOutOfBoundsException();
    }

    public INode<V> range(Object editor, long min, long max) {
      return (min <= key && key <= max) ? this : null;
    }

    public int size() {
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
        return new Branch<V>(editor, k, offset(k, key), 0, new INode[16])
            .put(editor, key, value, mergeFn)
            .put(editor, k, v, mergeFn);
      }
    }

    public INode<V> remove(Object editor, long k) {
      if (key == k) {
        return Empty.EMPTY;
      } else {
        return this;
      }
    }

    public Object get(long k, Object defaultVal) {
      return k == key ? value : defaultVal;
    }
  }

  // empty node
  public static class Empty<V> implements INode<V> {

    public static Empty EMPTY = new Empty();

    Empty() {
    }

    @Override
    public IEntry<Long, V> nth(int idx) {
      throw new IndexOutOfBoundsException();
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
    public Object get(long k, Object defaultVal) {
      return defaultVal;
    }

    @Override
    public Iterator<IEntry<Long, V>> iterator() {
      return new Iterator<IEntry<Long, V>>() {

        public boolean hasNext() {
          return false;
        }

        public IEntry<Long, V> next() {
          throw new NoSuchElementException();
        }
      };
    }

    @Override
    public int size() {
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
