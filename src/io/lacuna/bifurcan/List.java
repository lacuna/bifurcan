package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.ListNodes.Leaf;
import io.lacuna.bifurcan.nodes.ListNodes.Node;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class List<V> implements IList<V>, Cloneable {

  boolean linear;
  public Node<V> root;
  public byte prefixLen;
  public Object[] prefix;
  public byte suffixLen;
  public Object[] suffix;

  private final Object editor = new Object();

  public List() {
    linear = false;
    root = Node.EMPTY;
    prefixLen = 0;
    prefix = null;
    suffixLen = 0;
    suffix = null;
  }

  List(boolean linear, Node root, byte prefixLen, Object[] prefix, byte suffixLen, Object[] suffix) {
    this.linear = linear;
    this.root = root;
    this.prefixLen = prefixLen;
    this.suffixLen = suffixLen;
    this.prefix = prefix;
    this.suffix = suffix;
  }

  public static <V> List<V> from(java.util.List<V> list) {
    return list.stream().collect(Lists.collector());
  }

  @Override
  public V nth(long idx) {
    int rootSize = root.size();
    if (idx < 0 || idx >= (rootSize + prefixLen + suffixLen)) {
      throw new IndexOutOfBoundsException();
    }

    int i = (int) idx;

    // look in the prefix
    if (i < prefixLen) {
      return (V) prefix[prefix.length + i - prefixLen];

      // look in the tree
    } else if (i - prefixLen < rootSize) {
      return (V) root.nth(i - prefixLen);

      // look in the suffix
    } else {
      return (V) suffix[i - (rootSize + prefixLen)];
    }
  }

  @Override
  public long size() {
    return root.size() + prefixLen + suffixLen;
  }

  @Override
  public List<V> addLast(V value) {
    return (linear ? this : clone()).pushLast(value);
  }

  @Override
  public List<V> addFirst(V value) {
    return (linear ? this : clone()).pushFirst(value);
  }

  @Override
  public List<V> removeLast() {
    return (linear ? this : clone()).popLast();
  }

  @Override
  public List<V> removeFirst() {
    return (linear ? this : clone()).popFirst();
  }

  @Override
  public List<V> set(long idx, V value) {
    int size = (int) size();
    if (idx < 0 || idx > size) {
      throw new IndexOutOfBoundsException();
    }

    if (idx == size) {
      return addLast(value);
    } else {
      return (linear ? this : clone()).overwrite((int) idx, value);
    }
  }

  @Override
  public Iterator<V> iterator() {

    long size = size();

    if (size == 0) {
      return Iterators.EMPTY;
    }

    final Object[] initChunk;
    final int initOffset, initLimit;
    if (prefixLen > 0) {
      initChunk = prefix;
      initOffset = pIdx(0);
      initLimit = prefix.length;
    } else if (root.size() > 0) {
      initChunk = root.arrayFor(0);
      initOffset = 0;
      initLimit = initChunk.length;
    } else {
      initChunk = suffix;
      initOffset = 0;
      initLimit = suffixLen;
    }

    return new Iterator<V>() {

      long idx = 0;

      Object[] chunk = initChunk;
      int offset = initOffset;
      int limit = initLimit;
      int chunkSize = limit - offset;

      @Override
      public boolean hasNext() {
        return idx < size;
      }

      @Override
      public V next() {
        V val = (V) chunk[offset++];

        if (offset == limit) {
          idx += chunkSize;
          if (idx < size) {
            if (idx == prefixLen + root.size()) {
              chunk = suffix;
              limit = suffixLen;
            } else {
              chunk = root.arrayFor(idx - prefixLen);
              limit = chunk.length;
            }
            offset = 0;
            chunkSize = limit;
          }
        }

        return val;
      }
    };
  }

  @Override
  public List<V> slice(long start, long end) {
    if (start < 0 || end > size()) {
      throw new IndexOutOfBoundsException();
    }

    int s = (int) start;
    int e = (int) end;

    byte pLen = (byte) Math.max(0, prefixLen - s);
    Object[] pre = pLen == 0 ? null : new Object[32];
    if (pre != null) {
      arraycopy(prefix, pIdx(s), pre, 32 - pLen, pLen);
    }

    byte sLen = (byte) Math.max(0, suffixLen - ((int) size() - e));
    Object[] suf = sLen == 0 ? null : new Object[32];
    if (suf != null) {
      arraycopy(suffix, 0, suf, 0, sLen);
    }

    return new List<V>(linear, root.slice(editor, s - prefixLen, Math.min(root.size(), e - prefixLen)), pLen, pre, sLen, suf);
  }

  @Override
  public IList<V> concat(IList<V> l) {
    if (l instanceof List) {
      List<V> b = (List<V>) l;
      Node r = root;
      Object editor = new Object();

      // append our own suffix
      if (suffix != null && suffixLen > 0) {
        r = r.addLast(editor, suffixLeaf(), suffixLen);
      }

      // append their prefix
      if (b.prefix != null && b.prefixLen > 0) {
        r = r.addLast(editor, b.prefixLeaf(), b.prefixLen);
      }

      if (r.size() == 0) {
        r = b.root;
      } else if (b.root.size() > 0) {
        r = r.concat(editor, b.root);
      }

      return new List<V>(linear, r,
          prefixLen, prefix == null ? null : prefix.clone(),
          b.suffixLen, b.suffix == null ? null : b.suffix.clone());

    } else {
      return Lists.concat(this, l);
    }
  }

  @Override
  public List<V> forked() {
    return linear ? new List(false, root, prefixLen, prefix, suffixLen, suffix) : this;
  }

  @Override
  public List<V> linear() {
    return linear ? this : new List(true, root, prefixLen, prefix, suffixLen, suffix);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IList) {
      return Lists.equals(this, (IList<V>) obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return Lists.toString(this);
  }

  @Override
  public List<V> clone() {
    return new List<V>(linear, root,
        prefixLen, prefix == null ? null : prefix.clone(),
        suffixLen, suffix == null ? null : suffix.clone());
  }

  ///

  private Leaf<V> suffixLeaf() {
    Object[] suf = new Object[suffixLen];
    arraycopy(suffix, 0, suf, 0, suf.length);
    return new Leaf<V>(editor, suf);
  }

  private Leaf<V> prefixLeaf() {
    Object[] pre = new Object[prefixLen];
    arraycopy(prefix, pIdx(0), pre, 0, pre.length);
    return new Leaf<V>(editor, pre);
  }

  private int pIdx(int idx) {
    return prefix.length - prefixLen + idx;
  }

  List<V> overwrite(int idx, V value) {
    int rootSize = root.size();

    // overwrite prefix
    if (idx < prefixLen) {
      prefix[prefix.length - prefixLen + idx] = value;

      // overwrite tree
    } else if (idx < (prefixLen + rootSize)) {
      root = (Node<V>) root.set(editor, idx - prefixLen, value);

      // overwrite suffix
    } else {
      suffix[idx - (prefixLen + rootSize)] = value;
    }

    return this;
  }

  List<V> pushFirst(V value) {

    // create a prefix
    if (prefix == null) {
      prefix = new Object[32];
      prefix[31] = value;
      prefixLen = 1;

      // prefix overflow
    } else if (prefixLen == prefix.length - 1) {
      prefix[0] = value;
      root = root.addFirst(editor, new Leaf(editor, prefix), prefix.length);
      prefix = null;
      prefixLen = 0;

      // prepend to prefix
    } else {
      prefix[pIdx(-1)] = value;
      prefixLen++;
    }

    return this;
  }

  List<V> pushLast(V value) {

    // create a suffix
    if (suffix == null) {
      suffix = new Object[32];
      suffix[0] = value;
      suffixLen = 1;

      // suffix overflow
    } else if (suffixLen == suffix.length - 1) {
      suffix[suffixLen] = value;
      root = root.addLast(editor, new Leaf(editor, suffix), suffix.length);
      suffix = null;
      suffixLen = 0;

      // append to suffix
    } else {
      suffix[suffixLen++] = value;
    }

    return this;
  }

  List<V> popFirst() {

    // pull from the front of the suffix
    if (root.size() == 0 && prefixLen == 0) {
      if (suffixLen > 0) {
        arraycopy(suffix, 1, suffix, 0, --suffixLen);
        suffix[suffixLen] = null;
      }

      // prefix underflow
    } else if (prefixLen == 0) {
      Leaf leaf = root.first();
      if (leaf != null) {
        prefix = leaf.elements.clone();
        prefixLen = (byte) (leaf.size() - 1);
        root = root.removeFirst(this);
        prefix[pIdx(-1)] = null;
      }

      // truncate prefix
    } else {
      prefixLen--;
      prefix[pIdx(-1)] = null;
    }

    return this;
  }

  List<V> popLast() {

    // pull from the back of the prefix
    if (root.size() == 0 && suffixLen == 0) {
      if (prefixLen > 0) {
        prefixLen--;
        arraycopy(prefix, pIdx(-1), prefix, pIdx(0), prefixLen);
        prefix[pIdx(-1)] = null;
      }

      // suffix underflow
    } else if (suffixLen == 0) {
      Leaf leaf = root.last();
      if (leaf != null) {
        suffix = leaf.elements.clone();
        suffixLen = (byte) (leaf.size() - 1);
        root = root.removeLast(this);
        suffix[suffixLen] = null;
      }

      // truncate suffix
    } else {
      suffix[suffixLen--] = null;
    }

    return this;
  }
}
