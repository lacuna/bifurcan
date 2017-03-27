package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.ListNodes.Node;

import java.util.Collection;
import java.util.Iterator;

import static java.lang.System.arraycopy;

/**
 * An implementation of an immutable list which allows for elements to be added and removed from both ends of the
 * collection, as well as random-access reads and writes.  Due to its
 * <a href=https://infoscience.epfl.ch/record/169879/files/RMTrees.pdf>relaxed radix structure</a>, {@code slice()},
 * {@code concat()}, and {@code split()} are near-constant time.
 *
 * @author ztellman
 */
public class List<V> implements IList<V>, Cloneable {

  private Node root;
  private byte prefixLen, suffixLen;
  private Object[] prefix, suffix;

  private final boolean linear;
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

  public static <V> List<V> from(IList<V> list) {
    if (list instanceof List) {
      return ((List<V>) list).forked();
    } else {
      return from(list.iterator());
    }
  }

  public static <V> List<V> from(Iterable<V> iterable) {
    return from(iterable.iterator());
  }

  public static <V> List<V> from(Iterator<V> iterator) {
    List<V> list = new List<V>().linear();
    iterator.forEachRemaining(list::addLast);
    return list.forked();
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
  public boolean isLinear() {
    return linear;
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

    final Object[] initChunk;
    final int initOffset, initLimit;
    final int size = (int) size();
    final int rootSize = root.size();

    if (prefixLen > 0) {
      initChunk = prefix;
      initOffset = pIdx(0);
      initLimit = prefix.length;
    } else if (rootSize > 0) {
      initChunk = root.arrayFor(0);
      initOffset = 0;
      initLimit = initChunk.length;
    } else {
      initChunk = suffix;
      initOffset = 0;
      initLimit = suffixLen;
    }

    return new Iterator<V>() {

      int idx = 0;

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
            if (idx == prefixLen + rootSize) {
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

    int pStart = Math.max(0, prefixLen - s);
    int pEnd = Math.min(prefixLen, e);
    int pLen = pEnd - pStart;
    Object[] pre = pLen == 0 ? null : new Object[32];
    if (pre != null) {
      arraycopy(prefix, pIdx(pStart), pre, 32 - pLen, pLen);
    }

    int sStart = Math.max(0, s - (prefixLen + root.size()));
    int sEnd = Math.max(0, e - (prefixLen + root.size()));
    int sLen = sEnd - sStart;
    Object[] suf = sLen == 0 ? null : new Object[32];
    if (suf != null) {
      arraycopy(suffix, sStart, suf, 0, sLen);
    }

    return new List<V>(linear,
        root.slice(editor, Math.max(0, Math.min(root.size(), s - prefixLen)), Math.max(0, Math.min(root.size(), e - prefixLen))),
        (byte) pLen, pre, (byte) sLen, suf);
  }

  @Override
  public IList<V> concat(IList<V> l) {
    if (l instanceof List) {
      List<V> b = (List<V>) l;
      Node r = root;
      Object editor = new Object();

      // append our own suffix
      if (suffix != null && suffixLen > 0) {
        r = r.addLast(editor, suffixArray(), suffixLen);
      }

      // append their prefix
      if (b.prefix != null && b.prefixLen > 0) {
        r = r.addLast(editor, b.prefixArray(), b.prefixLen);
      }

      if (b.root.size() > 0) {
        r = r.concat(editor, b.root);
      }

      return new List<V>(linear, r,
          prefixLen, prefixLen > 0 ? prefix.clone() : null,
          b.suffixLen, b.suffixLen > 0 ? b.suffix.clone() : null);

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

  private Object[] suffixArray() {
    Object[] suf = new Object[suffixLen];
    arraycopy(suffix, 0, suf, 0, suf.length);
    return suf;
  }

  private Object prefixArray() {
    Object[] pre = new Object[prefixLen];
    arraycopy(prefix, pIdx(0), pre, 0, pre.length);
    return pre;
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
      root = root.set(editor, idx - prefixLen, value);

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
      root = root.addFirst(editor, prefix, prefix.length);
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
      root = root.addLast(editor, suffix, suffix.length);
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
      Object[] chunk = root.first();
      if (chunk != null) {
        prefix = chunk.clone();
        prefixLen = (byte) (chunk.length - 1);
        root = root.removeFirst(editor);
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
      Object[] chunk = root.last();
      if (chunk != null) {
        suffix = chunk.clone();
        suffixLen = (byte) (chunk.length - 1);
        root = root.removeLast(editor);
        suffix[suffixLen] = null;
      }

      // truncate suffix
    } else {
      suffix[suffixLen--] = null;
    }

    return this;
  }
}
