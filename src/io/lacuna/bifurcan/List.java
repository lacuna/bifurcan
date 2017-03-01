package io.lacuna.bifurcan;

import io.lacuna.bifurcan.nodes.RRNode;
import io.lacuna.bifurcan.nodes.RRNode.Leaf;

import java.util.Iterator;

import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class List<V> implements IList<V> {

  boolean linear;
  public RRNode root;
  public byte prefixLen;
  public Object[] prefix;
  public byte suffixLen;
  public Object[] suffix;

  public List() {
    linear = false;
    root = RRNode.EMPTY;
    prefixLen = 0;
    prefix = null;
    suffixLen = 0;
    suffix = null;
  }

  List(boolean linear, RRNode root, byte prefixLen, Object[] prefix, byte suffixLen, Object[] suffix) {
    this.linear = linear;
    this.root = root;
    this.prefixLen = prefixLen;
    this.suffixLen = suffixLen;
    this.prefix = prefix;
    this.suffix = suffix;
  }

  @Override
  public V nth(long idx) {
    int rootSize = root.size();
    if (idx < 0 || idx >= (rootSize + prefixLen + suffixLen)) {
      throw new IndexOutOfBoundsException(idx + " " + size());
    }

    int i = (int) idx;
    if (i < prefixLen) {
      return (V) prefix[prefix.length + i - prefixLen];
    } else if (i - prefixLen < rootSize) {
      return (V) root.nth(i - prefixLen);
    } else {
      return (V) suffix[i - (rootSize + prefixLen)];
    }
  }

  @Override
  public long size() {
    return root.size() + prefixLen + suffixLen;
  }

  @Override
  public IList<V> addLast(V value) {
    return (linear ? this : clone()).pushLast(value);
  }

  @Override
  public IList<V> addFirst(V value) {
    return (linear ? this : clone()).pushFirst(value);
  }

  @Override
  public IList<V> removeLast() {
    return (linear ? this : clone()).popLast();
  }

  @Override
  public IList<V> removeFirst() {
    return (linear ? this : clone()).popFirst();
  }

  @Override
  public Iterator<V> iterator() {
    return new Iterator<V>() {
      int prefixIdx = 0, suffixIdx = 0, leafIdx = 0, rootIdx = 0, rootLen = root.size();
      Iterator<Leaf> it = root.leafs();
      Leaf leaf = it.hasNext() ? it.next() : null;

      @Override
      public boolean hasNext() {
        return prefixIdx < prefixLen
            || rootIdx < rootLen
            || suffixIdx < suffixLen;
      }

      @Override
      public V next() {
        if (prefixIdx < prefixLen) {
          return (V) prefix[(prefix.length - prefixLen) + prefixIdx++];
        } else if (rootIdx < rootLen) {
          rootIdx++;
          if (leafIdx < leaf.size) {
            return (V) leaf.elements[leafIdx++];
          } else if (it.hasNext()) {
            leaf = it.next();
            leafIdx = 1;
            return (V) leaf.elements[0];
          } else {
            leaf = null;
          }
        }

        return (V) suffix[suffixIdx++];
      }
    };
  }

  @Override
  public IList<IList<V>> split(int parts) {
    return null;
  }

  @Override
  public IList<V> subList(long start, long end) {
    return null;
  }

  @Override
  public IList<V> concat(IList<V> l) {
    return null;
  }

  @Override
  public IList<V> forked() {
    return linear ? new List(false, root, prefixLen, prefix, suffixLen, suffix) : this;
  }

  @Override
  public IList<V> linear() {
    return linear ? this : new List(true, root, prefixLen, prefix, suffixLen, suffix);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof List) {
      return Lists.equals(this, (List) obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return Lists.toString(this);
  }

  ///

  @Override
  protected List<V> clone() {
    return new List<V>(linear, root, prefixLen, prefix == null ? null : prefix.clone(), suffixLen, suffix == null ? null : suffix.clone());
  }

  List<V> pushFirst(V value) {
    if (prefixLen == 31) {
      prefix[0] = value;
      root = root.addFirst(this, new Leaf(prefix), 32);
      prefix = null;
      prefixLen = 0;
    } else if (prefix == null) {
      prefix = new Object[32];
      prefix[31] = value;
      prefixLen = 1;
    } else {
      prefix[31 - prefixLen++] = value;
    }

    return this;
  }

  List<V> pushLast(V value) {
    if (suffixLen == 31) {
      suffix[31] = value;
      root = root.addLast(this, new Leaf(suffix), 32);
      suffix = null;
      suffixLen = 0;
    } else if (suffix == null) {
      suffix = new Object[32];
      suffix[0] = value;
      suffixLen = 1;
    } else {
      suffix[suffixLen++] = value;
    }

    return this;
  }

  List<V> popFirst() {
    if (root.size() == 0 && prefixLen == 0) {
      if (suffixLen > 0) {
        arraycopy(suffix, 1, suffix, 0, --suffixLen);
        suffix[suffixLen] = null;
      }
    } else if (prefixLen == 0) {
      Leaf leaf = root.first();
      if (leaf != null) {
        prefix = leaf.elements.clone();
        prefixLen = (byte) (leaf.size - 1);
        root = root.removeFirst(this);
        prefix[31 - prefixLen] = null;
      }
    } else {
      prefix[31 - prefixLen--] = null;
    }

    return this;
  }

  List<V> popLast() {
    if (root.size() == 0 && suffixLen == 0) {
      if (prefixLen > 0) {
        prefixLen--;
        arraycopy(prefix, 31 - prefixLen, prefix, 32 - prefixLen, prefixLen);
        prefix[31 - prefixLen] = null;
      }
    } else if (suffixLen == 0) {
      Leaf leaf = root.last();
      if (leaf != null) {
        suffix = leaf.elements.clone();
        suffixLen = (byte) (leaf.size - 1);
        root = root.removeLast(this);
        suffix[suffixLen] = null;
      }
    } else {
      suffix[suffixLen--] = null;
    }

    return this;
  }
}
