package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

import static io.lacuna.bifurcan.nodes.Util.*;
import static io.lacuna.bifurcan.utils.Bits.bitOffset;
import static io.lacuna.bifurcan.utils.Bits.highestBit;
import static java.lang.Integer.bitCount;
import static java.lang.System.arraycopy;

/**
 * @author ztellman
 */
public class IntMapNodes {

  public static class Node<V> {

    public final static Node POS_EMPTY = new Node(new Object(), 0, 0);
    public final static Node NEG_EMPTY = new Node(new Object(), -1, 0);

    public final Object editor;
    public final long prefix;
    public final int offset;

    public int datamap;
    public int nodemap;
    public long size;

    public long[] keys;
    public Object[] content;

    // constructors

    private Node(Object editor, long prefix, int offset, boolean empty) {
      this.editor = editor;
      this.prefix = prefix;
      this.offset = offset;
    }

    public Node(Object editor, long prefix, int offset) {
      this.editor = editor;
      this.prefix = prefix;
      this.offset = offset;
      this.keys = new long[2];
      this.content = new Object[2];
    }

    // lookup

    public Object get(long k, Object defaultVal) {
      Node<V> n = this;
      for (; ; ) {
        int mask = n.mask(k);
        if (n.isEntry(mask)) {
          int idx = n.entryIndex(mask);
          return n.keys[idx] == k ? n.content[idx] : defaultVal;
        } else if (n.isNode(mask)) {
          n = n.node(mask);
        } else {
          return defaultVal;
        }
      }
    }

    public IEntry<Long, V> nth(long idx) {
      PrimitiveIterator.OfInt masks = masks();
      while (masks.hasNext()) {
        int mask = masks.nextInt();
        if (isEntry(mask)) {
          if (idx-- == 0) {
            int entryIdx = entryIndex(mask);
            return IEntry.of(keys[entryIdx], (V) content[entryIdx]);
          }

        } else if (isNode(mask)) {
          Node<V> node = node(mask);
          if (idx < node.size()) {
            return node.nth(idx);
          } else {
            idx -= node.size();
          }
        }
      }

      throw new IndexOutOfBoundsException();
    }

    public OptionalLong indexOf(long key) {
      Node<V> n = this;
      long idx = 0;

      for (; ; ) {
        int mask = n.mask(key);
        if (n.isEntry(mask) || n.isNode(mask)) {
          PrimitiveIterator.OfInt masks = n.masks();
          while (masks.hasNext()) {
            int m = masks.next();
            if (mask == m) {
              if (n.isEntry(mask)) {
                return OptionalLong.of(idx);
              } else {
                n = n.node(mask);
                break;
              }
            } else {
              idx += n.isEntry(m) ? 1 : n.node(m).size();
            }
          }
        } else {
          return OptionalLong.empty();
        }
      }
    }

    public long floorIndex(long key, long offset) {

      if (min() > key) {
        return -1;

      } else {
        offset += size;
        PrimitiveIterator.OfInt masks = reverseMasks();
        while (masks.hasNext()) {
          int mask = masks.next();
          if (isEntry(mask)) {
            offset--;
            int idx = entryIndex(mask);
            if (keys[idx] <= key) {
              return offset;
            }
          } else if (isNode(mask)) {
            Node<V> n = node(mask);
            offset -= n.size;
            long idx = n.floorIndex(key, offset);
            if (idx >= 0) {
              return idx;
            }
          }
        }
      }

      return -1;
    }

    public long ceilIndex(long key, long offset) {

      if (max() < key) {
        return -1;

      } else {
        PrimitiveIterator.OfInt masks = masks();
        while (masks.hasNext()) {
          int mask = masks.next();
          if (isEntry(mask)) {
            int idx = entryIndex(mask);
            if (keys[idx] >= key) {
              return offset;
            }
            offset++;

          } else if (isNode(mask)) {
            Node<V> n = node(mask);
            long idx = n.ceilIndex(key, offset);
            if (idx >= 0) {
              return idx;
            }
            offset += n.size;

          }
        }
      }

      return -1;
    }

    // update

    public <U> Node<U> mapVals(Object editor, BiFunction<Long, V, U> f) {
      Node n = clone(editor);
      for (int i = bitCount(n.datamap); i >= 0; i--) {
        n.content[i] = f.apply(n.keys[i], (V) n.content[i]);
      }

      for (int i = content.length - 1 - bitCount(n.nodemap); i < content.length; i++) {
        n.content[i] = ((Node<V>) n.content[i]).mapVals(editor, f);
      }

      return n;
    }

    public Node<V> put(Object editor, long k, V v, BinaryOperator<V> mergeFn) {

      if (editor != this.editor) {
        return clone(editor).put(editor, k, v, mergeFn);
      } else if (size == 0) {
        Node<V> n = new Node<V>(editor, k, 0);
        return n.putEntry(n.mask(k), k, v);
      }

      int offsetPrime = offset(k, prefix);

      // common parent
      if (offsetPrime > this.offset) {
        Node<V> n = new Node<V>(editor, k, offsetPrime);
        if (size == 1) {
          n = n.putEntry(n.mask(prefix), keys[0], (V) content[0]);
        } else if (size > 0) {
          n = n.putNode(n.mask(prefix), this);
        }
        return n.putEntry(n.mask(k), k, v);

        // somewhere at or below our level
      } else {

        int mask = mask(k);
        if (isEntry(mask)) {
          int idx = entryIndex(mask);
          if (k == keys[idx]) {
            content[idx] = mergeFn.apply((V) content[idx], v);
            return this;
          } else {
            Node<V> n = new Node<V>(editor, k, offset(k, keys[idx]));
            n = n
              .putEntry(n.mask(keys[idx]), keys[idx], (V) content[idx])
              .putEntry(n.mask(k), k, v);
            return removeEntry(mask).putNode(mask, n);
          }
        } else if (isNode(mask)) {
          Node<V> n = node(mask);
          long prevSize = n.size();
          Node<V> nPrime = n.put(editor, k, v, mergeFn);
          setNode(mask, nPrime);
          if (n == nPrime) {
            size += nPrime.size() - prevSize;
          }
          return this;
        } else {
          return putEntry(mask, k, v);
        }
      }
    }

    public Node<V> remove(Object editor, long k) {
      int mask = mask(k);
      if ((mask & (nodemap | datamap)) == 0) {
        return this;
      } else if (editor != this.editor) {
        return clone(editor).remove(editor, k);
      }

      Node<V> result = null;

      if (isEntry(mask)) {
        int idx = entryIndex(mask);
        result = keys[idx] == k ? removeEntry(mask) : this;
      } else if (isNode(mask)) {
        Node<V> n = node(mask);
        long prevSize = n.size();
        boolean isLinear = n.editor == editor;
        Node<V> nPrime = n.remove(editor, k);
        if (isLinear) {
          size -= prevSize - nPrime.size();
        }

        if (nPrime.size == 0) {
          result = removeNode(mask);
        } else if (nPrime.size == 1) {
          result = removeNode(mask).putEntry(mask, nPrime.keys[0], (V) nPrime.content[0]);
        } else {
          result = setNode(mask, nPrime);
        }
      }

      return result.collapse();
    }


    // iteration

    private PrimitiveIterator.OfInt masks() {
      return Util.masks(nodemap | datamap);
    }

    private PrimitiveIterator.OfInt reverseMasks() {
      return Util.reverseMasks(nodemap | datamap);
    }

    public Iterator<IEntry<Long, V>> iterator() {

      if (size() == 0) {
        return Iterators.EMPTY;
      }

      return new Iterator<IEntry<Long, V>>() {

        final Node<V>[] stack = new Node[16];
        final byte[] cursors = new byte[32];
        int depth = 0;

        {
          stack[0] = Node.this;
          int bits = Node.this.nodemap | Node.this.datamap;
          cursors[0] = (byte) Util.startIndex(bits);
          cursors[1] = (byte) Util.endIndex(bits);
          nextValue();
        }

        private void nextValue() {
          while (depth >= 0) {
            int pos = depth << 1;
            int idx = cursors[pos];
            int limit = cursors[pos + 1];

            if (idx <= limit) {
              Node<V> curr = stack[depth];
              int mask = 1 << idx;

              if (curr.isEntry(mask)) {
                return;
              } else if (curr.isNode(mask)) {
                Node<V> next = curr.node(mask);
                stack[++depth] = next;
                int bits = next.nodemap | next.datamap;
                cursors[pos + 2] = (byte) Util.startIndex(bits);
                cursors[pos + 3] = (byte) Util.endIndex(bits);
                cursors[pos]++;
              } else {
                cursors[pos]++;
              }
            } else {
              depth--;
            }
          }
        }

        @Override
        public boolean hasNext() {
          return depth >= 0;
        }

        @Override
        public IEntry<Long, V> next() {
          Node<V> n = stack[depth];
          int mask = 1 << cursors[depth << 1];
          int idx = n.entryIndex(mask);
          IEntry<Long, V> e = IEntry.of(n.keys[idx], (V) n.content[idx]);

          cursors[depth << 1]++;
          nextValue();
          return e;
        }
      };
    }

    // set operations

    public Node<V> slice(Object editor, long min, long max) {

      if (!overlap(min, max)) {
        return null;
      } else if (min <= min() && max() <= max) {
        return this;
      }

      Node<V> n = new Node<V>(editor, prefix, offset);

      PrimitiveIterator.OfInt masks = masks();
      while (masks.hasNext()) {
        int mask = masks.nextInt();
        if (isEntry(mask)) {
          int idx = entryIndex(mask);
          long key = keys[idx];
          if (min <= key && key <= max) {
            n = n.put(editor, key, (V) content[idx], null);
          }
        } else if (isNode(mask)) {
          Node<V> child = node(mask).slice(editor, min, max);
          if (child != null) {
            n = merge(editor, n, child, null);
          }
        }
      }

      return n.collapse();
    }

    // misc

    public long size() {
      return size;
    }

    public boolean equals(Node<V> n, BiPredicate<V, V> equalsFn) {

      if (n == this) {
        return true;
      }

      if (size == n.size && datamap == n.datamap && nodemap == n.nodemap) {
        int numEntries = bitCount(datamap);
        for (int i = 0; i < numEntries; i++) {
          if (keys[i] != n.keys[i] || !equalsFn.test((V) content[i], (V) n.content[i])) {
            return false;
          }
        }

        int numNodes = bitCount(nodemap);
        for (int i = 0; i < numNodes; i++) {
          Node<V> child = (Node<V>) content[content.length - 1 - i];
          if (!child.equals((Node<V>) n.content[n.content.length - 1 - i], equalsFn)) {
            return false;
          }
        }

        return true;
      }

      return false;
    }

    /// private

    private int mask(long key) {
      return 1 << ((key & (0xFL << offset)) >>> offset);
    }

    public long key(int mask) {
      return keys[entryIndex(mask)];
    }

    private boolean isEntry(int mask) {
      return (datamap & mask) != 0;
    }

    public boolean isNode(int mask) {
      return (nodemap & mask) != 0;
    }

    private int entryIndex(int mask) {
      return compressedIndex(datamap, mask);
    }

    private int nodeIndex(int mask) {
      return compressedIndex(nodemap, mask);
    }

    private long min() {
      long mask = prefix;
      mask &= ~(offset == 60 ? -1 : ((1L << (offset + 4)) - 1));
      mask |= prefix & ~Long.MAX_VALUE;

      return mask;
    }

    private long max() {
      long mask = prefix;
      mask |= offset == 60 ? (prefix < 0 ? -1 : Long.MAX_VALUE) : ((1L << (offset + 4)) - 1);
      mask |= prefix & ~Long.MAX_VALUE;

      return mask;
    }

    private boolean overlap(long min, long max) {
      return IntMapNodes.overlap(min, max, min(), max());
    }

    public Node<V> node(int mask) {
      return (Node<V>) content[content.length - 1 - nodeIndex(mask)];
    }

    private Node<V> clone(Object editor) {
      Node<V> n = new Node<V>(editor, prefix, offset, false);
      n.datamap = datamap;
      n.nodemap = nodemap;
      n.size = size;
      n.content = content.clone();
      n.keys = keys.clone();

      return n;
    }

    private void grow() {
      if (content.length == 32) {
        return;
      }

      Object[] c = new Object[content.length << 1];
      long[] k = new long[keys.length << 1];
      int numNodes = bitCount(nodemap);
      int numEntries = bitCount(datamap);

      arraycopy(content, 0, c, 0, numEntries);
      arraycopy(content, content.length - numNodes, c, c.length - numNodes, numNodes);
      arraycopy(keys, 0, k, 0, numEntries);

      this.keys = k;
      this.content = c;
    }

    private Node<V> collapse() {
      if (datamap == 0 && nodemap > 0 && Bits.isPowerOfTwo(nodemap)) {
        return node(nodemap);
      } else if (size() == 1) {
        long key = keys[0];
        Node<V> n = new Node<V>(editor, key, 0);
        return n.putEntry(n.mask(key), key, (V) content[0]);
      } else {
        return this;
      }
    }

    Node<V> putEntry(int mask, long key, V value) {

      assert ((datamap | nodemap) & mask) == 0;

      int numEntries = bitCount(datamap);
      int count = numEntries + bitCount(nodemap);
      if ((count + 1) > content.length) {
        grow();
      }

      int idx = entryIndex(mask);
      if (idx != numEntries) {
        arraycopy(content, idx, content, idx + 1, numEntries - idx);
        arraycopy(keys, idx, keys, idx + 1, numEntries - idx);
      }
      datamap |= mask;
      size++;

      keys[idx] = key;
      content[idx] = value;

      return this;
    }

    Node<V> removeEntry(final int mask) {

      assert (mask & datamap) > 0;

      // shrink?

      final int idx = entryIndex(mask);
      final int numEntries = bitCount(datamap);
      if (idx != numEntries - 1) {
        arraycopy(content, idx + 1, content, idx, numEntries - 1 - idx);
        arraycopy(keys, idx + 1, keys, idx, numEntries - 1 - idx);
      }
      datamap &= ~mask;
      size--;

      content[numEntries - 1] = null;
      keys[numEntries - 1] = 0;

      return this;
    }

    Node<V> setNode(int mask, Node<V> node) {

      assert (nodemap & mask) > 0;

      int idx = content.length - 1 - nodeIndex(mask);
      size += node.size() - ((Node<V>) content[idx]).size();
      content[idx] = node;

      return this;
    }

    Node<V> putNode(final int mask, Node<V> node) {

      assert ((nodemap | datamap) & mask) == 0;
      assert node.offset < this.offset;

      if (node.size() == 1) {
        return putEntry(mask, node.keys[0], (V) node.content[0]);
      }

      int numNodes = bitCount(nodemap);
      int count = bitCount(datamap) + numNodes;
      if ((count + 1) > content.length) {
        grow();
      }

      int idx = nodeIndex(mask);
      if (numNodes > 0) {
        arraycopy(content, content.length - numNodes, content, content.length - 1 - numNodes, numNodes - idx);
      }
      nodemap |= mask;
      size += node.size();

      content[content.length - 1 - idx] = node;

      return this;
    }

    Node<V> removeNode(final int mask) {
      // shrink?

      int idx = nodeIndex(mask);
      int numNodes = bitCount(nodemap);
      size -= node(mask).size();
      arraycopy(content, content.length - numNodes, content, content.length + 1 - numNodes, numNodes - 1 - idx);
      nodemap &= ~mask;

      content[content.length - numNodes] = null;

      return this;
    }

  }

  public static int offset(long a, long b) {
    return bitOffset(highestBit(a ^ b)) & ~0x3;
  }

  private static boolean overlap(long min0, long max0, long min1, long max1) {
    return (max1 - min0) >= 0 && (max0 - min1) >= 0;
  }

  public static <V> IList<Node<V>> split(Object editor, Node<V> node, long targetSize) {
    IList<Node<V>> result = new LinearList<>();
    if ((node.size() >> 1) < targetSize) {
      result.addLast(node);
    } else {
      Node<V> acc = new Node<>(editor, node.prefix, node.offset);

      PrimitiveIterator.OfInt masks = Util.masks(node.datamap | node.nodemap);
      while (masks.hasNext()) {
        int mask = masks.nextInt();

        if (acc.size() >= targetSize) {
          result.addLast(acc);
          acc = new Node<>(editor, node.prefix, node.offset);
        }

        if (node.isEntry(mask)) {
          acc = transferEntry(mask, node, acc);
        } else if (node.isNode(mask)) {
          Node<V> child = node.node(mask);
          if (child.size() >= (targetSize << 1)) {
            split(editor, child, targetSize).forEach(result::addLast);
          } else {
            acc = acc.putNode(mask, child);
          }
        }
      }

      if (acc.size() > 0) {
        result.addLast(acc);
      }
    }

    return result;
  }

  public static <V> Node<V> merge(Object editor, Node<V> a, Node<V> b, BinaryOperator<V> mergeFn) {

    if (a.size() == 0) {
      return b;
    } else if (b.size() == 0) {
      return a;
    }

    int offsetPrime = offset(a.prefix, b.prefix);

    // don't overlap, share a common parent
    if (offsetPrime > a.offset && offsetPrime > b.offset) {
      Node<V> n = new Node<V>(editor, a.prefix, offsetPrime);
      return merge(editor, n.putNode(n.mask(a.prefix), a), b, mergeFn);
    }

    // a contains b
    if (a.offset > b.offset) {
      int mask = a.mask(b.prefix);
      if (a.isEntry(mask)) {
        int idx = a.entryIndex(mask);
        long key = a.keys[idx];
        V val = (V) a.content[idx];
        return a.clone(editor)
          .removeEntry(mask)
          .putNode(mask, b)
          .put(editor, key, val, (x, y) -> mergeFn.apply(y, x));

      } else if (a.isNode(mask)) {
        return a.clone(editor)
          .setNode(mask, merge(editor, a.node(mask), b, mergeFn));

      } else {
        return a.clone(editor)
          .putNode(mask, b);
      }

      // b contains a
    } else if (a.offset < b.offset) {
      return merge(editor, b, a, (x, y) -> mergeFn.apply(y, x));

      // a and b are siblings
    } else {
      Node<V> result = new Node<V>(editor, a.prefix, a.offset);

      PrimitiveIterator.OfInt masks = Util.masks(a.datamap | a.nodemap | b.datamap | b.nodemap);
      while (masks.hasNext()) {

        int mask = masks.nextInt();
        int state = mergeState(mask, a.nodemap, a.datamap, b.nodemap, b.datamap);
        int idx;
        switch (state) {
          case NODE_NONE:
          case NONE_NODE:
            result = transferNode(mask, state == NODE_NONE ? a : b, result);
            break;
          case ENTRY_NONE:
          case NONE_ENTRY:
            result = transferEntry(mask, state == ENTRY_NONE ? a : b, result);
            break;
          case ENTRY_ENTRY:
            result = transferEntry(mask, a, result);
            idx = b.entryIndex(mask);
            result = result.put(editor, b.keys[idx], (V) b.content[idx], mergeFn);
            break;
          case NODE_NODE:
            result = result.putNode(mask, merge(editor, a.node(mask), b.node(mask), mergeFn));
            break;
          case NODE_ENTRY:
            idx = b.entryIndex(mask);
            result = result
              .putNode(mask, a.node(mask))
              .put(editor, b.keys[idx], (V) b.content[idx], mergeFn);
            break;
          case ENTRY_NODE:
            idx = a.entryIndex(mask);
            result = result
              .putNode(mask, b.node(mask))
              .put(editor, a.keys[idx], (V) a.content[idx], (x, y) -> mergeFn.apply(y, x));
            break;
          case NONE_NONE:
            break;
        }
      }

      return result;
    }
  }

  public static <V> Node<V> difference(Object editor, Node<V> a, Node<V> b) {

    int offsetPrime = offset(a.prefix, b.prefix);

    // don't overlap, share a common parent
    if (offsetPrime > a.offset && offsetPrime > b.offset) {
      return a;
    }

    Node<V> result = null;

    // a contains b
    if (a.offset > b.offset) {
      int mask = a.mask(b.prefix);
      if (a.isEntry(mask)) {
        long key = a.key(mask);
        Node<V> nPrime = b.get(key, DEFAULT_VALUE) == DEFAULT_VALUE
          ? a
          : a.clone(editor).remove(editor, key);
        result = nPrime.size() == 0 ? null : nPrime;
      } else if (a.isNode(mask)) {
        result = a.clone(editor).removeNode(mask);
        Node<V> nPrime = difference(editor, a.node(mask), b);
        if (nPrime != null) {
          result = result.putNode(mask, nPrime);
        }
      } else {
        result = a;
      }

      // b contains a
    } else if (a.offset < b.offset) {
      int mask = b.mask(a.prefix);
      if (b.isEntry(mask)) {
        Node<V> nPrime = a.remove(editor, b.key(mask));

        result = nPrime.size() == 0 ? null : nPrime;
      } else if (b.isNode(mask)) {
        result = difference(editor, a, b.node(mask));
      } else {
        result = a;
      }

      // a and b are siblings
    } else {
      result = new Node<V>(editor, a.prefix, a.offset);

      PrimitiveIterator.OfInt masks = Util.masks(a.datamap | a.nodemap);
      while (masks.hasNext()) {

        int mask = masks.nextInt();
        int state = mergeState(mask, a.nodemap, a.datamap, b.nodemap, b.datamap);
        switch (state) {
          case NODE_NONE:
            result = transferNode(mask, a, result);
            break;
          case ENTRY_NONE:
            result = transferEntry(mask, a, result);
            break;
          case ENTRY_ENTRY:
            if (a.key(mask) != b.key(mask)) {
              result = transferEntry(mask, a, result);
            }
            break;
          case NODE_NODE:
            Node<V> nPrime = difference(editor, a.node(mask), b.node(mask));
            if (nPrime != null) {
              result = result.putNode(mask, nPrime);
            }
            break;
          case NODE_ENTRY:
            nPrime = a.node(mask).remove(editor, b.key(mask));
            if (nPrime.size > 0) {
              result = result.putNode(mask, nPrime);
            }
            break;
          case ENTRY_NODE:
            if (b.get(a.key(mask), DEFAULT_VALUE) == DEFAULT_VALUE) {
              result = transferEntry(mask, a, result);
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    return result == null || result.size() == 0 ? null : result.collapse();
  }

  public static <V> Node<V> intersection(Object editor, Node<V> a, Node<V> b) {

    int offsetPrime = offset(a.prefix, b.prefix);

    // don't overlap, share a common parent
    if (offsetPrime > a.offset && offsetPrime > b.offset) {
      return null;
    }

    Node<V> result  = null;

    // a contains b
    if (a.offset > b.offset) {
      int mask = a.mask(b.prefix);
      if (a.isEntry(mask)) {
        result = b.get(a.key(mask), DEFAULT_VALUE) == DEFAULT_VALUE
          ? null
          : transferEntry(mask, a, new Node<V>(editor, a.prefix, a.offset));
      } else if (a.isNode(mask)) {
        result = intersection(editor, a.node(mask), b);
      } else {
        result = null;
      }

      // b contains a
    } else if (a.offset < b.offset) {
      int mask = b.mask(a.prefix);
      if (b.isEntry(mask)) {
        long key = b.key(mask);
        Object value = a.get(key, DEFAULT_VALUE);
        result = value == DEFAULT_VALUE
          ? null
          : new Node<V>(editor, a.prefix, a.offset).putEntry(a.mask(key), key, (V) value);
      } else if (b.isNode(mask)) {
        result = intersection(editor, a, b.node(mask));
      } else {
        result = null;
      }

      // a and b are siblings
    } else {
      result = new Node<V>(editor, a.prefix, a.offset);

      PrimitiveIterator.OfInt masks = Util.masks((a.datamap | a.nodemap) & (b.datamap | b.nodemap));
      while (masks.hasNext()) {

        int mask = masks.nextInt();
        int state = mergeState(mask, a.nodemap, a.datamap, b.nodemap, b.datamap);
        switch (state) {
          case ENTRY_ENTRY:
            if (a.key(mask) == b.key(mask)) {
              result = transferEntry(mask, a, result);
            }
            break;
          case NODE_NODE:
            Node<V> n = intersection(editor, a.node(mask), b.node(mask));
            if (n != null) {
              result = result.putNode(mask, n);
            }
            break;
          case NODE_ENTRY:
            long key = b.key(mask);
            Object val = a.get(key, DEFAULT_VALUE);
            if (val != DEFAULT_VALUE) {
              result = result.putEntry(mask, key, (V) val);
            }
            break;
          case ENTRY_NODE:
            if (b.get(a.key(mask), DEFAULT_VALUE) != DEFAULT_VALUE) {
              result = transferEntry(mask, a, result);
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    return (result == null || result.size() == 0) ? null : result.collapse();
  }

  private static <V> Node<V> transferNode(int mask, Node<V> src, Node<V> dst) {
    return dst.putNode(mask, src.node(mask));
  }

  private static <V> Node<V> transferEntry(int mask, Node<V> src, Node<V> dst) {
    int idx = src.entryIndex(mask);
    return dst.putEntry(mask, src.keys[idx], (V) src.content[idx]);
  }

}
