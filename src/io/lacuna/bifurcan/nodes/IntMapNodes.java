package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.IMap.IEntry;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
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

  public static int offset(long a, long b) {
    return bitOffset(highestBit(a ^ b)) & ~0x3;
  }

  private static boolean overlap(long min0, long max0, long min1, long max1) {
    return (max1 - min0) >= 0 && (max0 - min1) >= 0;
  }

  public static class Node<V> {

    public final static Node POS_EMPTY = new Node(new Object(), 0, 0);
    public final static Node NEG_EMPTY = new Node(new Object(), -1, 0);

    public final Object editor;
    public final long prefix;
    public final int offset;

    public int datamap;
    public int nodemap;
    public int size;

    public long[] keys;
    public Object[] content;

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

    private long keyMask() {
      return 0xFL << offset;
    }

    public int mask(long key) {
      return 1 << ((key & keyMask()) >>> offset);
    }

    public boolean isEntry(int mask) {
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
      return prefix & ~(offset == 60 ? -1 : ((1L << (offset + 4)) - 1));
    }

    private long max() {
      return prefix | (offset == 60 ? -1 : ((1L << (offset + 4)) - 1));
    }

    private boolean overlap(long min, long max) {
      return IntMapNodes.overlap(min, max, min(), max());
    }

    private Node<V> node(int mask) {
      return (Node<V>) content[content.length - 1 - nodeIndex(mask)];
    }

    private PrimitiveIterator.OfInt masks() {
      return Util.masks(nodemap | datamap);
    }

    private PrimitiveIterator.OfInt reverseMasks() {
      return Util.reverseMasks(nodemap | datamap);
    }

    public IEntry<Long, V> nth(int idx) {
      PrimitiveIterator.OfInt masks = masks();
      while (masks.hasNext()) {
        int mask = masks.nextInt();
        if (isEntry(mask)) {
          if (idx-- == 0) {
            int entryIdx = entryIndex(mask);
            return new Maps.Entry<>(keys[entryIdx], (V) content[entryIdx]);
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

    public IEntry<Long, V> floor(long key) {

      // all possible values are higher than the key
      if (min() > key) {
        return null;

        // all possible values are lower than the key
      } else if (max() < key) {
        int mask = Bits.highestBit(nodemap | datamap);
        if (isEntry(mask)) {
          int idx = entryIndex(mask);
          return new Maps.Entry<>(keys[idx], (V) content[idx]);
        } else {
          return node(mask).floor(key);
        }

        // somewhere in between
      } else {
        PrimitiveIterator.OfInt masks = reverseMasks();
        while (masks.hasNext()) {
          int mask = masks.next();
          if (isEntry(mask)) {
            int idx = entryIndex(mask);
            if (keys[idx] <= key) {
              return new Maps.Entry<>(keys[idx], (V) content[idx]);
            }
          } else if (isNode(mask)) {
            IEntry<Long, V> entry = node(mask).floor(key);
            if (entry != null) {
              return entry;
            }
          }
        }
      }

      return null;
    }

    public IEntry<Long, V> ceil(long key) {

      // all possible values are lower than the key
      if (max() < key) {
        return null;

        // all possible values are higher than the key
      } else if (min() > key) {
        int mask = Bits.lowestBit(nodemap | datamap);
        if (isEntry(mask)) {
          int idx = entryIndex(mask);
          return new Maps.Entry<>(keys[idx], (V) content[idx]);
        } else {
          return node(mask).ceil(key);
        }

        // somewhere in between
      } else {
        PrimitiveIterator.OfInt masks = masks();
        while (masks.hasNext()) {
          int mask = masks.next();
          if (isEntry(mask)) {
            int idx = entryIndex(mask);
            if (keys[idx] >= key) {
              return new Maps.Entry<>(keys[idx], (V) content[idx]);
            }
          } else if (isNode(mask)) {
            IEntry<Long, V> entry = node(mask).ceil(key);
            if (entry != null) {
              return entry;
            }
          }
        }
      }

      return null;
    }

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
            n = n.putNode(mask, child);
          }
        }
      }

      return n;
    }

    public Iterator<IEntry<Long, V>> iterator() {
      return new Iterator<IMap.IEntry<Long, V>>() {

        final LinearList<Node<V>> nodeStack = new LinearList<>();
        final LinearList<PrimitiveIterator.OfInt> maskStack = new LinearList<>();
        Node<V> node = Node.this;
        PrimitiveIterator.OfInt masks = node.masks();

        @Override
        public boolean hasNext() {
          return masks.hasNext() || nodeStack.size() > 0;
        }

        @Override
        public IMap.IEntry<Long, V> next() {
          while (true) {

            if (!masks.hasNext()) {
              if (nodeStack.size() == 0) {
                throw new NoSuchElementException();
              }
              node = nodeStack.popLast();
              masks = maskStack.popLast();
            }

            int mask = masks.nextInt();
            if (node.isEntry(mask)) {
              int idx = node.entryIndex(mask);
              return new Maps.Entry<>(node.keys[idx], (V) node.content[idx]);
            } else if (node.isNode(mask)) {
              if (masks.hasNext()) {
                nodeStack.addLast(node);
                maskStack.addLast(masks);
              }
              node = node.node(mask);
              masks = node.masks();
            }
          }
        }
      };
    }

    public Object get(long k, Object defaultVal) {
      int mask = mask(k);
      if (isEntry(mask)) {
        int idx = entryIndex(mask);
        return keys[idx] == k ? content[idx] : defaultVal;
      } else if (isNode(mask)) {
        return node(mask).get(k, defaultVal);
      } else {
        return defaultVal;
      }
    }

    public int size() {
      return size;
    }

    public long key(int mask) {
      return keys[entryIndex(mask)];
    }

    public Node<V> merge(Object editor, Node<V> node, BinaryOperator<V> mergeFn) {
      if (editor != this.editor) {
        return clone(editor).merge(editor, node, mergeFn);
      }

      return IntMapNodes.merge(editor, this, node, mergeFn);
    }

    public Node<V> difference(Object editor, Node<V> node) {
      if (editor != this.editor) {
        return clone(editor).difference(editor, node);
      }

      return IntMapNodes.difference(editor, this, node);
    }

    public Node<V> intersection(Object editor, Node<V> node) {
      if (editor != this.editor) {
        return clone(editor).intersection(editor, node);
      }

      return IntMapNodes.intersection(editor, this, node);
    }

    public Node<V> put(Object editor, long k, V v, BinaryOperator<V> mergeFn) {

      if (editor != this.editor) {
        return clone(editor).put(editor, k, v, mergeFn);
      }

      int offsetPrime = offset(k, prefix);

      // common parent
      if (offsetPrime > this.offset) {
        Node<V> n = new Node<V>(editor, k, offsetPrime);
        switch (size) {
          case 0:
            break;
          case 1:
            n = n.putEntry(n.mask(prefix), keys[0], (V) content[0]);
            break;
          default:
            n = n.putNode(n.mask(prefix), this);
            break;
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
            n = n.putEntry(n.mask(keys[idx]), keys[idx], (V) content[idx]).putEntry(n.mask(k), k, v);
            return removeEntry(mask).putNode(mask, n);
          }
        } else if (isNode(mask)) {
          Node<V> n = node(mask);
          int prevSize = n.size();
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

      if (isEntry(mask)) {
        int idx = entryIndex(mask);
        return keys[idx] == k ? removeEntry(mask) : this;
      } else if (isNode(mask)) {
        Node<V> n = node(mask);
        int prevSize = n.size();
        Node<V> nPrime = n.remove(editor, k);
        if (n == nPrime) {
          size -= prevSize - nPrime.size();
        }
        switch (nPrime.size()) {
          case 0:
            return removeNode(mask);
          case 1:
            return removeNode(mask).putEntry(mask, nPrime.keys[0], (V) nPrime.content[0]);
          default:
            return setNode(mask, nPrime);
        }
      }

      return this;
    }

    public boolean equals(Node<V> n, BiPredicate<V, V> equalsFn) {
      if (datamap == n.datamap && nodemap == n.nodemap) {
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

    ///

    private Node<V> clone(Object editor) {
      Node<V> n = new Node<V>(editor, prefix, offset, false);
      n.datamap = datamap;
      n.nodemap = nodemap;
      n.size = size;
      n.content = content.clone();
      n.keys = keys.clone();

      return n;
    }

    public Iterable<Node<V>> nodes() {
      return () ->
          Iterators.range(
              content.length - Integer.bitCount(nodemap),
              content.length,
              i -> (Node<V>) content[(int) i]);
    }

    private Iterable<IMap.IEntry<Long, V>> entries() {
      return () ->
          Iterators.range(bitCount(datamap),
              i -> new Maps.Entry<>(keys[(int) i], (V) content[(int) i]));
    }

    private void grow() {
      Object[] c = new Object[content.length << 1];
      int numNodes = bitCount(nodemap);
      arraycopy(content, 0, c, 0, bitCount(datamap));
      arraycopy(content, content.length - numNodes, c, c.length - numNodes, numNodes);
      this.content = c;

      long[] k = new long[keys.length << 1];
      arraycopy(keys, 0, k, 0, bitCount(datamap));
      this.keys = k;
    }

    Node<V> putEntry(int mask, long key, V value) {
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
      int idx = content.length - 1 - nodeIndex(mask);
      size += node.size() - ((Node<V>) content[idx]).size();
      content[idx] = node;

      return this;
    }

    Node<V> putNode(final int mask, Node<V> node) {
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

  public static <V> IList<Node<V>> split(Object editor, Node<V> node, int targetSize) {
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
            split(editor, child, targetSize).stream()
                .map(n -> new Node<V>(editor, node.prefix, node.offset).putNode(mask, n))
                .forEach(result::addLast);
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
      return new Node<V>(editor, a.prefix, offsetPrime)
          .merge(editor, a, mergeFn)
          .merge(editor, b, mergeFn);
    }

    // we contain the other node
    if (a.offset > b.offset) {
      int mask = a.mask(b.prefix);
      if (a.isEntry(mask)) {
        int idx = a.entryIndex(mask);
        long key = a.keys[idx];
        V val = (V) a.content[idx];
        return a.removeEntry(mask).putNode(mask, b.put(editor, key, val, (x, y) -> mergeFn.apply(y, x)));
      } else if (a.isNode(mask)) {
        return a.setNode(mask, a.node(mask).merge(editor, b, mergeFn));
      } else {
        return a.putNode(mask, b);
      }

      // it contains us
    } else if (a.offset < b.offset) {
      return b.merge(editor, a, (x, y) -> mergeFn.apply(y, x));

      // we're on the same level
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
            result = transferEntry(mask, b, result);
            break;
          case NODE_NODE:
            result = result.putNode(mask, a.node(mask).merge(editor, b.node(mask), mergeFn));
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

    // we contain the other node
    if (a.offset > b.offset) {
      int mask = a.mask(b.prefix);
      if (a.isEntry(mask)) {
        return b.get(a.key(mask), DEFAULT_VALUE) == DEFAULT_VALUE ? a : null;
      } else if (a.isNode(mask)) {
        return a.setNode(mask, a.node(mask).difference(editor, b));
      } else {
        return a;
      }

      // it contains us
    } else if (a.offset < b.offset) {
      int mask = b.mask(a.prefix);
      if (b.isEntry(mask)) {
        return a.remove(editor, b.key(mask));
      } else if (b.isNode(mask)) {
        return a.difference(editor, b.node(mask));
      } else {
        return a;
      }

      // we're on the same level
    } else {
      Node<V> result = new Node<V>(editor, a.prefix, a.offset);

      PrimitiveIterator.OfInt masks = Util.masks(a.datamap | a.nodemap | b.datamap | b.nodemap);
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
            result = result.setNode(mask, a.node(mask).difference(editor, b.node(mask)));
            break;
          case NODE_ENTRY:
            result = result.setNode(mask, a.node(mask).remove(editor, b.key(mask)));
            break;
          case ENTRY_NODE:
            if (b.get(a.key(mask), DEFAULT_VALUE) == DEFAULT_VALUE) {
              result = transferEntry(mask, a, result);
            }
            break;
          case NONE_ENTRY:
          case NONE_NODE:
          case NONE_NONE:
            break;
        }
      }

      return result;
    }
  }

  public static <V> Node<V> intersection(Object editor, Node<V> a, Node<V> b) {

    int offsetPrime = offset(a.prefix, b.prefix);

    // don't overlap, share a common parent
    if (offsetPrime > a.offset && offsetPrime > b.offset) {
      return a;
    }

    // we contain the other node
    if (a.offset > b.offset) {
      int mask = a.mask(b.prefix);
      if (a.isEntry(mask)) {
        return b.get(a.key(mask), DEFAULT_VALUE) == DEFAULT_VALUE ? a : null;
      } else if (a.isNode(mask)) {
        return a.setNode(mask, a.node(mask).difference(editor, b));
      } else {
        return a;
      }

      // it contains us
    } else if (a.offset < b.offset) {
      int mask = b.mask(a.prefix);
      if (b.isEntry(mask)) {
        return a.remove(editor, b.key(mask));
      } else if (b.isNode(mask)) {
        return a.difference(editor, b.node(mask));
      } else {
        return a;
      }

      // we're on the same level
    } else {
      Node<V> result = new Node<V>(editor, a.prefix, a.offset);

      PrimitiveIterator.OfInt masks = Util.masks(a.datamap | a.nodemap | b.datamap | b.nodemap);
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
            result = transferNode(mask, a.node(mask).intersection(editor, b.node(mask)), result);
            break;
          case NODE_ENTRY:
            long key = a.key(mask);
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
          case ENTRY_NONE:
          case NODE_NONE:
          case NONE_ENTRY:
          case NONE_NODE:
          case NONE_NONE:
            break;
        }
      }

      return result;
    }
  }

  private static <V> Node<V> transferNode(int mask, Node<V> src, Node<V> dst) {
    return dst.putNode(mask, src.node(mask));
  }

  private static <V> Node<V> transferEntry(int mask, Node<V> src, Node<V> dst) {
    int idx = src.entryIndex(mask);
    return dst.putEntry(mask, src.keys[idx], (V) src.content[idx]);
  }


}
