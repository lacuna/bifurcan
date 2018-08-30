package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.ArrayVector;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

import static io.lacuna.bifurcan.nodes.MapNodes.Node.SHIFT_INCREMENT;
import static io.lacuna.bifurcan.nodes.Util.*;
import static java.lang.Integer.bitCount;
import static java.lang.System.arraycopy;

/**
 * This is an implementation based on the one described in https://michael.steindorfer.name/publications/oopsla15.pdf.
 * <p>
 * It adds in support for transient/linear updates, and allows for empty buffer space between the nodes and nodes
 * to minimize allocations when a node is repeatedly updated in-place.
 *
 * @author ztellman
 */
public class MapNodes {

  interface INode<K, V> {

    INode<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, BinaryOperator<V> merge);

    INode<K, V> remove(int shift, Object editor, int hash, K key, BiPredicate<K, K> equals);

    <U> INode<K, U> mapVals(Object editor, BiFunction<K, V, U> f);

    int hash(int idx);

    long size();

    IEntry<K, V> nth(long idx);

    long indexOf(int shift, int hash, K key, BiPredicate<K, K> equals);

    Iterable<IEntry<K, V>> entries();

    boolean equals(INode<K, V> n, BiPredicate<K, K> keyEquals, BiPredicate<V, V> valEquals);
  }

  public static class Node<K, V> implements INode<K, V> {

    public static final Node EMPTY = new Node(new Object());

    public static final int SHIFT_INCREMENT = 5;

    public int datamap = 0;
    public int nodemap = 0;
    public int[] hashes;
    public Object[] content;
    Object editor;
    long size;

    // constructor

    public Node() {
    }

    private Node(Object editor) {
      this.editor = editor;
      this.hashes = new int[2];
      this.content = new Object[4];
    }

    // lookup

    @Override
    public long indexOf(int shift, int hash, K key, BiPredicate<K, K> equals) {
      int mask = hashMask(hash, shift);
      if (isEntry(mask)) {
        int idx = entryIndex(mask);
        return /*hash == hashes[idx] &&*/ equals.test(key, (K) content[idx << 1]) ? idx : -1;

      } else if (isNode(mask)) {
        long idx = node(mask).indexOf(shift + SHIFT_INCREMENT, hash, key, equals);
        if (idx == -1) {
          return -1;
        } else {
          int nodeIdx = nodeIndex(mask);
          idx += bitCount(datamap);
          for (int i = 0; i < nodeIdx; i++) {
            idx += ((INode<K, V>) content[content.length - (i + 1)]).size();
          }
          return idx;
        }

      } else {
        return -1;
      }
    }

    @Override
    public IEntry<K, V> nth(long idx) {

      // see if the entry is local to this node
      int numEntries = bitCount(datamap);
      if (idx < numEntries) {
        int contentIdx = (int) (idx << 1);
        return new Maps.Entry<>((K) content[contentIdx], (V) content[contentIdx + 1]);
      }

      // see if the entry is local to our children
      if (idx < size) {
        idx -= numEntries;
        for (INode<K, V> node : nodes()) {
          if (idx < node.size()) {
            return node.nth(idx);
          }
          idx -= node.size();
        }
      }

      throw new IndexOutOfBoundsException();
    }

    @Override
    public int hash(int idx) {
      return hashes[idx];
    }

    // updates

    private boolean mergeEntry(int shift, int mask, int hash, K key, V value, BiPredicate<K, K> equals, BinaryOperator<V> merge) {
      int idx = entryIndex(mask);

      // there's a match
      boolean collision = hash == hashes[idx];
      if (collision && equals.test(key, (K) content[idx << 1])) {

        idx = (idx << 1) + 1;
        content[idx] = merge.apply((V) content[idx], value);
        return false;

        // collision, put them both in a node together
      } else {
        K currKey = (K) content[idx << 1];
        V currValue = (V) content[(idx << 1) + 1];

        INode<K, V> node;
        if (collision) {
          node = new Collision<>(hash, currKey, currValue, key, value);
        } else {
          node = new Node<K, V>(editor)
            .put(shift + SHIFT_INCREMENT, editor, hashes[idx], currKey, currValue, equals, merge)
            .put(shift + SHIFT_INCREMENT, editor, hash, key, value, equals, merge);
        }

        removeEntry(mask).putNode(mask, node);
        return true;
      }
    }

    @Override
    public Node<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, BinaryOperator<V> merge) {

      if (editor != this.editor) {
        return clone(editor).put(shift, editor, hash, key, value, equals, merge);
      }

      Node<K, V> n = this;
      int currShift = shift;
      boolean increment;
      for (; ; ) {

        int mask = hashMask(hash, currShift);

        // overwrite potential collision
        if (n.isEntry(mask)) {
          increment = n.mergeEntry(currShift, mask, hash, key, value, equals, merge);
          break;

          // we have to go deeper
        } else if (n.isNode(mask)) {
          INode<K, V> child = n.node(mask);

          // since we're not changing anything at this level, just head down
          if (child instanceof Node && ((Node<K, V>) child).editor == editor) {
            n = (Node<K, V>) child;
            currShift += SHIFT_INCREMENT;

            // we need to maintain the stack, sadly
          } else {
            long prevSize = child.size();
            INode<K, V> nodePrime = child.put(currShift + SHIFT_INCREMENT, editor, hash, key, value, equals, merge);
            increment = nodePrime.size() != prevSize;
            n.setNode(mask, nodePrime, increment ? 1 : 0);
            break;
          }

          // no existing entry
        } else {
          n.putEntry(mask, hash, key, value);
          increment = true;
          break;
        }
      }

      // we've descended, and need to update the sizes of our parents
      if (n != this && increment) {
        Node<K, V> currNode = this;
        currShift = shift;
        while (currNode != n) {
          currNode.size++;
          currNode = (Node<K, V>) currNode.node(hashMask(hash, currShift));
          currShift += SHIFT_INCREMENT;
        }
      }

      return this;
    }

    @Override
    public INode<K, V> remove(int shift, Object editor, int hash, K key, BiPredicate<K, K> equals) {

      int mask = hashMask(hash, shift);

      // there's a potential match
      if (isEntry(mask)) {
        int idx = entryIndex(mask);

        // there is a match
        if (hashes[idx] == hash && equals.test(key, (K) content[idx << 1])) {
          return (this.editor == editor ? this : clone(editor)).removeEntry(mask).collapse(shift);

          // nope
        } else {
          return this;
        }

        // we must go deeper
      } else if (isNode(mask)) {
        INode<K, V> node = node(mask);
        long prevSize = node.size();
        INode<K, V> nodePrime = node.remove(shift + SHIFT_INCREMENT, editor, hash, key, equals);

        Node<K, V> n = this.editor == editor ? this : clone(editor);
        switch ((int) nodePrime.size()) {
          case 0:
            return n.removeNode(mask, prevSize).collapse(shift);
          case 1:
            IEntry<K, V> e = nodePrime.nth(0);
            return n.removeNode(mask, prevSize).putEntry(mask, nodePrime.hash(0), e.key(), e.value());
          default:
            return n.setNode(mask, nodePrime, nodePrime.size() - prevSize).collapse(shift);
        }

        // no such thing
      } else {
        return this;
      }
    }

    public <U> Node<K, U> mapVals(Object editor, BiFunction<K, V, U> f) {
      Node n = clone(editor);
      for (int i = bitCount(n.datamap) - 1; i >= 0; i--) {
        int idx = i << 1;
        n.content[idx + 1] = f.apply((K) n.content[idx], (V) n.content[idx + 1]);
      }

      for (int i = content.length - bitCount(n.nodemap); i < content.length; i++) {
        n.content[i] = ((INode<K, V>) n.content[i]).mapVals(editor, f);
      }

      return n;
    }


    // iteration

    public Iterator<IEntry<K, V>> iterator() {

      return new Iterator<IEntry<K, V>>() {

        final Node[] stack = new Node[7];
        final byte[] cursors = new byte[14];
        int depth = 0;

        {
          stack[0] = Node.this;
          cursors[1] = (byte) bitCount(Node.this.nodemap);
        }

        Object[] content = Node.this.content;
        int idx = 0;
        int limit = bitCount(Node.this.datamap) << 1;

        private boolean nextNode() {

          while (depth >= 0) {

            int pos = depth << 1;
            int idx = cursors[pos];
            int limit = cursors[pos + 1];

            if (idx < limit) {
              Node<K, V> curr = stack[depth];
              INode<K, V> next = (INode<K, V>) curr.content[curr.content.length - 1 - idx];
              cursors[pos]++;

              if (next instanceof Node) {
                Node<K, V> n = (Node<K, V>) next;

                if (n.nodemap != 0) {
                  stack[++depth] = n;
                  cursors[pos + 2] = 0;
                  cursors[pos + 3] = (byte) bitCount(n.nodemap);
                }

                if (n.datamap != 0) {
                  this.content = n.content;
                  this.idx = 0;
                  this.limit = bitCount(n.datamap) << 1;

                  return true;
                }

              } else {
                Collision<K, V> c = (Collision<K, V>) next;
                this.content = c.entries;
                this.idx = 0;
                this.limit = c.entries.length;

                return true;
              }
            } else {
              depth--;
            }
          }

          return false;
        }

        @Override
        public boolean hasNext() {
          return idx < limit || nextNode();
        }

        @Override
        public IEntry<K, V> next() {
          if (idx >= limit) {
            if (!nextNode()) {
              throw new NoSuchElementException();
            }
          }

          IEntry<K, V> e = new Maps.Entry<>((K) content[idx], (V) content[idx + 1]);
          idx += 2;
          return e;
        }
      };
    }

    @Override
    public Iterable<IEntry<K, V>> entries() {
      return () ->
        Iterators.range(bitCount(datamap),
          i -> {
            int idx = (int) (i << 1);
            return new Maps.Entry<>((K) content[idx], (V) content[idx + 1]);
          });
    }

    // misc

    @Override
    public long size() {
      return size;
    }

    public boolean equals(INode<K, V> o, BiPredicate<K, K> keyEquals, BiPredicate<V, V> valEquals) {

      if (this == o) {
        return true;
      }

      if (o instanceof Node) {
        Node<K, V> n = (Node<K, V>) o;
        if (n.size == size && n.datamap == datamap && n.nodemap == nodemap) {
          Iterator<IEntry<K, V>> ea = entries().iterator();
          Iterator<IEntry<K, V>> eb = n.entries().iterator();
          while (ea.hasNext()) {
            if (!ea.next().equals(eb.next(), keyEquals, valEquals)) {
              return false;
            }
          }

          Iterator<INode<K, V>> na = nodes().iterator();
          Iterator<INode<K, V>> nb = n.nodes().iterator();
          while (na.hasNext()) {
            if (!na.next().equals(nb.next(), keyEquals, valEquals)) {
              return false;
            }
          }

          return true;
        }
      }

      return false;
    }

    /////

    private Node<K, V> clone(Object editor) {
      Node<K, V> node = new Node<>();
      node.datamap = datamap;
      node.nodemap = nodemap;
      node.hashes = hashes.clone();
      node.content = content.clone();
      node.editor = editor;
      node.size = size;

      return node;
    }

    private Iterable<INode<K, V>> nodes() {
      return () -> Iterators.range(0, bitCount(nodemap), i -> (INode<K, V>) content[content.length - 1 - (int) i]);
    }

    private INode<K, V> collapse(int shift) {
      return (shift > 0
        && datamap == 0
        && Bits.isPowerOfTwo(nodemap)
        && node(nodemap) instanceof Collision)
        ? node(nodemap)
        : this;
    }

    private void grow() {
      if (content.length == 64) {
        return;
      }

      Object[] c = new Object[content.length << 1];
      int[] h = new int[hashes.length << 1];
      int numNodes = bitCount(nodemap);
      int numEntries = bitCount(datamap);

      arraycopy(content, 0, c, 0, numEntries << 1);
      arraycopy(content, content.length - numNodes, c, c.length - numNodes, numNodes);
      arraycopy(hashes, 0, h, 0, numEntries);

      this.hashes = h;
      this.content = c;
    }

    Node<K, V> putEntry(int mask, int hash, K key, V value) {
      int numEntries = bitCount(datamap);
      int count = (numEntries << 1) + bitCount(nodemap);
      if ((count + 2) > content.length) {
        grow();
      }

      final int idx = entryIndex(mask);
      final int entryIdx = idx << 1;
      if (idx != numEntries) {
        arraycopy(content, entryIdx, content, entryIdx + 2, (numEntries - idx) << 1);
        arraycopy(hashes, idx, hashes, idx + 1, numEntries - idx);
      }
      datamap |= mask;
      size++;

      hashes[idx] = hash;
      content[entryIdx] = key;
      content[entryIdx + 1] = value;

      return this;
    }

    Node<K, V> removeEntry(final int mask) {
      // shrink?

      final int idx = entryIndex(mask);
      final int numEntries = bitCount(datamap);
      if (idx != numEntries - 1) {
        arraycopy(content, (idx + 1) << 1, content, idx << 1, (numEntries - 1 - idx) << 1);
        arraycopy(hashes, idx + 1, hashes, idx, numEntries - 1 - idx);
      }
      datamap &= ~mask;
      size--;

      int entryIdx = (numEntries - 1) << 1;
      content[entryIdx] = null;
      content[entryIdx + 1] = null;

      return this;
    }

    Node<K, V> setNode(int mask, INode<K, V> node, long sizeDelta) {
      content[content.length - 1 - nodeIndex(mask)] = node;
      size += sizeDelta;
      return this;
    }

    Node<K, V> putNode(final int mask, INode<K, V> node) {
      if (node.size() == 1) {
        IEntry<K, V> e = node.nth(0);
        return putEntry(mask, node.hash(0), e.key(), e.value());
      } else {
        int count = (bitCount(datamap) << 1) + bitCount(nodemap);
        if ((count + 1) > content.length) {
          grow();
        }

        int idx = nodeIndex(mask);
        int numNodes = bitCount(nodemap);
        if (numNodes > 0) {
          arraycopy(content, content.length - numNodes, content, content.length - 1 - numNodes, numNodes - idx);
        }
        nodemap |= mask;
        size += node.size();

        content[content.length - 1 - idx] = node;

        return this;
      }
    }

    Node<K, V> removeNode(final int mask, long nodeSize) {
      // shrink?

      int idx = nodeIndex(mask);
      int numNodes = bitCount(nodemap);
      size -= nodeSize;
      arraycopy(content, content.length - numNodes, content, content.length + 1 - numNodes, numNodes - 1 - idx);
      nodemap &= ~mask;

      content[content.length - numNodes] = null;

      return this;
    }

    private int entryIndex(int mask) {
      return compressedIndex(datamap, mask);
    }

    private int nodeIndex(int mask) {
      return compressedIndex(nodemap, mask);
    }

    private INode<K, V> node(int mask) {
      return (INode<K, V>) content[content.length - 1 - nodeIndex(mask)];
    }

    private boolean isEntry(int mask) {
      return (datamap & mask) != 0;
    }

    private boolean isNode(int mask) {
      return (nodemap & mask) != 0;
    }
  }

  public static class Collision<K, V> implements INode<K, V> {

    public final int hash;
    public final Object[] entries;

    // constructors

    public Collision(int hash, K k1, V v1, K k2, V v2) {
      this(hash, new Object[]{k1, v1, k2, v2});
    }

    private Collision(int hash, Object[] entries) {
      this.hash = hash;
      this.entries = entries;
    }

    // lookup

    public boolean contains(int hash, K key, BiPredicate<K, K> equals) {
      return get(hash, key, equals, DEFAULT_VALUE) != DEFAULT_VALUE;
    }

    public Object get(int hash, K key, BiPredicate<K, K> equals, Object defaultValue) {
      if (hash != this.hash) {
        return defaultValue;
      }

      int idx = indexOf(key, equals);
      if (idx < 0) {
        return defaultValue;
      } else {
        return entries[idx + 1];
      }
    }

    @Override
    public int hash(int idx) {
      return hash;
    }

    @Override
    public IEntry<K, V> nth(long idx) {
      int i = (int) idx << 1;
      return new Maps.Entry<>((K) entries[i], (V) entries[i + 1]);
    }

    @Override
    public long indexOf(int shift, int hash, K key, BiPredicate<K, K> equals) {
      if (this.hash == hash) {
        for (int i = 0; i < entries.length; i += 2) {
          if (equals.test(key, (K) entries[i])) {
            return i >> 1;
          }
        }
      }

      return -1;
    }

    // update

    @Override
    public INode<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, BinaryOperator<V> merge) {
      if (hash != this.hash) {
        return new Node<K, V>(editor)
          .putNode(hashMask(this.hash, shift), this)
          .put(shift, editor, hash, key, value, equals, merge);
      } else {
        int idx = indexOf(key, equals);
        return idx < 0
          ? new Collision<K, V>(hash, ArrayVector.append(entries, key, value))
          : new Collision<K, V>(hash, ArrayVector.set(entries, idx, key, merge.apply((V) entries[idx + 1], value)));
      }
    }

    @Override
    public INode<K, V> remove(int shift, Object editor, int hash, K key, BiPredicate<K, K> equals) {
      if (hash != this.hash) {
        return this;
      } else {
        int idx = indexOf(key, equals);
        return idx < 0 ? this : new Collision<K, V>(hash, ArrayVector.remove(entries, idx, 2));
      }
    }

    public <U> Collision<K, U> mapVals(Object editor, BiFunction<K, V, U> f) {
      Collision c = new Collision(hash, entries.clone());
      for (int i = 0; i < entries.length; i += 2) {
        c.entries[i + 1] = f.apply((K) c.entries[i], (V) c.entries[i + 1]);
      }

      return c;
    }


    // iteration

    public Iterable<IEntry<K, V>> entries() {
      return () ->
        Iterators.range(entries.length >> 1,
          i -> {
            int idx = (int) (i << 1);
            return new Maps.Entry<>((K) entries[idx], (V) entries[idx + 1]);
          });
    }

    // misc

    public long size() {
      return entries.length >> 1;
    }

    @Override
    public boolean equals(INode<K, V> o, BiPredicate<K, K> keyEquals, BiPredicate<V, V> valEquals) {

      if (this == o) {
        return true;
      }

      if (o instanceof Collision) {
        Collision<K, V> c = (Collision<K, V>) o;
        if (c.size() == size()) {

          Iterator<IEntry<K, V>> it = entries().iterator();
          while (it.hasNext()) {
            IEntry<K, V> e = it.next();
            int idx = c.indexOf(e.key(), keyEquals);
            if (idx < 0 || !valEquals.test(e.value(), (V) entries[idx + 1])) {
              return false;
            }
          }
          return true;
        }
      }

      return false;
    }

    ///

    private int indexOf(K key, BiPredicate<K, K> equals) {
      for (int i = 0; i < entries.length; i += 2) {
        if (equals.test(key, (K) entries[i])) {
          return i;
        }
      }
      return -1;
    }
  }

  ///

  private static int hashMask(int hash, int shift) {
    return 1 << ((hash >>> shift) & 31);
  }

  public static <K, V> boolean contains(Node<K, V> node, int shift, int hash, K key, BiPredicate<K, K> equals) {
    return get(node, shift, hash, key, equals, DEFAULT_VALUE) != DEFAULT_VALUE;
  }

  public static <K, V> Object get(Node<K, V> node, int shift, int hash, K key, BiPredicate<K, K> equals, Object defaultValue) {

    Object currNode = node;
    while (!(currNode instanceof Collision)) {

      Node<K, V> n = (Node<K, V>) currNode;
      int mask = hashMask(hash, shift);

      // there's a potential matching entry
      if (n.isEntry(mask)) {
        int idx = n.entryIndex(mask) << 1;
        return /*n.hashes[idx] == hash &&*/ equals.test(key, (K) n.content[idx])
          ? n.content[idx + 1]
          : defaultValue;

        // we must go deeper
      } else if (n.isNode(mask)) {
        currNode = n.node(mask);
        shift += SHIFT_INCREMENT;

        // no such thing
      } else {
        return defaultValue;
      }
    }

    Collision<K, V> c = (Collision<K, V>) currNode;
    return c.get(hash, key, equals, defaultValue);
  }

  /// Set operations

  public static <K, V> INode<K, V> mergeNodes(int shift, Object editor, INode<K, V> a, INode<K, V> b, BiPredicate<K, K> equals, BinaryOperator<V> merge) {
    Collision<K, V> ca, cb;
    Node<K, V> na, nb;

    // Node / Node
    if (a instanceof Node && b instanceof Node) {
      return merge(shift, editor, (Node<K, V>) a, (Node<K, V>) b, equals, merge);

      // Node / Collision
    } else if (a instanceof Node && b instanceof Collision) {
      na = (Node<K, V>) a;
      cb = (Collision<K, V>) b;
      for (IEntry<K, V> e : cb.entries()) {
        na = na.put(shift, editor, cb.hash, e.key(), e.value(), equals, merge);
      }
      return na;

      // Collision / Node
    } else if (a instanceof Collision && b instanceof Node) {
      BinaryOperator<V> inverted = (x, y) -> merge.apply(y, x);
      ca = (Collision<K, V>) a;
      nb = (Node<K, V>) b;
      for (IEntry<K, V> e : ca.entries()) {
        nb = nb.put(shift, editor, ca.hash, e.key(), e.value(), equals, inverted);
      }
      return nb;

      // Collision / Collision
    } else {
      cb = (Collision<K, V>) b;
      for (IEntry<K, V> e : cb.entries()) {
        a = a.put(shift, editor, cb.hash, e.key(), e.value(), equals, merge);
      }
      return a;
    }
  }

  public static <K, V> Node<K, V> merge(int shift, Object editor, Node<K, V> a, Node<K, V> b, BiPredicate<K, K> equals, BinaryOperator<V> merge) {
    Node<K, V> result = new Node<K, V>(editor);

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
          result = (Node<K, V>) result.put(shift, editor, b.hash(idx), (K) b.content[idx << 1], (V) b.content[(idx << 1) + 1], equals, merge);
          break;
        case NODE_NODE:
          result = result.putNode(mask, mergeNodes(shift + 5, editor, a.node(mask), b.node(mask), equals, merge));
          break;
        case NODE_ENTRY:
          idx = b.entryIndex(mask);
          result = (Node<K, V>) result
            .putNode(mask, a.node(mask))
            .put(shift, editor, b.hash(idx), (K) b.content[idx << 1], (V) b.content[(idx << 1) + 1], equals, merge);
          break;
        case ENTRY_NODE:
          idx = a.entryIndex(mask);
          result = (Node<K, V>) result
            .putNode(mask, b.node(mask))
            .put(shift, editor, a.hash(idx), (K) a.content[idx << 1], (V) a.content[(idx << 1) + 1], equals, (x, y) -> merge.apply(y, x));
          break;
        case NONE_NONE:
          break;
      }
    }

    return result;
  }

  public static <K, V> INode<K, V> diffNodes(int shift, Object editor, INode<K, V> a, INode<K, V> b, BiPredicate<K, K> equals) {
    Collision<K, V> ca, cb;
    Node<K, V> na, nb;

    // Node / Node
    if (a instanceof Node && b instanceof Node) {
      return difference(shift, editor, (Node<K, V>) a, (Node<K, V>) b, equals);

      // Node / Collision
    } else if (a instanceof Node && b instanceof Collision) {
      cb = (Collision<K, V>) b;
      for (IEntry<K, V> e : cb.entries()) {
        a = a.remove(shift, editor, cb.hash, e.key(), equals);
      }
      return a.size() > 0 ? a : null;

      // Collision / Node
    } else if (a instanceof Collision && b instanceof Node) {
      ca = (Collision<K, V>) a;
      nb = (Node<K, V>) b;
      for (IEntry<K, V> e : ca.entries()) {
        if (get(nb, shift, ca.hash, e.key(), equals, DEFAULT_VALUE) != DEFAULT_VALUE) {
          ca = (Collision<K, V>) ca.remove(shift, editor, ca.hash, e.key(), equals);
        }
      }
      return ca.size() > 0 ? ca : null;

      // Collision / Collision
    } else {
      ca = (Collision<K, V>) a;
      cb = (Collision<K, V>) b;
      if (ca.hash == cb.hash) {
        for (IEntry<K, V> e : cb.entries()) {
          ca = (Collision<K, V>) ca.remove(shift, editor, ca.hash, e.key(), equals);
        }
      }
      return ca.size() > 0 ? ca : null;
    }
  }

  public static <K, V> Node<K, V> difference(int shift, Object editor, Node<K, V> a, Node<K, V> b, BiPredicate<K, K> equals) {
    Node<K, V> result = new Node<K, V>(editor);

    INode<K, V> n;
    int idx;
    PrimitiveIterator.OfInt masks = Util.masks(a.nodemap | a.datamap | b.nodemap | b.datamap);
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
          int ia = a.entryIndex(mask);
          int ib = b.entryIndex(mask);
          if (b.hashes[ib] != a.hashes[ia] || !equals.test((K) b.content[ib << 1], (K) a.content[ia << 1])) {
            result = transferEntry(mask, a, result);
          }
          break;
        case NODE_NODE:
          n = diffNodes(shift + 5, editor, a.node(mask), b.node(mask), equals);
          if (n != null) {
            result = result.putNode(mask, n);
          }
          break;
        case NODE_ENTRY:
          idx = b.entryIndex(mask);
          n = a.node(mask).remove(shift + 5, editor, b.hashes[idx], (K) b.content[idx << 1], equals);
          if (n.size() > 0) {
            result = result.putNode(mask, n);
          }
          break;
        case ENTRY_NODE:
          idx = a.entryIndex(mask);
          if (get(b, shift, a.hashes[idx], (K) a.content[idx << 1], equals, DEFAULT_VALUE) == DEFAULT_VALUE) {
            result = transferEntry(mask, a, result);
          }
          break;
        case NONE_ENTRY:
        case NONE_NODE:
        case NONE_NONE:
          break;
      }
    }

    return result.size() > 0 ? result : null;
  }

  public static <K, V> INode<K, V> intersectNodes(int shift, Object editor, INode<K, V> a, INode<K, V> b, BiPredicate<K, K> equals) {
    Collision<K, V> ca, cb;
    Node<K, V> na, nb;

    // Node / Node
    if (a instanceof Node && b instanceof Node) {
      return intersection(shift, editor, (Node<K, V>) a, (Node<K, V>) b, equals);

      // Node / Collision
    } else if (a instanceof Node && b instanceof Collision) {
      cb = (Collision<K, V>) b;
      na = (Node<K, V>) a;
      Collision<K, V> result = new Collision<K, V>(cb.hash, new Object[0]);
      for (IEntry<K, V> e : b.entries()) {
        Object val = get(na, shift, cb.hash, e.key(), equals, DEFAULT_VALUE);
        if (val != DEFAULT_VALUE) {
          result = (Collision<K, V>) result.put(shift, editor, cb.hash, e.key(), (V) val, equals, null);
        }
      }
      return result.size() > 0 ? result : null;

      // Collision / Node
    } else if (a instanceof Collision && b instanceof Node) {

      ca = (Collision<K, V>) a;
      nb = (Node<K, V>) b;

      for (IEntry<K, V> e : ca.entries()) {
        if (!contains(nb, shift, ca.hash, e.key(), equals)) {
          ca = (Collision<K, V>) ca.remove(shift, editor, ca.hash, e.key(), equals);
        }
      }
      return ca.size() > 0 ? ca : null;

      // Collision / Collision
    } else {
      ca = (Collision<K, V>) a;
      cb = (Collision<K, V>) b;
      if (ca.hash != cb.hash) {
        return null;
      }

      for (IEntry<K, V> e : ca.entries()) {
        if (!cb.contains(ca.hash, e.key(), equals)) {
          ca = (Collision<K, V>) ca.remove(shift, editor, ca.hash, e.key(), equals);
        }
      }
      return ca.size() > 0 ? ca : null;
    }
  }

  public static <K, V> Node<K, V> intersection(int shift, Object editor, Node<K, V> a, Node<K, V> b, BiPredicate<K, K> equals) {
    Node<K, V> result = new Node<K, V>(editor);

    PrimitiveIterator.OfInt masks = Util.masks(a.nodemap | a.datamap | b.nodemap | b.datamap);
    while (masks.hasNext()) {
      int mask = masks.nextInt();
      int state = mergeState(mask, a.nodemap, a.datamap, b.nodemap, b.datamap);
      int idx, ia, ib;
      switch (state) {
        case ENTRY_ENTRY:
          ia = a.entryIndex(mask);
          ib = b.entryIndex(mask);
          if (b.hashes[ib] == a.hashes[ia] && equals.test((K) b.content[ib << 1], (K) a.content[ia << 1])) {
            result = transferEntry(mask, a, result);
          }
          break;
        case NODE_NODE:
          INode<K, V> n = intersectNodes(shift + 5, editor, a.node(mask), b.node(mask), equals);
          if (n != null) {
            result = result.putNode(mask, n);
          }
          break;
        case NODE_ENTRY:
          idx = b.entryIndex(mask);
          int hash = b.hashes[idx];
          K key = (K) b.content[idx << 1];
          Object val = get(a, shift, hash, key, equals, DEFAULT_VALUE);
          if (val != DEFAULT_VALUE) {
            result = result.put(shift, editor, hash, key, (V) val, equals, null);
          }
          break;
        case ENTRY_NODE:
          idx = a.entryIndex(mask);
          if (get(b, shift, a.hashes[idx], (K) a.content[idx << 1], equals, DEFAULT_VALUE) != DEFAULT_VALUE) {
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

    return result.size() > 0 ? result : null;
  }

  public static <K, V> IList<Node<K, V>> split(Object editor, Node<K, V> node, int targetSize) {
    IList<Node<K, V>> result = new LinearList<>();
    if ((node.size() >> 1) < targetSize) {
      result.addLast(node);
    } else {
      Node<K, V> acc = new Node<>(editor);

      PrimitiveIterator.OfInt masks = Util.masks(node.datamap | node.nodemap);
      while (masks.hasNext()) {
        int mask = masks.nextInt();

        if (acc.size() >= targetSize) {
          result.addLast(acc);
          acc = new Node<>(editor);
        }

        if (node.isEntry(mask)) {
          acc = transferEntry(mask, node, acc);
        } else if (node.isNode(mask)) {
          INode<K, V> child = node.node(mask);
          if (child instanceof Node && child.size() >= (targetSize << 1)) {
            split(editor, (Node<K, V>) child, targetSize).stream()
              .map(n -> new Node<K, V>(editor).putNode(mask, n))
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

  private static <K, V> Node<K, V> transferNode(int mask, Node<K, V> src, Node<K, V> dst) {
    return dst.putNode(mask, src.node(mask));
  }

  private static <K, V> Node<K, V> transferEntry(int mask, Node<K, V> src, Node<K, V> dst) {
    int idx = src.entryIndex(mask);
    return dst.putEntry(mask, src.hashes[idx], (K) src.content[idx << 1], (V) src.content[(idx << 1) + 1]);
  }
}
