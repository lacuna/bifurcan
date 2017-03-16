package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IMap.IEntry;
import io.lacuna.bifurcan.utils.ArrayVector;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;

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

  static class PutCommand<K, V> {
    public final Object editor;
    public final int hash;
    public final K key;
    public final V value;
    public final BiPredicate<K, K> equals;
    public final BinaryOperator<V> merge;

    public PutCommand(Object editor, int hash, K key, V value, BiPredicate<K, K> equals, BinaryOperator<V> merge) {
      this.editor = editor;
      this.hash = hash;
      this.key = key;
      this.value = value;
      this.equals = equals;
      this.merge = merge;
    }

    public PutCommand(PutCommand<K, V> c, int hash, K key, V value) {
      this.editor = c.editor;
      this.hash = hash;
      this.key = key;
      this.value = value;
      this.equals = c.equals;
      this.merge = c.merge;
    }
  }

  static class RemoveCommand<K, V> {
    public final Object editor;
    public final int hash;
    public final K key;
    public final BiPredicate<K, K> equals;

    public RemoveCommand(Object editor, int hash, K key, BiPredicate<K, K> equals) {
      this.editor = editor;
      this.hash = hash;
      this.key = key;
      this.equals = equals;
    }
  }

  interface INode<K, V> {

    default INode<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, BinaryOperator<V> merge) {
      return put(shift, new PutCommand<>(editor, hash, key, value, equals, merge));
    }

    INode<K, V> put(int shift, PutCommand<K, V> command);

    default INode<K, V> remove(int shift, Object editor, int hash, K key, BiPredicate<K, K> equals) {
      return remove(shift, new RemoveCommand<>(editor, hash, key, equals));
    }

    INode<K, V> remove(int shift, RemoveCommand<K, V> command);

    Object get(int shift, int hash, K key, BiPredicate<K, K> equals, Object defaultValue);

    int hash(int idx);

    long size();

    IEntry<K, V> nth(long idx);

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

    public Node() {
    }

    Node(Object editor) {
      this.editor = editor;
      this.hashes = new int[2];
      this.content = new Object[4];
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public IEntry<K, V> nth(long idx) {

      // see if the entry is local to this node
      int entries = Integer.bitCount(datamap);
      if (idx < entries) {
        K key = (K) content[(int) idx << 1];
        V val = (V) content[((int) idx << 1) + 1];
        return new Maps.Entry<>(key, val);
      }

      // see if the entry is local to our children
      idx -= entries;
      if (idx < size) {
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
    public Iterable<IEntry<K, V>> entries() {
      return () ->
          Iterators.range(bitCount(datamap),
              i -> {
                int idx = (int) (i << 1);
                return new Maps.Entry<>((K) content[idx], (V) content[idx + 1]);
              });
    }

    @Override
    public Object get(int shift, int hash, K key, BiPredicate<K, K> equals, Object defaultValue) {
      int mask = hashMask(hash, shift);

      // there's a potential matching entry
      if (isEntry(mask)) {
        int idx = entryIndex(mask);
        return hashes[idx] == hash && equals.test(key, (K) content[idx << 1])
            ? content[(idx << 1) + 1]
            : defaultValue;

        // we must go deeper
      } else if (isNode(mask)) {
        return node(mask).get(shift + SHIFT_INCREMENT, hash, key, equals, defaultValue);

        // no such thing
      } else {
        return defaultValue;
      }
    }

    @Override
    public int hash(int idx) {
      return hashes[idx];
    }

    // this is factored out of `put` for greater inlining joy
    private Node<K, V>  mergeEntry(int shift, int mask, PutCommand<K, V> c) {
      int idx = entryIndex(mask);

      // there's a match
      boolean collision = c.hash == hashes[idx];
      if (collision && c.equals.test(c.key, (K) content[idx << 1])) {

        Node<K, V> n = (c.editor == editor ? this : clone(c.editor));
        idx = (idx << 1) + 1;
        n.content[idx] = c.merge.apply((V) n.content[idx], c.value);
        return n;

        // collision, put them both in a node together
      } else {
        K key = (K) content[idx << 1];
        V value = (V) content[(idx << 1) + 1];

        INode<K, V> node;
        if (collision) {
          node = new Collision<K, V>(c.hash, key, value, c.key, c.value);
        } else {
          node = new Node<K, V>(c.editor)
              .put(shift + SHIFT_INCREMENT, new PutCommand<>(c, hashes[idx], key, value))
              .put(shift + SHIFT_INCREMENT, c);
        }

        return (c.editor == editor ? this : clone(c.editor)).removeEntry(mask).putNode(mask, node);
      }
    }

    @Override
    public Node<K, V> put(int shift, PutCommand<K, V> c) {
      int mask = hashMask(c.hash, shift);

      // overwrite potential collision
      if (isEntry(mask)) {
        return mergeEntry(shift, mask, c);

        // we have to go deeper
      } else if (isNode(mask)) {
        INode<K, V> node = node(mask);
        long prevSize = node.size();
        INode<K, V> nodePrime = node.put(shift + SHIFT_INCREMENT, c);

        return (c.editor == editor ? this : clone(c.editor)).setNode(mask, nodePrime, nodePrime.size() - prevSize);

        // no existing entry
      } else {
        return (c.editor == editor ? this : clone(c.editor)).putEntry(mask, c.hash, c.key, c.value);
      }
    }

    @Override
    public INode<K, V> remove(int shift, RemoveCommand<K, V> c) {
      int mask = hashMask(c.hash, shift);

      // there's a potential match
      if (isEntry(mask)) {
        int idx = entryIndex(mask);

        // there is a match
        if (hashes[idx] == c.hash && c.equals.test(c.key, (K) content[idx << 1])) {
          return (c.editor == editor ? this : clone(c.editor)).removeEntry(mask).collapse(shift);

          // nope
        } else {
          return this;
        }

        // we must go deeper
      } else if (isNode(mask)) {
        INode<K, V> node = node(mask);
        long prevSize = node.size();
        INode<K, V> nodePrime = node.remove(shift + SHIFT_INCREMENT, c);

        Node<K, V> n = c.editor == editor ? this : clone(c.editor);
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

    public Iterator<IEntry<K, V>> iterator() {

      return new Iterator<IEntry<K, V>>() {

        final LinearList<INode<K, V>> nodes = LinearList.from(nodes());
        Iterator<IEntry<K, V>> iterator = entries().iterator();

        @Override
        public boolean hasNext() {
          return iterator.hasNext() || nodes.size() > 0;
        }

        @Override
        public IEntry<K, V> next() {
          while (!iterator.hasNext()) {
            INode<K, V> node = nodes.popFirst();
            iterator = node.entries().iterator();
            if (node instanceof Node) {
              ((Node<K, V>) node).nodes().forEach(n -> nodes.addLast(n));
            }
          }

          return iterator.next();
        }
      };
    }

    public boolean equals(INode<K, V> o, BiPredicate<K, K> keyEquals, BiPredicate<V, V> valEquals) {
      if (o instanceof Node) {
        Node<K, V> n = (Node<K, V>) o;
        if (n.datamap == datamap && n.nodemap == nodemap) {
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
      return () ->
          Iterators.range(
              content.length - Integer.bitCount(nodemap),
              content.length,
              i -> (INode<K, V>) content[(int) i]);
    }

    private INode<K, V> collapse(int shift) {
      return (shift > 0 && datamap == 0 && Bits.isPowerOfTwo(nodemap) && node(nodemap) instanceof Collision)
          ? node(nodemap)
          : this;
    }

    private void grow() {
      Object[] c = new Object[content.length << 1];
      int numNodes = bitCount(nodemap);
      arraycopy(content, 0, c, 0, bitCount(datamap) << 1);
      arraycopy(content, content.length - numNodes, c, c.length - numNodes, numNodes);
      this.content = c;

      int[] h = new int[hashes.length << 1];
      arraycopy(hashes, 0, h, 0, bitCount(datamap));
      this.hashes = h;
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

    public Collision(int hash, K k1, V v1, K k2, V v2) {
      this(hash, new Object[]{k1, v1, k2, v2});
    }

    private Collision(int hash, Object[] entries) {
      this.hash = hash;
      this.entries = entries;
    }

    private int indexOf(K key, BiPredicate<K, K> equals) {
      for (int i = 0; i < entries.length; i += 2) {
        if (equals.test(key, (K) entries[i])) {
          return i;
        }
      }
      return -1;
    }

    @Override
    public INode<K, V> put(int shift, PutCommand<K, V> c) {
      if (c.hash != hash) {
        return new Node<K, V>(c.editor).putNode(hashMask(hash, shift), this).put(shift, c);
      } else {
        int idx = indexOf(c.key, c.equals);
        return idx < 0
            ? new Collision<K, V>(hash, ArrayVector.append(entries, c.key, c.value))
            : new Collision<K, V>(hash, ArrayVector.set(entries, idx, c.key, c.merge.apply((V) entries[idx + 1], c.value)));
      }
    }

    @Override
    public Object get(int shift, int hash, K key, BiPredicate<K, K> equals, Object defaultValue) {
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
    public INode<K, V> remove(int shift, RemoveCommand<K, V> c) {
      int idx = indexOf(c.key, c.equals);
      if (idx < 0) {
        return this;
      } else {
        return new Collision<K, V>(hash, ArrayVector.remove(entries, idx, 2));
      }
    }

    public long size() {
      return entries.length >> 1;
    }

    @Override
    public IEntry<K, V> nth(long idx) {
      int i = (int) idx << 1;
      return new Maps.Entry<>((K) entries[i], (V) entries[i + 1]);
    }

    @Override
    public boolean equals(INode<K, V> o, BiPredicate<K, K> keyEquals, BiPredicate<V, V> valEquals) {
      if (o instanceof Collision) {
        Collision<K, V> n = (Collision<K, V>) o;
        if (n.size() == size()) {
          Iterator<IEntry<K, V>> it = entries().iterator();
          while (it.hasNext()) {
            IEntry<K, V> e = it.next();
            int idx = n.indexOf(e.key(), keyEquals);
            if (idx < 0 || !valEquals.test(e.value(), (V) entries[idx + 1])) {
              return false;
            }
          }

          return true;
        }
      }

      return false;
    }

    public Iterable<IEntry<K, V>> entries() {
      return () ->
          Iterators.range(entries.length >> 1,
              i -> {
                int idx = (int) (i << 1);
                return new Maps.Entry<>((K) entries[idx], (V) entries[idx + 1]);
              });
    }
  }

  ///

  private static int hashMask(int hash, int shift) {
    return 1 << ((hash >>> shift) & 31);
  }

  public static <K, V> INode<K, V> mergeNodes(int shift, Object editor, INode<K, V> a, INode<K, V> b, BiPredicate<K, K> equals, BinaryOperator<V> merge) {
    Collision<K, V> ca, cb;
    Node<K, V> na, nb;

    // Node / Node
    if (a instanceof Node && b instanceof Node) {
      return merge(shift + 5, editor, (Node<K, V>) a, (Node<K, V>) b, equals, merge);

      // Node / Collision
    } else if (a instanceof Node && b instanceof Collision) {
      na = (Node<K, V>) a;
      cb = (Collision<K, V>) b;
      for (IEntry<K, V> e : cb.entries()) {
        na = (Node<K, V>) na.put(shift + 5, editor, cb.hash, e.key(), e.value(), equals, merge);
      }
      return na;

      // Collision / Node
    } else if (a instanceof Collision && b instanceof Node) {
      BinaryOperator<V> inverted = (x, y) -> merge.apply(y, x);
      ca = (Collision<K, V>) a;
      nb = (Node<K, V>) b;
      for (IEntry<K, V> e : ca.entries()) {
        nb = (Node<K, V>) nb.put(shift + 5, editor, ca.hash, e.key(), e.value(), equals, inverted);
      }
      return nb;

      // Collision / Collision
    } else {
      cb = (Collision<K, V>) b;
      for (IEntry<K, V> e : cb.entries()) {
        a = a.put(shift + 5, editor, cb.hash, e.key(), e.value(), equals, merge);
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
          result = transferEntry(mask, b, result);
          break;
        case NODE_NODE:
          result = result.putNode(mask, mergeNodes(shift, editor, a.node(mask), b.node(mask), equals, merge));
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

  public static <K, V> Node<K, V> difference(int shift, Object editor, Node<K, V> a, Node<K, V> b, BiPredicate<K, K> equals) {
    Node<K, V> result = new Node<K, V>(editor);

    PrimitiveIterator.OfInt masks = Util.masks(a.nodemap | a.datamap | b.nodemap | b.datamap);
    while (masks.hasNext()) {
      int mask = masks.nextInt();
      int state = mergeState(mask, a.nodemap, a.datamap, b.nodemap, b.datamap);
      int idx;
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

          // complicated
          result = transferNode(mask, null, result);
          break;
        case NODE_ENTRY:
          idx = b.entryIndex(mask);
          result = (Node<K, V>) a.node(mask).remove(shift + 5, editor, b.hashes[idx], (K) b.content[idx << 1], equals);
          break;
        case ENTRY_NODE:
          idx = a.entryIndex(mask);
          if (b.get(shift, a.hashes[idx], (K) a.content[idx << 1], equals, DEFAULT_VALUE) == DEFAULT_VALUE) {
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
          // complicated
          result = transferNode(mask, null, result);
          break;
        case NODE_ENTRY:
          idx = b.entryIndex(mask);
          int hash = b.hashes[idx];
          K key = (K) b.content[idx << 1];
          Object val = a.get(shift, hash, key, equals, DEFAULT_VALUE);
          if (val != DEFAULT_VALUE) {
            result = (Node<K, V>) result.put(shift, editor, hash, key, (V) val, equals, null);
          }
          break;
        case ENTRY_NODE:
          idx = a.entryIndex(mask);
          if (b.get(shift, a.hashes[idx], (K) a.content[idx << 1], equals, DEFAULT_VALUE) != DEFAULT_VALUE) {
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
