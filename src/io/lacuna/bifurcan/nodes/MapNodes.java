package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.ArrayVector;

import java.util.Iterator;
import java.util.function.BiPredicate;

import static java.lang.Integer.bitCount;
import static java.lang.System.arraycopy;

/**
 * This is an implementation based on the one described in https://michael.steindorfer.name/publications/oopsla15.pdf.
 *
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
    public final IMap.ValueMerger<V> merge;

    public PutCommand(Object editor, int hash, K key, V value, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge) {
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

    default INode<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge) {
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

    IMap.IEntry<K, V> nth(long idx);

    Iterator<IMap.IEntry<K, V>> entries();
  }

  private static final int NONE_NONE = 0;
  private static final int NODE_NONE = 0x1;
  private static final int ENTRY_NONE = 0x2;
  private static final int NONE_NODE = 0x4;
  private static final int NONE_ENTRY = 0x8;
  private static final int ENTRY_NODE = ENTRY_NONE | NONE_NODE;
  private static final int NODE_ENTRY = NODE_NONE | NONE_ENTRY;
  private static final int ENTRY_ENTRY = ENTRY_NONE | NONE_ENTRY;
  private static final int NODE_NODE = NODE_NONE | NONE_NODE;

  private static final Object DEFAULT_VALUE = new Object();

  private static int mergeState(int mask, int nodeA, int dataA, int nodeB, int dataB) {
    int state = 0;
    state |= (mask & nodeA) > 0 ? 0x1 : 0;
    state |= (mask & dataA) > 0 ? 0x2 : 0;
    state |= (mask & nodeB) > 0 ? 0x4 : 0;
    state |= (mask & dataB) > 0 ? 0x8 : 0;

    return state;
  }

  public static <K, V> Node<K, V> merge(int shift, Object editor, Node<K, V> a, Node<K, V> b, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge) {
    Node<K, V> result = new Node<K, V>(editor);
    for (int i = 0; i < 32; i++) {
      int mask = 1 << i;
      int state = mergeState(mask, a.nodemap, a.datamap, a.nodemap, b.datamap);
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
          // complicated
          result = transferNode(mask, null, result);
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
              .put(shift, editor, a.hash(idx), (K) a.content[idx << 1], (V) a.content[(idx << 1) + 1], equals, (x, y) -> merge.merge(y, x));
          break;
        case NONE_NONE:
          break;
      }
    }

    return result;
  }

  public static <K, V> Node<K, V> difference(int shift, Object editor, Node<K, V> a, Node<K, V> b, BiPredicate<K, K> equals) {
    Node<K, V> result = new Node<K, V>(editor);

    for (int i = 0; i < 32; i++) {
      int mask = 1 << i;
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

    for (int i = 0; i < 32; i++) {
      int mask = 1 << i;
      int state = mergeState(mask, a.nodemap, a.datamap, b.nodemap, b.datamap);
      int idx;
      switch (state) {
        case ENTRY_ENTRY:
          int ia = a.entryIndex(mask);
          int ib = b.entryIndex(mask);
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
          if (a.get(shift, b.hashes[idx], (K) b.content[idx << 1], equals, DEFAULT_VALUE) != DEFAULT_VALUE) {
            result = transferEntry(mask, b, result);
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

  private static <K, V> Node<K, V> transferNode(int mask, Node<K, V> src, Node<K, V> dst) {
    return dst.putNode(mask, src.node(mask));
  }

  private static <K, V> Node<K, V> transferEntry(int mask, Node<K, V> src, Node<K, V> dst) {
    int idx = src.entryIndex(mask);
    return dst.putEntry(mask, src.hashes[idx], (K) src.content[idx << 1], (V) src.content[(idx << 1) + 1]);
  }

  static class Collision<K, V> implements INode<K, V> {

    public final int hash;
    public final Object[] entries;

    public Collision(int hash, K k1, V v1, K k2, V v2) {
      this(hash, new Object[] {k1, v1, k2, v2});
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
        return new Node<K, V>(c.editor).putNode(Node.hashMask(hash, shift), this).put(shift, c);
      } else {
        int idx = indexOf(c.key, c.equals);
        return idx < 0
            ? new Collision<K, V>(hash, ArrayVector.append(entries, c.key, c.value))
            : new Collision<K, V>(hash, ArrayVector.set(entries, idx, c.key, c.merge.merge((V) entries[idx + 1], c.value)));
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
    public IMap.IEntry<K, V> nth(long idx) {
      int i = (int) idx << 1;
      return new Maps.Entry<>((K) entries[i], (V) entries[i + 1]);
    }

    public Iterator<IMap.IEntry<K, V>> entries() {
      return new Iterator<IMap.IEntry<K, V>>() {
        int idx = 0;
        @Override
        public boolean hasNext() {
          return idx < entries.length;
        }

        @Override
        public IMap.IEntry<K, V> next() {
          idx += 2;
          return new Maps.Entry<K, V>((K) entries[idx - 2], (V) entries[idx - 1]);
        }
      };
    }
  }

  public static class Node<K, V> implements INode<K, V> {

    public static final Node EMPTY = new Node(new Object());

    public static final int SHIFT_INCREMENT = 5;

    int datamap = 0;
    int nodemap = 0;
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
    public IMap.IEntry<K, V> nth(long idx) {

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
    public Iterator<IMap.IEntry<K, V>> entries() {
      int numEntries = bitCount(datamap);
      return new Iterator<IMap.IEntry<K, V>>() {

        int idx = 0;

        @Override
        public boolean hasNext() {
          return idx < numEntries;
        }

        @Override
        public IMap.IEntry<K, V> next() {
          int entryIdx = idx++ << 1;
          return new Maps.Entry<>((K) content[entryIdx], (V) content[entryIdx + 1]);
        }
      };
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
    private Node<K, V> mergeEntry(int shift, int mask, PutCommand<K, V> c) {
      int idx = entryIndex(mask);

      // there's a match
      boolean collision = c.hash == hashes[idx];
      if (collision && c.equals.test(c.key, (K) content[idx << 1])) {

        Node<K, V> n = (c.editor == editor ? this : clone(c.editor));
        idx = (idx << 1) + 1;
        n.content[idx] = c.merge.merge((V) n.content[idx], c.value);
        return n;

        // collision, put them both in a node together
      } else {
        K key = (K) content[idx << 1];
        V value = (V) content[(idx << 1) + 1];

        INode<K, V> node;
        if (shift < 30 && !collision) {
          node = new Node<K, V>(c.editor)
              .put(shift + SHIFT_INCREMENT, new PutCommand<>(c, hashes[idx], key, value))
              .put(shift + SHIFT_INCREMENT, c);
        } else {
          node = new Collision<K, V>(c.hash, key, value, c.key, c.value);
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

        if (node == nodePrime) {
          size += node.size() - prevSize;
          return this;
        } else {
          return (c.editor == editor ? this : clone(c.editor)).setNode(mask, nodePrime);
        }

        // no existing entry
      } else {
        return (c.editor == editor ? this : clone(c.editor)).putEntry(mask, c.hash, c.key, c.value);
      }
    }

    @Override
    public Node<K, V> remove(int shift, RemoveCommand<K, V> c) {
      int mask = hashMask(c.hash, shift);

      // there's a potential match
      if (isEntry(mask)) {
        int idx = entryIndex(mask);

        // there is a match
        if (hashes[idx] == c.hash && c.equals.test(c.key, (K) content[idx << 1])) {
          return (c.editor == editor ? this : clone(c.editor)).removeEntry(mask);

          // nope
        } else {
          return this;
        }

        // we must go deeper
      } else if (isNode(mask)) {
        INode<K, V> node = node(mask);
        long prevSize = node.size();
        INode<K, V> nodePrime = node.remove(shift + SHIFT_INCREMENT, c);

        if (node == nodePrime) {
          size += node.size() - prevSize;
          return this;
        } else {
          Node<K, V> n = c.editor == editor ? this : clone(c.editor);

          switch ((int) nodePrime.size()) {
            case 0:
              return n.removeNode(mask);
            case 1:
              IMap.IEntry<K, V> e = nodePrime.nth(0);
              return n.removeNode(mask).putEntry(mask, nodePrime.hash(0), e.key(), e.value());
            default:
              return n.setNode(mask, nodePrime);
          }
        }

        // no such thing
      } else {
        return this;
      }
    }

    public Iterator<IMap.IEntry<K, V>> iterator() {

      return new Iterator<IMap.IEntry<K, V>>() {

        final IList<INode<K, V>> nodes = LinearList.from(nodes());
        Iterator<IMap.IEntry<K, V>> iterator = entries();

        @Override
        public boolean hasNext() {
          return iterator.hasNext() || nodes.size() > 0;
        }

        @Override
        public IMap.IEntry<K, V> next() {
          while (!iterator.hasNext()) {
            INode<K, V> node = nodes.first();
            nodes.removeFirst();
            iterator = node.entries();
            if (node instanceof Node) {
              ((Node<K, V>) node).nodes().forEach(n -> nodes.addLast(n));
            }
          }

          return iterator.next();
        }
      };
    }

    public IList<Node<K, V>> split(int parts) {
      return null;
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
      return () -> new Iterator<INode<K, V>>() {
        int idx = content.length - Integer.bitCount(nodemap);

        @Override
        public boolean hasNext() {
          return idx < content.length;
        }

        @Override
        public INode<K, V> next() {
          return (INode<K, V>) content[idx++];
        }
      };
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

    Node<K, V> setNode(int mask, INode<K, V> node) {
      int idx = content.length - 1 - nodeIndex(mask);
      size += node.size() - ((INode<K, V>) content[idx]).size();
      content[idx] = node;

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

    Node<K, V> removeNode(final int mask) {
      // shrink?

      int idx = nodeIndex(mask);
      int numNodes = bitCount(nodemap);
      size -= node(mask).size();
      arraycopy(content, content.length - numNodes, content, content.length + 1 - numNodes, numNodes - 1 - idx);
      nodemap &= ~mask;

      content[content.length - numNodes] = null;

      return this;
    }

    static int compressedIndex(int bitmap, int hashMask) {
      return bitCount(bitmap & (hashMask - 1));
    }

    static int hashMask(int hash, int shift) {
      return 1 << ((hash >>> shift) & 31);
    }

    private int entryIndex(int hashMask) {
      return compressedIndex(datamap, hashMask);
    }

    private int nodeIndex(int hashMask) {
      return compressedIndex(nodemap, hashMask);
    }

    private INode<K, V> node(int hashMask) {
      return (INode<K, V>) content[content.length - 1 - nodeIndex(hashMask)];
    }

    private boolean isEntry(int hashMask) {
      return (datamap & hashMask) != 0;
    }

    private boolean isNode(int hashMask) {
      return (nodemap & hashMask) != 0;
    }
  }
}
