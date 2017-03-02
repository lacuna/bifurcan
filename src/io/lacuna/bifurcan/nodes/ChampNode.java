package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.IMap.IEntry;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.Maps;

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
public class ChampNode<K, V> implements IMapNode<K, V> {

  public static final ChampNode EMPTY = new ChampNode(new Object());

  public static final int SHIFT_INCREMENT = 5;

  int datamap = 0;
  int nodemap = 0;
  public int[] hashes;
  public Object[] content;
  Object editor;
  long size;

  public ChampNode() {
  }

  ChampNode(Object editor) {
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
      for (IMapNode<K, V> node : nodes()) {
        if (idx < node.size()) {
          return node.nth(idx);
        }
        idx -= node.size();
      }
    }

    throw new IndexOutOfBoundsException();
  }

  @Override
  public Iterator<IEntry<K, V>> entries() {
    int numEntries = bitCount(datamap);
    return new Iterator<IEntry<K, V>>() {

      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < numEntries;
      }

      @Override
      public IEntry<K, V> next() {
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
  private ChampNode<K, V> mergeEntry(int shift, int mask, PutCommand<K, V> c) {
    int idx = entryIndex(mask);

    // there's a match
    boolean collision = c.hash == hashes[idx];
    if (collision && c.equals.test(c.key, (K) content[idx << 1])) {

      ChampNode<K, V> n = (c.editor == editor ? this : clone(c.editor));
      idx = (idx << 1) + 1;
      n.content[idx] = c.merge.merge((V) n.content[idx], c.value);
      return n;

      // collision, put them both in a node together
    } else {
      K key = (K) content[idx << 1];
      V value = (V) content[(idx << 1) + 1];

      IMapNode<K, V> node;
      if (shift < 30 && !collision) {
        node = new ChampNode<K, V>(c.editor)
            .put(shift + SHIFT_INCREMENT, new PutCommand<>(c, hashes[idx], key, value))
            .put(shift + SHIFT_INCREMENT, c);
      } else {
        node = new CollisionNode<K, V>(c.hash, key, value, c.key, c.value);
      }

      return (c.editor == editor ? this : clone(c.editor)).removeEntry(mask).putNode(mask, node);
    }
  }

  @Override
  public ChampNode<K, V> put(int shift, PutCommand<K, V> c) {
    int mask = hashMask(c.hash, shift);

    // overwrite potential collision
    if (isEntry(mask)) {
      return mergeEntry(shift, mask, c);

      // we have to go deeper
    } else if (isNode(mask)) {
      IMapNode<K, V> node = node(mask);
      long prevSize = node.size();
      IMapNode<K, V> nodePrime = node.put(shift + SHIFT_INCREMENT, c);

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
  public ChampNode<K, V> remove(int shift, RemoveCommand<K, V> c) {
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
      IMapNode<K, V> node = node(mask);
      long prevSize = node.size();
      IMapNode<K, V> nodePrime = node.remove(shift + SHIFT_INCREMENT, c);

      if (node == nodePrime) {
        size += node.size() - prevSize;
        return this;
      } else {
        ChampNode<K, V> n = c.editor == editor ? this : clone(c.editor);

        switch ((int) nodePrime.size()) {
          case 0:
            return n.removeNode(mask);
          case 1:
            IEntry<K, V> e = nodePrime.nth(0);
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

  public Iterator<IEntry<K, V>> iterator() {

    return new Iterator<IEntry<K, V>>() {

      final IList<IMapNode<K, V>> nodes = LinearList.from(nodes());
      Iterator<IEntry<K, V>> iterator = entries();

      @Override
      public boolean hasNext() {
        return iterator.hasNext() || nodes.size() > 0;
      }

      @Override
      public IEntry<K, V> next() {
        while (!iterator.hasNext()) {
          IMapNode<K, V> node = nodes.first();
          nodes.removeFirst();
          iterator = node.entries();
          if (node instanceof ChampNode) {
            ((ChampNode<K, V>) node).nodes().forEach(n -> nodes.addLast(n));
          }
        }

        return iterator.next();
      }
    };
  }

  /////

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

  private int mergeState(int mask, int nodeA, int dataA, int nodeB, int dataB) {
    int state = 0;
    state |= (mask & nodeA) > 0 ? 0x1 : 0;
    state |= (mask & dataA) > 0 ? 0x2 : 0;
    state |= (mask & nodeB) > 0 ? 0x4 : 0;
    state |= (mask & dataB) > 0 ? 0x8 : 0;

    return state;
  }

  public ChampNode<K, V> merge(int shift, Object editor, ChampNode<K, V> o, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge) {
    ChampNode<K, V> node = new ChampNode<K, V>(editor);
    for (int i = 0; i < 32; i++) {
      int mask = 1 << i;
      int state = mergeState(mask, nodemap, datamap, o.nodemap, o.datamap);
      int idx;
      switch (state) {
        case NODE_NONE:
        case NONE_NODE:
          node = node.transferNode(mask, state == NODE_NONE ? this : o);
          break;
        case ENTRY_NONE:
        case NONE_ENTRY:
          node = node.transferEntry(mask, state == ENTRY_NONE ? this : o);
          break;
        case ENTRY_ENTRY:
          node = node.transferEntry(mask, this).transferEntry(mask, o);
          break;
        case NODE_NODE:
          // complicated
          node = node.transferNode(mask, null);
          break;
        case NODE_ENTRY:
          idx = o.entryIndex(mask);
          node = (ChampNode<K, V>) node
              .putNode(mask, node(mask))
              .put(shift, editor, o.hash(idx), (K) o.content[idx << 1], (V) o.content[(idx << 1) + 1], equals, merge);
          break;
        case ENTRY_NODE:
          idx = entryIndex(mask);
          node = (ChampNode<K, V>) node
              .putNode(mask, o.node(mask))
              .put(shift, editor, hash(idx), (K) content[idx << 1], (V) content[(idx << 1) + 1], equals, (a, b) -> merge.merge(b, a));
          break;
        case NONE_NONE:
          break;
      }
    }

    return node;
  }

  public ChampNode<K, V> difference(int shift, Object editor, ChampNode<K, V> o, BiPredicate<K, K> equals) {
    ChampNode<K, V> node = new ChampNode<K, V>(editor);

    for (int i = 0; i < 32; i++) {
      int mask = 1 << i;
      int state = mergeState(mask, nodemap, datamap, o.nodemap, o.datamap);
      int idx;
      switch (state) {
        case NODE_NONE:
          node = node.transferNode(mask, this);
          break;
        case ENTRY_NONE:
          node = node.transferEntry(mask, this);
          break;
        case ENTRY_ENTRY:
          int ia = o.entryIndex(mask);
          int ib = entryIndex(mask);
          if (o.hashes[ia] != hashes[ib] || !equals.test((K) o.content[ia << 1], (K) content[ib << 1])) {
            node.transferEntry(mask, this);
          }
          break;
        case NODE_NODE:
          // complicated
          node = node.transferNode(mask, null);
          break;
        case NODE_ENTRY:
          idx = o.entryIndex(mask);
          node = (ChampNode<K, V>) node(mask).remove(shift + 5, editor, o.hashes[idx], (K) o.content[idx << 1], equals);
          break;
        case ENTRY_NODE:
          idx = entryIndex(mask);
          if (o.get(shift, hashes[idx], (K) content[idx << 1], equals, DEFAULT_VALUE) == DEFAULT_VALUE) {
            node = transferEntry(mask, this);
          }
          break;
        case NONE_ENTRY:
        case NONE_NODE:
        case NONE_NONE:
          break;
      }
    }

    return node;
  }

  public ChampNode<K, V> intersection(int shift, Object editor, ChampNode<K, V> o, BiPredicate<K, K> equals) {
    ChampNode<K, V> node = new ChampNode<K, V>(editor);

    for (int i = 0; i < 32; i++) {
      int mask = 1 << i;
      int state = mergeState(mask, nodemap, datamap, o.nodemap, o.datamap);
      int idx;
      switch (state) {
        case ENTRY_ENTRY:
          int ia = o.entryIndex(mask);
          int ib = entryIndex(mask);
          if (o.hashes[ia] == hashes[ib] && equals.test((K) o.content[ia << 1], (K) content[ib << 1])) {
            node.transferEntry(mask, this);
          }
          break;
        case NODE_NODE:
          // complicated
          node = node.transferNode(mask, null);
          break;
        case NODE_ENTRY:
          idx = o.entryIndex(mask);
          if (get(shift, o.hashes[idx], (K) o.content[idx << 1], equals, DEFAULT_VALUE) != DEFAULT_VALUE) {
            node = transferEntry(mask, o);
          }
          break;
        case ENTRY_NODE:
          idx = entryIndex(mask);
          if (o.get(shift, hashes[idx], (K) content[idx << 1], equals, DEFAULT_VALUE) != DEFAULT_VALUE) {
            node = transferEntry(mask, this);
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

    return node;
  }

  private ChampNode<K, V> transferNode(int mask, ChampNode<K, V> node) {
    return putNode(mask, node.node(mask));
  }

  private ChampNode<K, V> transferEntry(int mask, ChampNode<K, V> node) {
    int idx = node.entryIndex(mask);
    return putEntry(mask, node.hashes[idx], (K) node.content[idx << 1], (V) node.content[(idx << 1) + 1]);
  }


  /////


  private ChampNode<K, V> clone(Object editor) {
    ChampNode<K, V> node = new ChampNode<>();
    node.datamap = datamap;
    node.nodemap = nodemap;
    node.hashes = hashes.clone();
    node.content = content.clone();
    node.editor = editor;
    node.size = size;

    return node;
  }

  private Iterable<IMapNode<K, V>> nodes() {
    return () -> new Iterator<IMapNode<K, V>>() {
      int idx = content.length - Integer.bitCount(nodemap);

      @Override
      public boolean hasNext() {
        return idx < content.length;
      }

      @Override
      public IMapNode<K, V> next() {
        return (IMapNode<K, V>) content[idx++];
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

  ChampNode<K, V> putEntry(int mask, int hash, K key, V value) {
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

  ChampNode<K, V> removeEntry(final int mask) {
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

  ChampNode<K, V> setNode(int mask, IMapNode<K, V> node) {
    int idx = content.length - 1 - nodeIndex(mask);
    size += node.size() - ((IMapNode<K, V>) content[idx]).size();
    content[idx] = node;

    return this;
  }

  ChampNode<K, V> putNode(final int mask, IMapNode<K, V> node) {
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

  ChampNode<K, V> removeNode(final int mask) {
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

  private IMapNode<K, V> node(int hashMask) {
    return (IMapNode<K, V>) content[content.length - 1 - nodeIndex(hashMask)];
  }

  private boolean isEntry(int hashMask) {
    return (datamap & hashMask) != 0;
  }

  private boolean isNode(int hashMask) {
    return (nodemap & hashMask) != 0;
  }
}
