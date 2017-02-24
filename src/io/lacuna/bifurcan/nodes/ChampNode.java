package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.IteratorStack;

import java.util.Iterator;
import java.util.function.BiPredicate;

import static java.lang.Integer.bitCount;
import static java.lang.System.arraycopy;

/**
 * This is an implementation based on the one described in https://michael.steindorfer.name/publications/oopsla15.pdf.
 *
 * It adds in support for transient/linear updates, and allows for empty buffer space between the entries and nodes
 * to minimize allocations when a node is repeatedly updated in-place.
 *
 * @author ztellman
 */
public class ChampNode<K, V> implements IMapNode<K, V> {

  public static final ChampNode EMPTY = new ChampNode(new Object());

  public static final int SHIFT_INCREMENT = 5;

  int datamap = 0;
  int nodemap = 0;
  public int[] hashes = new int[2];
  public Object[] content = new Object[4];
  Object editor;
  long size;

  public ChampNode() {
  }

  private ChampNode(Object editor) {
    this.editor = editor;
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

  // this is factored out of `put` for greater inlining joy
  private ChampNode<K, V> insertEntry(int shift, int mask, PutCommand<K, V> c) {
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
        node = new CollisionNode<K, V>(key, value, c.key, c.value);
      }

      return (c.editor == editor ? this : clone(c.editor)).removeEntry(mask).putNode(mask, node);
    }
  }

  @Override
  public ChampNode<K, V> put(int shift, PutCommand<K, V> c) {
    int mask = hashMask(c.hash, shift);

    if (isEntry(mask)) {
      return insertEntry(shift, mask, c);

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

        // TODO: if size == 1, we should pull the entry into this node
        return nodePrime.size() == 0 ? n.removeNode(mask) : n.setNode(mask, nodePrime);
      }

      // no such thing
    } else {
      return this;
    }
  }

  @Override
  public Iterator<IMap.IEntry<K, V>> iterator() {
    final int entries = Integer.bitCount(datamap);
    return new Iterator<IMap.IEntry<K, V>>() {

      int idx = 0;
      IteratorStack<IMap.IEntry<K, V>> stack = new IteratorStack<>();

      @Override
      public boolean hasNext() {
        return idx < size();
      }

      @Override
      public IMap.IEntry<K, V> next() {
        int i = idx++;
        if (i < entries) {
          return new Maps.Entry((K) content[i << 1], (V) content[(i << 1) + 1]);
        } else if (i == entries) {
          nodes().forEach(n -> stack.addLast(n.iterator()));
          return stack.next();
        } else {
          return stack.next();
        }
      }
    };
  }

  public ChampNode<K, V> merge(Object editor, ChampNode node, IMap.ValueMerger<V> merge) {
    return null;
  }

  public ChampNode<K, V> difference(Object editor, ChampNode node) {
    return null;
  }

  public ChampNode<K, V> intersection(Object editor, ChampNode node) {
    return null;
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
    content = c;

    int[] h = new int[hashes.length << 1];
    arraycopy(hashes, 0, h, 0, bitCount(datamap));
    hashes = h;
  }

  private ChampNode<K, V> putEntry(int mask, int hash, K key, V value) {
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

  private ChampNode<K, V> removeEntry(final int mask) {
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

  private ChampNode<K, V> setNode(int mask, IMapNode<K, V> node) {
    int idx = content.length - 1 - nodeIndex(mask);
    size += node.size() - ((IMapNode<K, V>) content[idx]).size();
    content[idx] = node;

    return this;
  }

  private ChampNode<K, V> putNode(final int mask, IMapNode<K, V> node) {
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

  private ChampNode<K, V> removeNode(final int mask) {
    // shrink?

    int idx = nodeIndex(mask);
    int numNodes = bitCount(nodemap);
    size -= node(mask).size();
    arraycopy(content, content.length - numNodes, content, content.length + 1 - numNodes, numNodes - 1 - idx);
    nodemap &= ~mask;

    content[content.length - 1 - idx] = null;

    return this;
  }

  private static int compressedIndex(int bitmap, int hashMask) {
    return bitCount(bitmap & (hashMask - 1));
  }

  private static int hashMask(int hash, int shift) {
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
