package io.lacuna.bifurcan.nodes;

import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.Maps;
import io.lacuna.bifurcan.utils.ArrayVector;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static java.lang.Integer.bitCount;

/**
 * @author ztellman
 */
public class ChampNode<K, V> implements IMapNode<K, V> {

  public static final ChampNode EMPTY;

  static {
    EMPTY = new ChampNode();
    EMPTY.hashes = new int[0];
    EMPTY.content = new Object[0];
    EMPTY.editor = new Object();
  }

  public static final int SHIFT_INCREMENT = 5;

  int datamap = 0;
  int nodemap = 0;
  int[] hashes;
  public Object[] content;
  Object editor;
  long size;

  public ChampNode() {
  }

  public long size() {
    return size;
  }

  public IMap.IEntry<K, V> nth(long idx) {
    int entries = Integer.bitCount(datamap);
    if (idx < entries) {
      K key = (K) content[(int) idx << 1];
      V val = (V) content[((int) idx << 1) + 1];
      return new Maps.Entry<>(key, val);
    }

    idx -= entries;
    if (idx < size) {
      int nodes = Integer.bitCount(nodemap);
      for (int i = content.length - nodes; i < content.length; i++) {
        IMapNode<K, V> node = (IMapNode<K, V>) content[i];
        if (idx < node.size()) {
          return node.nth(idx);
        }
        idx -= node.size();
      }
    }

    throw new IndexOutOfBoundsException();
  }

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

  public ChampNode<K, V> put(int shift, Object editor, int hash, K key, V value, BiPredicate<K, K> equals, IMap.ValueMerger<V> merge) {
    int mask = hashMask(hash, shift);

    // there's a potential match
    if (isEntry(mask)) {
      int idx = entryIndex(mask);

      // there is a match
      if (hashes[idx] == hash && equals.test(key, (K) content[idx << 1])) {

        // we can overwrite it
        if (editor == this.editor) {
          idx = (idx << 1) + 1;
          content[idx] = merge.merge((V) content[idx], value);
          return this;

          // we can't overwrite it
        } else {
          return putEntry(editor, mask, true, hash, key, merge.merge((V) content[(idx << 1) + 1], value));
        }

        // hash collision, create a child
      } else {
        K currKey = (K) content[idx << 1];
        V currVal = (V) content[(idx << 1) + 1];

        IMapNode<K, V> node;
        if (shift < 30) {
          node = ((ChampNode<K, V>) EMPTY)
              .put(shift + SHIFT_INCREMENT, editor, hashes[idx], currKey, currVal, equals, merge)
              .put(shift + SHIFT_INCREMENT, editor, hash, key, value, equals, merge);
        } else {
          node = new CollisionNode<K, V>(currKey, currVal, key, value);
        }
        return removeEntry(editor, mask).putNode(editor, mask, false, node, node.size());
      }

      // we must go deeper
    } else if (isNode(mask)) {
      int idx = nodeIndex(mask);
      IMapNode<K, V> node = (IMapNode<K, V>) content[idx];
      IMapNode<K, V> nodePrime = node.put(shift + SHIFT_INCREMENT, editor, hash, key, value, equals, merge);

      if (node == nodePrime) {
        return this;
      } else {
        return putNode(editor, mask, true, nodePrime, nodePrime.size() - node.size());
      }

      // no such thing
    } else {
      return putEntry(editor, mask, false, hash, key, value);
    }
  }

  public ChampNode<K, V> remove(int shift, Object editor, int hash, K key, BiPredicate<K, K> equals) {
    int mask = hashMask(hash, shift);

    // there's a potential match
    if (isEntry(mask)) {
      int idx = entryIndex(mask);

      // there is a match
      if (hashes[idx] == hash && equals.test(key, (K) content[idx << 1])) {
        return removeEntry(editor, mask);

        // nope
      } else {
        return this;
      }

      // we must go deeper
    } else if (isNode(mask)) {
      int idx = nodeIndex(mask);
      IMapNode<K, V> node = (IMapNode<K, V>) content[idx];
      IMapNode<K, V> nodePrime = node.remove(shift + SHIFT_INCREMENT, editor, hash, key, equals);

      if (node == nodePrime) {
        return this;
      } else if (nodePrime.size() == 0) {
        return removeNode(editor, mask, node.size());
      } else {
        return putNode(editor, mask, true, nodePrime, nodePrime.size() - node.size());
      }

      // no such thing
    } else {
      return this;
    }
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

  private ChampNode<K, V> removeNode(Object editor, int hashMask, long size) {
    ChampNode<K, V> node = editor == this.editor ? this : new ChampNode<K, V>();
    node.datamap = datamap;
    node.nodemap = nodemap & ~hashMask;
    node.hashes = hashes;
    node.editor = editor;
    node.size = this.size - size;

    int idx = nodeIndex(hashMask);
    node.content = ArrayVector.remove(content, idx, 1);

    return node;
  }

  private ChampNode<K, V> putNode(Object editor, int hashMask, boolean overwrite, Object child, long sizeDelta) {
    ChampNode<K, V> node = editor == this.editor ? this : new ChampNode<K, V>();
    node.datamap = datamap;
    node.nodemap = nodemap | hashMask;
    node.hashes = hashes;
    node.editor = editor;
    node.size = size + sizeDelta;

    int idx = nodeIndex(hashMask) + (overwrite ? 0 : 1);
    node.content = overwrite
        ? ArrayVector.set(content, idx, child)
        : ArrayVector.insert(content, idx, child);

    return node;
  }

  private ChampNode<K, V> removeEntry(Object editor, int hashMask) {

    ChampNode<K, V> node = editor == this.editor ? this : new ChampNode<K, V>();
    node.datamap = datamap & ~hashMask;
    node.nodemap = nodemap;
    node.editor = editor;
    node.size = size - 1;

    int idx = entryIndex(hashMask);
    node.hashes = ArrayVector.remove(hashes, idx, 1);

    idx = idx << 1;
    node.content = ArrayVector.remove(content, idx, 2);

    return node;
  }

  private ChampNode<K, V> putEntry(Object editor, int hashMask, boolean overwrite, int hash, K key, V value) {

    ChampNode<K, V> node = editor == this.editor ? this : new ChampNode<K, V>();
    node.datamap = datamap | hashMask;
    node.nodemap = nodemap;
    node.editor = editor;
    node.size = size + (overwrite ? 0 : 1);

    int idx = entryIndex(hashMask);
    node.hashes = overwrite
        ? ArrayVector.set(hashes, idx, hash)
        : ArrayVector.insert(hashes, idx, hash);

    idx = idx << 1;
    node.content = overwrite
        ? ArrayVector.set(content, idx, key, value)
        : ArrayVector.insert(content, idx, key, value);

    return node;
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
    return content.length - 1 - compressedIndex(nodemap, hashMask);
  }

  private IMapNode<K, V> node(int hashMask) {
    return (IMapNode<K, V>) content[nodeIndex(hashMask)];
  }

  private boolean isEntry(int hashMask) {
    return (datamap & hashMask) != 0;
  }

  private boolean isNode(int hashMask) {
    return (nodemap & hashMask) != 0;
  }
}
