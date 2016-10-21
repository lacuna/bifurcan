package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.BitVector;
import io.lacuna.bifurcan.utils.Bits;

import java.util.Objects;
import java.util.Optional;
import java.util.function.*;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearMap<K, V> implements IMap<K, V> {

  private static final float MAX_LOAD_FACTOR = 0.85f;

  private interface Biterator {
    int next();
  }

  private static class Entry<K, V> implements IMap.Entry<K, V> {
    private final K key;
    private final V value;

    public Entry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K key() {
      return key;
    }

    public V value() {
      return value;
    }
  }

  private final ToLongFunction<K> hashFn;
  private final BiPredicate<K, K> equalsFn;
  private final byte indexBits;
  private final byte hashBits;

  private final long[] index;
  private final Entry<K, V>[] entries;
  private int size;

  public LinearMap() {
    this(16, 32, Objects::hashCode, Objects::equals);
  }

  private LinearMap(int capacity, int hashBits, ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    if (!Bits.isPowerOfTwo(capacity)) {
      throw new IllegalArgumentException("capacity must be power of two");
    }

    if (hashBits < 2 || hashBits > 64) {
      throw new IllegalArgumentException("hashBits must be within [2, 64]");
    }

    this.indexBits = (byte) Bits.bitLog2(capacity);
    this.hashBits = (byte) hashBits;
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;

    this.entries = new Entry[capacity];
    this.index = new long[(((indexBits + hashBits) * capacity) >> 6)];
    this.size = 0;
  }

  private IMap<K, V> resize(int capacity) {
    IMap<K, V> m = new LinearMap<>(capacity, hashBits, hashFn, equalsFn);
    for (int i = 0; i < size; i++) {
      Entry<K, V> e = entries[i];
      m = m.put(e.key, e.value);
    }
    return m;
  }

  private long keyHash(K key) {
    long hash = hashFn.applyAsLong(key) & Bits.maskBelow(hashBits);
    return hash == 0 ? 1 : hash;
  }

  private long entryHash(int bitIndex) {
    return BitVector.get(index, bitIndex, hashBits);
  }

  private int entryIndex(int bitIndex) {
    return (int) BitVector.get(index, bitIndex + hashBits, indexBits);
  }

  private Biterator biterator(int start) {
    return new Biterator() {
      private final int stride = indexBits + hashBits;
      private final int limit = stride << indexBits;
      private int idx = start - stride;

      @Override
      public int next() {
        idx += stride;
        if (idx >= limit) {
          idx -= limit;
        }
        return idx;
      }
    };
  }

  private int preferredIndex(long hash) {
    return (indexBits + hashBits) * (int) (hash & Bits.maskBelow(indexBits));
  }

  private int probeLength(int preferred, int actual) {
    return actual < preferred
            ? ((indexBits + hashBits) << indexBits) - (preferred - actual)
            : actual - preferred;
  }

  private int possibleKeyIndex(long hash) {
    int stride = hashBits + indexBits;
    int preferred = preferredIndex(hash);
    Biterator biterator = biterator(preferred);

    for (int length = 0; ; length += stride) {
      int idx = biterator.next();
      long currHash = entryHash(idx);
      if (currHash == 0) {
        return idx;
      }

      if (hash == currHash) {
        return idx;
      } else {
        int currPreferred = preferredIndex(currHash);
        if (probeLength(currPreferred, idx) > length) {
          return idx;
        }
      }
    }
  }

  private int keyIndex(K key) {
    long hash = keyHash(key);
    Biterator it = biterator(possibleKeyIndex(hash));

    for (; ; ) {
      int idx = it.next();
      if (entryHash(idx) != hash) {
        return -1;
      }

      Entry<K, V> entry = entries[entryIndex(idx)];
      if (equalsFn.test(entry.key, key)) {
        return idx;
      }
    }
  }

  @Override
  public IMap<K, V> put(K key, V value) {

    if (size > (int)(entries.length * MAX_LOAD_FACTOR)) {
      return resize(entries.length << 1).put(key, value);
    }

    long hash = keyHash(key);
    Biterator it = biterator(possibleKeyIndex(hash));
    Entry<K, V> entry = new Entry<K, V>(key, value);

    int preferred = preferredIndex(hash);
    for (; ; ) {
      int idx = it.next();
      System.out.println("idx : " + idx);
      //try { Thread.sleep(100); } catch (InterruptedException e) {}
      long currHash = entryHash(idx);
      if (currHash == 0) {
        // nothing there, fill it in
        entries[size] = entry;
        BitVector.overwrite(index, (size << hashBits) | hash, idx, hashBits + indexBits);
        System.out.println(hash + " " + entryHash(idx));
        size++;
        break;
      } else if (currHash == hash) {
        int currIndex = entryIndex(idx);
        Entry<K, V> currEntry = entries[currIndex];
        if (equalsFn.test(key, currEntry.key)) {
          entries[currIndex] = new Entry<K,V>(key, value);
          break;
        }
      } else {
        int currPreferred = preferredIndex(currHash);
        if (probeLength(preferred, idx) > probeLength(currPreferred, idx)) {
          // we deserve this location more, so swap them out
          int currIndex = entryIndex(idx);
          Entry<K, V> currEntry = entries[currIndex];

          entries[currIndex] = entry;
          BitVector.overwrite(index, hash, idx, hashBits);

          preferred = currPreferred;
          entry = currEntry;
          hash = currHash;
        }
      }
    }
    return this;
  }

  @Override
  public IMap<K, V> remove(K key) {
    long hash = keyHash(key);
    Biterator it = biterator(possibleKeyIndex(hash));

    for (; ; ) {
      int idx = it.next();
      if (entryHash(idx) != hash) {
        break;
      }

      int entryIdx = entryIndex(idx);
      Entry<K, V> entry = entries[entryIdx];
      if (equalsFn.test(entry.key, key)) {
        size--;
        BitVector.overwrite(index, idx, 0, hashBits);
        if (size > 0) {
          Entry<K, V> lastEntry = entries[size];
          int lastIdx = keyIndex(lastEntry.key);
          BitVector.overwrite(index, lastIdx + hashBits, entryIdx, indexBits);
          entries[entryIdx] = lastEntry;
        }
        break;
      }
    }

    return this;
  }

  @Override
  public Optional<V> get(K key) {
    int idx = keyIndex(key);
    if (idx < 0) {
      return Optional.empty();
    } else {
      return Optional.of(entries[entryIndex(idx)].value);
    }
  }

  @Override
  public IList<IMap.Entry<K, V>> entries() {
    return Lists.from(size, i -> entries[(int) i]);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IMap<K, V> forked() {
    return null;
  }

  @Override
  public IMap<K, V> linear() {
    return this;
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }
}
