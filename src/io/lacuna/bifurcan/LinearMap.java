package io.lacuna.bifurcan;

import io.lacuna.bifurcan.Maps.Entry;
import io.lacuna.bifurcan.utils.Bits;

import java.util.Objects;
import java.util.Optional;
import java.util.function.*;

import static io.lacuna.bifurcan.utils.Bits.log2Ceil;

/**
 * A hash-map implementation which uses Robin Hood hashing for placement, and allows for customized hashing and equality
 * semantics.
 * <p>
 * Unlike {@code HashMap.entrySet()}, the {@code entries()} method is O(1), returning an IList that proxies through to the
 * underlying {@code entries} array, which is guaranteed to be densely packed.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearMap<K, V> implements IMap<K, V> {

  public static final float DEFAULT_LOAD_FACTOR = 0.9f;

  private static final int NONE = 0;
  private static final int FALLBACK = 1;

  private final ToIntFunction<K> hashFn;
  private final BiPredicate<K, K> equalsFn;
  private final int indexMask;
  private final int threshold;

  private final long[] table;

  private final IEntry<K, V>[] entries;
  private int size;

  public LinearMap() {
    this(16);
  }

  public LinearMap(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public LinearMap(int initialCapacity, float loadFactor) {
    this(initialCapacity, loadFactor, Objects::hashCode, Objects::equals);
  }

  /**
   * Creates a map from an existing {@code java.util.Map}, using the default Java hashing and equality mechanisms.
   *
   * @param m an existing {@code java.util.Map}
   */
  public LinearMap(java.util.Map<K, V> m) {
    this(necessaryCapacity(m.size(), DEFAULT_LOAD_FACTOR), DEFAULT_LOAD_FACTOR);
    m.entrySet().forEach(e -> put(e.getKey(), e.getValue()));
  }

  public LinearMap(IMap<K, V> m) {
    this(necessaryCapacity((int) m.size(), DEFAULT_LOAD_FACTOR), DEFAULT_LOAD_FACTOR);
    m.entries().stream().forEach(e -> put(e.key(), e.value()));
  }

  public LinearMap(int initialCapacity, float loadFactor, ToIntFunction<K> hashFn, BiPredicate<K, K> equalsFn) {

    initialCapacity = (int) (1L << log2Ceil(initialCapacity));

    if (loadFactor < 0 || loadFactor > 1) {
      throw new IllegalArgumentException("loadFactor must be within [0,1]");
    }

    this.indexMask = (int) Bits.maskBelow(Bits.bitOffset(initialCapacity));
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
    this.threshold = (int) (initialCapacity * loadFactor);

    this.entries = new IEntry[threshold + 1];
    this.table = new long[initialCapacity];
    this.size = 0;
  }

  private static int necessaryCapacity(int size, float loadFactor) {
    return (int) (1L << log2Ceil((long) Math.ceil(size / loadFactor)));
  }

  private LinearMap<K, V> resize(int capacity) {
    LinearMap<K, V> m = new LinearMap<>(capacity, (float) threshold / table.length, hashFn, equalsFn);
    System.arraycopy(entries, 0, m.entries, 0, size);
    m.size = size;

    for (int idx = 0; idx < table.length; idx++) {
      long row = table[idx];
      int hash = Row.hash(row);
      if (hash != NONE && !Row.tombstone(row)) {
        m.naivePut(hash, Row.entryIndex(row));
      }
    }

    return m;
  }

  private int indexFor(K key) {
    int hash = keyHash(key);

    for (int idx = indexFor(hash); ; idx = nextIndex(idx)) {
      long row = table[idx];
      int currHash = Row.hash(row);
      if (currHash == hash && !Row.tombstone(row) && equalsFn.test(key, entry(idx).key())) {
        return idx;
      } else if (currHash == NONE || isTooFar(idx, hash, currHash)) {
        return -1;
      }
    }
  }

  private void naivePut(int hash, int entryIndex) {
    for (int idx = indexFor(hash); ; idx = nextIndex(idx)) {
      long row = table[idx];
      int currHash = Row.hash(row);
      if (currHash == NONE) {
        table[idx] = Row.construct(hash, entryIndex);
        break;
      } else if (isTooFar(idx, hash, currHash)) {
        int currEntryIndex = Row.entryIndex(row);
        table[idx] = Row.construct(hash, entryIndex);

        entryIndex = currEntryIndex;
        hash = currHash;
      }
    }
  }

  // factored out for better inlining
  private boolean putCheckEquality(int idx, IEntry<K, V> entry) {
    long row = table[idx];
    int currIndex = Row.entryIndex(row);
    IEntry<K, V> currEntry = entries[currIndex];
    if (equalsFn.test(entry.key(), currEntry.key())) {
      entries[currIndex] = entry;
      table[idx] = Row.removeTombstone(row);
      return true;
    } else {
      return false;
    }
  }

  private void put(int hash, IEntry<K, V> entry) {
    for (int idx = indexFor(hash); ; idx = nextIndex(idx)) {
      long row = table[idx];
      int currHash = Row.hash(row);
      boolean isNone = currHash == NONE;
      boolean currTombstone = Row.tombstone(row);

      if (currHash == hash && !currTombstone && putCheckEquality(idx, entry)) {
        break;
      } else if (isNone || isTooFar(idx, hash, currHash)) {
        // if it's empty, or it's a tombstone and there's no possible exact match further down, use it
        if (isNone || currTombstone) {
          entries[size] = entry;
          table[idx] = Row.construct(hash, size);
          size++;
          break;
        }

        // we deserve this location more, so swap them out
        int currEntryIndex = Row.entryIndex(row);
        IEntry<K, V> currEntry = entries[currEntryIndex];
        entries[currEntryIndex] = entry;
        table[idx] = Row.construct(hash, currEntryIndex);

        entry = currEntry;
        hash = currHash;
      }
    }
  }

  @Override
  public IMap<K, V> put(K key, V value) {
    if (size >= threshold) {
      return resize(table.length << 1).put(key, value);
    } else {
      put(keyHash(key), new Entry<>(key, value));
      return this;
    }
  }

  @Override
  public IMap<K, V> remove(K key) {
    int idx = indexFor(key);
    if (idx >= 0) {
      long row = table[idx];
      size--;
      int entryIndex = Row.entryIndex(row);
      if (entryIndex != size) {
        IEntry<K, V> lastEntry = entries[size];
        int lastIdx = indexFor(lastEntry.key());
        table[lastIdx] = Row.construct(Row.hash(table[lastIdx]), entryIndex);
        entries[entryIndex] = lastEntry;
      }
      table[idx] = Row.addTombstone(row);
      entries[size] = null;
    }

    return this;
  }

  @Override
  public Optional<V> get(K key) {
    int hash = keyHash(key);

    for (int idx = indexFor(hash); ; idx = nextIndex(idx)) {
      long row = table[idx];
      int currHash = Row.hash(row);
      if (currHash == hash && !Row.tombstone(row)) {
        IEntry<K, V> entry = entry(idx);
        if (equalsFn.test(entry.key(), key)) {
          return Optional.of(entry.value());
        }
      } else if (currHash == NONE || isTooFar(idx, hash, currHash)) {
        return Optional.empty();
      }
    }
  }

  @Override
  public IList<IEntry<K, V>> entries() {
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
  public boolean equals(Object obj) {
    if (obj instanceof IMap) {
      return Maps.equals(this, (IMap<K, V>) obj);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) Maps.hash(this);
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }

  /// Utility functions

  private static class Row {

    static final long HASH_MASK = Bits.maskBelow(32);
    static final long ENTRY_INDEX_MASK = Bits.maskBelow(31);
    static final long TOMBSTONE_MASK = 1L << 63;

    static long construct(int hash, int entryIndex) {
      return hash | (entryIndex & ENTRY_INDEX_MASK) << 32;
    }

    static int hash(long row) {
      return (int) (row & HASH_MASK);
    }

    static int entryIndex(long row) {
      return (int) ((row >>> 32) & ENTRY_INDEX_MASK);
    }

    static boolean tombstone(long row) {
      return (row & TOMBSTONE_MASK) != 0;
    }

    static long addTombstone(long row) {
      return row | TOMBSTONE_MASK;
    }

    static long removeTombstone(long row) {
      return row & ~TOMBSTONE_MASK;
    }
  }

  private int indexFor(int hash) {
    return hash & indexMask;
  }

  private int nextIndex(int idx) {
    return (idx + 1) & indexMask;
  }

  private IEntry<K, V> entry(int idx) {
    return entries[Row.entryIndex(table[idx])];
  }

  int probeDistance(int hash, int index) {
    return ((index + table.length - (hash & indexMask)) & indexMask);
  }

  private boolean isTooFar(int idx, int initHash, int hash) {
    return probeDistance(initHash, idx) > probeDistance(hash, idx);
  }

  private int keyHash(K key) {
    int hash = hashFn.applyAsInt(key);

    // make sure we don't have too many collisions in the lower bits
    hash ^= (hash >>> 20) ^ (hash >>> 12);
    hash ^= (hash >>> 7) ^ (hash >>> 4);
    return hash == NONE ? FALLBACK : hash;
  }

}
