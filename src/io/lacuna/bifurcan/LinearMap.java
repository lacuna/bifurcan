package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Bits;

import java.util.Objects;
import java.util.function.*;

import static io.lacuna.bifurcan.Lists.lazyMap;
import static io.lacuna.bifurcan.utils.Bits.log2Ceil;
import static java.lang.System.arraycopy;

/**
 * A hash-map implementation which uses Robin Hood hashing for placement, and allows for customized hashing and equality
 * semantics.  Performance is equivalent to {@code java.util.HashMap} for reads, moderately faster for writes, and more
 * robust to cases of poor hash distribution.
 * <p>
 * The {@code entries()} method is O(1) and allows random access, returning an IList that proxies through to an
 * underlying array.  Partitioning this list is the most efficient way to process the collection in parallel.
 * <p>
 * However, {@code LinearMap} also exposes O(N) {@code split()} and {@code merge()} methods, which despite their
 * asymptotic complexity can be quite fast in practice.  The appropriate way to split this collection will depend
 * on the use case.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearMap<K, V> implements IMap<K, V> {

  /// Fields

  public static final int MAX_CAPACITY = 1 << 29;
  private static final float LOAD_FACTOR = 0.95f;

  private static final int NONE = 0;
  private static final int FALLBACK = 1;

  private final ToIntFunction<K> hashFn;
  private final BiPredicate<K, K> equalsFn;

  private int indexMask;
  public long[] table;
  public Object[] entries;
  private int size;

  /// Constructors

  public LinearMap() {
    this(16);
  }

  public LinearMap(int initialCapacity) {
    this(initialCapacity, Objects::hashCode, Objects::equals);
  }

  public static <K, V> LinearMap<K, V> from(java.util.Map<K, V> map) {
    LinearMap<K, V> l = new LinearMap<K, V>(map.size());
    map.entrySet().forEach(e -> l.put(e.getKey(), e.getValue()));
    return l;
  }

  public static <K, V> LinearMap<K, V> from(IMap<K, V> map) {
    if (map instanceof LinearMap) {
      LinearMap<K, V> m = (LinearMap<K, V>) map;
      LinearMap<K, V> l = new LinearMap<K, V>(m.entries.length >> 1, m.hashFn, m.equalsFn);

      arraycopy(m.entries, 0, l.entries, 0, m.size << 1);
      arraycopy(m.table, 0, l.table, 0, m.table.length);
      l.size = m.size;

      return l;
    } else {
      LinearMap<K, V> l = new LinearMap<K, V>((int) map.size());
      map.entries().stream().forEach(e -> l.put(e.key(), e.value()));
      return l;
    }
  }

  public static <K, V> LinearMap<K, V> from(IList<IEntry<K, V>> entries) {
    if (entries.size() > MAX_CAPACITY) {
      throw new IllegalArgumentException("LinearMap cannot hold more than 1 << 29 entries");
    }
    LinearMap<K, V> m = new LinearMap<>((int) entries.size());
    for (IEntry<K, V> e : entries) {
      m = m.put(e.key(), e.value());
    }
    return m;
  }

  public LinearMap(int initialCapacity, ToIntFunction<K> hashFn, BiPredicate<K, K> equalsFn) {

    if (initialCapacity > MAX_CAPACITY) {
      throw new IllegalArgumentException("initialCapacity cannot be larger than " + MAX_CAPACITY);
    }

    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
    this.size = 0;

    resize(initialCapacity);
  }

  /// Accessors

  @Override
  public LinearMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public LinearMap<K, V> put(K key, V value, IMap.ValueMerger<V> merge) {
    if ((size << 1) == entries.length) {
      resize(size << 1);
    }
    put(keyHash(key), key, value, merge);
    return this;
  }

  @Override
  public LinearMap<K, V> remove(K key) {
    int idx = tableIndex(keyHash(key), key);

    if (idx >= 0) {
      long row = table[idx];
      size--;
      int keyIndex = Row.keyIndex(row);
      int lastKeyIndex = size << 1;

      // if we're not the last entry, swap the last entry into our slot, so we remain dense
      if (keyIndex != lastKeyIndex) {
        K lastKey = (K) entries[lastKeyIndex];
        V lastValue = (V) entries[lastKeyIndex + 1];
        int lastIdx = tableIndex(keyHash(lastKey), lastKey);
        table[lastIdx] = Row.construct(Row.hash(table[lastIdx]), keyIndex);
        putEntry(keyIndex, lastKey, lastValue);
      }

      table[idx] = Row.addTombstone(row);
      putEntry(lastKeyIndex, null, null);
    }

    return this;
  }

  @Override
  public boolean contains(K key) {
    return tableIndex(keyHash(key), key) >= 0;
  }

  @Override
  public V get(K key, V defaultValue) {
    int idx = tableIndex(keyHash(key), key);
    if (idx >= 0) {
      long row = table[idx];
      return (V) entries[Row.keyIndex(row) + 1];
    } else {
      return defaultValue;
    }
  }

  @Override
  public IList<IEntry<K, V>> entries() {
    return Lists.from(size, i -> {
      int idx = ((int) i) << 1;
      return new Maps.Entry<>((K) entries[idx], (V) entries[idx + 1]);
    });
  }

  @Override
  public ISet<K> keys() {
    return Sets.from(lazyMap(entries(), IEntry::key), this::contains);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IMap<K, V> forked() {
    throw new IllegalStateException("a LinearMap cannot be efficiently transformed into a forked representation");
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
  protected LinearMap<K, V> clone() {
    LinearMap<K, V> m = new LinearMap<K, V>(entries.length, hashFn, equalsFn);
    arraycopy(table, 0, m.table, 0, table.length);
    arraycopy(entries, 0, m.entries, 0, entries.length);
    m.size = size;
    return m;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (long row : table) {
      if (Row.populated(row)) {
        V value = (V) entries[Row.keyIndex(row) + 1];
        hash += (Row.hash(row) * 31) + Objects.hashCode(value);
      }
    }
    return hash;
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }

  @Override
  public IList<IMap<K, V>> split(int parts) {
    parts = Math.min(parts, size);
    IList<IMap<K, V>> list = new LinearList<>(parts);
    if (parts <= 1) {
      return list.addLast(this);
    }

    int partSize = table.length / parts;
    for (int p = 0; p < parts; p++) {
      int start = p * partSize;
      int finish = (p == (parts - 1)) ? table.length : start + partSize;

      LinearMap<K, V> m = new LinearMap<>(finish - start);

      for (int i = start; i < finish; i++) {
        long row = table[i];
        if (Row.populated(row)) {
          int keyIndex = Row.keyIndex(row);
          int resultKeyIndex = m.size << 1;
          m.putEntry(resultKeyIndex, (K) entries[keyIndex], (V) entries[keyIndex + 1]);
          m.putTable(Row.hash(row), resultKeyIndex);
          m.size++;
        }
      }

      if (m.size > 0) {
        list.addLast(m);
      }
    }

    return list;
  }

  @Override
  public LinearMap<K, V> merge(IMap<K, V> o, ValueMerger<V> mergeFn) {
    if (o.size() == 0) {
      return this;
    } else if (o instanceof LinearMap) {
      LinearMap<K, V> l = (LinearMap<K, V>) o;
      resize(size + l.size);
      for (long row : l.table) {
        if (Row.populated(row)) {
          int keyIndex = Row.keyIndex(row);
          put(Row.hash(row), (K) l.entries[keyIndex], (V) l.entries[keyIndex + 1], mergeFn);
        }
      }
    } else {
      for (IEntry<K, V> e : o.entries()) {
        put(e.key(), e.value(), mergeFn);
      }
    }
    return this;
  }

  /// Bookkeeping functions

  LinearMap<K, V> difference(LinearMap<K, ?> m) {
    LinearMap<K, V> result = new LinearMap<>(size);
    combine(m, result, i -> i == -1);
    return result;
  }

  LinearMap<K, V> intersection(LinearMap<K, ?> m) {
    LinearMap<K, V> result = new LinearMap<K, V>(Math.min(size, (int) m.size()));
    combine(m, result, i -> i != -1);
    return result;
  }

  private void combine(LinearMap<K, ?> m, LinearMap<K, V> result, IntPredicate indexPredicate) {
    for (long row : table) {
      if (Row.populated(row)) {
        int currKeyIndex = Row.keyIndex(row);
        K currKey = (K) entries[currKeyIndex];
        int entryIndex = m.tableIndex(Row.hash(row), currKey);
        if (indexPredicate.test(entryIndex)) {
          int resultKeyIndex = result.size << 1;
          result.putEntry(resultKeyIndex, currKey, (V) entries[currKeyIndex + 1]);
          result.putTable(Row.hash(row), resultKeyIndex);
          result.size++;
        }
      }
    }
  }

  private void resize(int capacity) {

    if (capacity > MAX_CAPACITY) {
      throw new IllegalStateException("the map cannot be larger than " + MAX_CAPACITY);
    }

    capacity = Math.max(4, capacity);
    int tableLength = (1 << log2Ceil((long) Math.ceil(capacity / LOAD_FACTOR)));
    indexMask = tableLength - 1;

    // update table
    if (table == null) {
      table = new long[tableLength];
    } else if (table.length != tableLength) {
      long[] nTable = new long[tableLength];
      for (long row : table) {
        if (Row.populated(row)) {
          int hash = Row.hash(row);
          putTable(nTable, hash, Row.keyIndex(row), estimatedIndex(hash));
        }
      }
      table = nTable;
    }

    // update entries
    if (entries == null) {
      entries = new Object[capacity << 1];
    } else {
      Object[] nEntries = new Object[capacity << 1];
      arraycopy(entries, 0, nEntries, 0, size << 1);
      entries = nEntries;
    }

  }

  private int tableIndex(int hash, K key) {
    for (int idx = estimatedIndex(hash), dist = 0; ; idx = nextIndex(idx), dist++) {
      long row = table[idx];
      int currHash = Row.hash(row);
      if (currHash == hash && !Row.tombstone(row) && equalsFn.test(key, (K) entries[Row.keyIndex(row)])) {
        return idx;
      } else if (currHash == NONE || dist > probeDistance(currHash, idx)) {
        return -1;
      }
    }
  }

  private void putTable(long[] table, int hash, int keyIndex, int tableIndex) {
    for (int idx = tableIndex, dist = probeDistance(hash, tableIndex), abs = 0; ; idx = nextIndex(idx), dist++, abs++) {
      long row = table[idx];
      int currHash = Row.hash(row);

      if (abs > table.length) {
        throw new IllegalStateException();
      }

      if (currHash == NONE) {
        table[idx] = Row.construct(hash, keyIndex);
        break;
      } else if (dist > probeDistance(currHash, idx)) {
        int currKeyIndex = Row.keyIndex(row);
        table[idx] = Row.construct(hash, keyIndex);

        if (Row.tombstone(row)) {
          break;
        }

        dist = probeDistance(currHash, idx);
        keyIndex = currKeyIndex;
        hash = currHash;
      }
    }
  }

  private void putEntry(int keyIndex, K key, V value) {
    entries[keyIndex] = key;
    entries[keyIndex + 1] = value;
  }

  private void putTable(int hash, int keyIndex) {
    putTable(table, hash, keyIndex, estimatedIndex(hash));
  }

  // factored out for better inlining
  private boolean putCheckEquality(int idx, K key, V value, IMap.ValueMerger<V> mergeFn) {
    long row = table[idx];
    int keyIndex = Row.keyIndex(row);
    K currKey = (K) entries[keyIndex];
    if (equalsFn.test(key, currKey)) {
      entries[keyIndex + 1] = mergeFn.merge((V) entries[keyIndex + 1], value);
      table[idx] = Row.removeTombstone(row);
      return true;
    } else {
      return false;
    }
  }

  private void put(int hash, K key, V value, IMap.ValueMerger<V> mergeFn) {
    for (int idx = estimatedIndex(hash), dist = 0; ; idx = nextIndex(idx), dist++) {
      long row = table[idx];
      int currHash = Row.hash(row);
      boolean isNone = currHash == NONE;
      boolean currTombstone = Row.tombstone(row);

      if (currHash == hash && !currTombstone && putCheckEquality(idx, key, value, mergeFn)) {
        break;
      } else if (isNone || dist > probeDistance(currHash, idx)) {

        // we know there isn't any collision, so add it to the end
        int keyIndex = size << 1;
        putEntry(keyIndex, key, value);
        size++;

        if (isNone || currTombstone) {
          table[idx] = Row.construct(hash, keyIndex);
        } else {
          putTable(table, hash, keyIndex, idx);
        }

        break;
      }
    }
  }

  /// Utility functions

  static class Row {

    static final long HASH_MASK = (1L << 32) - 1;
    static final long KEY_INDEX_MASK = (1L << 31) - 1;
    static final long TOMBSTONE_MASK = 1L << 63;

    static long construct(int hash, int keyIndex) {
      return (hash & HASH_MASK) | (keyIndex & KEY_INDEX_MASK) << 32;
    }

    static int hash(long row) {
      return (int) (row & HASH_MASK);
    }

    static boolean populated(long row) {
      return (row & HASH_MASK) != NONE && (row & TOMBSTONE_MASK) == 0;
    }

    static int keyIndex(long row) {
      return (int) ((row >> 32) & KEY_INDEX_MASK);
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

  private int estimatedIndex(int hash) {
    return hash & indexMask;
  }

  private int nextIndex(int idx) {
    return (idx + 1) & indexMask;
  }

  private int probeDistance(int hash, int index) {
    return (index + table.length - (hash & indexMask)) & indexMask;
  }

  private int keyHash(K key) {
    int hash = hashFn.applyAsInt(key);

    // make sure we don't have too many collisions in the lower bits
    hash ^= (hash >>> 20) ^ (hash >>> 12);
    hash ^= (hash >>> 7) ^ (hash >>> 4);
    return hash == NONE ? FALLBACK : hash;
  }
}
