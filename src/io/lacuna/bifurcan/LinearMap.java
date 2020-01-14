package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.DiffMap;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.*;
import java.util.Map;
import java.util.function.*;

import static io.lacuna.bifurcan.utils.Bits.log2Ceil;
import static java.lang.System.arraycopy;

/**
 * A hash-map implementation which uses Robin Hood hashing for placement, and allows for customized hashing and equality
 * semantics.  Performance is equivalent to {@link java.util.HashMap} for lookups and construction, and superior in the
 * case of poor hash distribution.  Because entries are stored contiguously, performance of {@link LinearMap#clone()} and
 * iteration is significantly better than {@link java.util.HashMap}.
 * <p>
 * The {@link LinearMap#entries()} method is O(1) and allows random access, returning an IList that proxies through to an
 * underlying array.  Partitioning this list is the most efficient way to process the collection in parallel.
 * <p>
 * However, {@code LinearMap} also exposes O(N) {@link #split(int)} and {@link #merge(IMap, BinaryOperator)}
 * methods, which despite their asymptotic complexity can be quite fast in practice.  The appropriate way to split this
 * collection will depend on the use case.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearMap<K, V> implements IMap<K, V>, Cloneable {

  /// Fields

  public static final int MAX_CAPACITY = 1 << 29;
  private static final float LOAD_FACTOR = 0.95f;

  private static final int NONE = 0;
  private static final int FALLBACK = 1;

  private final ToLongFunction<K> hashFn;
  private final BiPredicate<K, K> equalsFn;

  private int indexMask;
  long[] table;
  Object[] entries;
  private int size;
  private int hash = -1;

  /// Constructors

  /**
   * @return a copy of {@code map}
   */
  public static <K, V> LinearMap<K, V> from(java.util.Map<K, V> map) {
    return from(map.entrySet());
  }

  /**
   * @return a copy of {@code map}, with the same equality semantics
   */
  public static <K, V> LinearMap<K, V> from(IMap<K, V> map) {
    if (map instanceof LinearMap) {
      return ((LinearMap<K, V>) map).clone();
    } else {
      LinearMap<K, V> result = new LinearMap<K, V>((int) map.size(), map.keyHash(), map.keyEquality());
      map.forEach(e -> result.put(e.key(), e.value()));
      return result;
    }
  }

  /**
   * @return a map representing all elements in {@code entries}
   */
  public static <K, V> LinearMap<K, V> from(Iterable<IEntry<K, V>> entries) {
    return from(entries.iterator());
  }

  /**
   * @return a map representing all remaining entries in {@code iterator}
   */
  public static <K, V> LinearMap<K, V> from(Iterator<IEntry<K, V>> iterator) {
    LinearMap<K, V> m = new LinearMap<>();
    iterator.forEachRemaining(e -> m.put(e.key(), e.value()));
    return m;
  }

  /**
   * @return a map representing the entries in {@code collection}
   */
  public static <K, V> LinearMap<K, V> from(Collection<Map.Entry<K, V>> collection) {
    return collection.stream().collect(Maps.linearCollector(Map.Entry::getKey, Map.Entry::getValue, collection.size()));
  }

  public LinearMap() {
    this(16);
  }

  /**
   * @param initialCapacity the initial capacity of the map
   */
  public LinearMap(int initialCapacity) {
    this(initialCapacity, Maps.DEFAULT_HASH_CODE, Maps.DEFAULT_EQUALS);
  }

  /**
   * @param hashFn   a function which yields the hash value of keys
   * @param equalsFn a function which checks equality of keys
   */
  public LinearMap(ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    this(16, hashFn, equalsFn);
  }

  /**
   * @param initialCapacity the initial capacity of the map
   * @param hashFn          a function which yields the hash value of keys
   * @param equalsFn        a function which checks equality of keys
   */
  public LinearMap(int initialCapacity, ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    if (initialCapacity > MAX_CAPACITY) {
      throw new IllegalArgumentException("initialCapacity cannot be larger than " + MAX_CAPACITY);
    }

    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
    this.size = 0;

    resize(initialCapacity);
  }

  private LinearMap(int tableLength, int entriesLength, ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
    this.size = 0;

    resize(tableLength, entriesLength);
  }

  /// Accessors

  @Override
  public ToLongFunction<K> keyHash() {
    return hashFn;
  }

  @Override
  public BiPredicate<K, K> keyEquality() {
    return equalsFn;
  }

  @Override
  public LinearMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public LinearMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    if ((size << 1) == entries.length) {
      resize(size << 1);
    }
    put(keyHash(key), key, value, merge);
    hash = -1;

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
      hash = -1;
    }

    return this;
  }

  public LinearMap<K, V> clear() {
    Arrays.fill(entries, null);
    Arrays.fill(table, 0);
    size = 0;
    hash = -1;

    return this;
  }

  @Override
  public <U> LinearMap<K, U> mapValues(BiFunction<K, V, U> f) {
    LinearMap m = clone();
    for (int i = 0; i < m.size(); i++) {
      int idx = i << 1;
      m.entries[idx + 1] = f.apply((K) m.entries[idx], (V) m.entries[idx + 1]);
    }

    return m;
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
  public LinearMap<K, V> update(K key, UnaryOperator<V> update) {
    int idx = tableIndex(keyHash(key), key);
    if (idx >= 0) {
      long row = table[idx];
      int valIdx = Row.keyIndex(row) + 1;
      entries[valIdx] = update.apply((V) entries[valIdx]);
      hash = -1;
    } else {
      put(key, update.apply(null));
    }

    return this;
  }

  @Override
  public ISet<K> keys() {
    return new LinearSet<K>((LinearMap<K, Void>) this);
  }

  @Override
  public OptionalLong indexOf(K key) {
    int idx = tableIndex(keyHash(key), key);
    return idx >= 0 ? OptionalLong.of(Row.keyIndex(table[idx]) >> 1) : OptionalLong.empty();
  }

  @Override
  public IEntry<K, V> nth(long index) {
    int idx = ((int) index) << 1;
    return IEntry.of((K) entries[idx], (V) entries[idx + 1]);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public boolean isLinear() {
    return true;
  }

  @Override
  public IMap<K, V> forked() {
    return new DiffMap<>(this);
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
  public LinearMap<K, V> clone() {
    LinearMap<K, V> m = new LinearMap<K, V>(table.length, entries.length, hashFn, equalsFn);
    m.size = size;
    m.indexMask = indexMask;
    arraycopy(table, 0, m.table, 0, table.length);
    arraycopy(entries, 0, m.entries, 0, size << 1);
    return m;
  }

  @Override
  public int hashCode() {
    if (hash == -1) {
      hash = 0;
      for (long row : table) {
        if (Row.populated(row)) {
          V value = (V) entries[Row.keyIndex(row) + 1];
          hash += (Row.hash(row) * 31) + Objects.hashCode(value);
        }
      }
    }

    return hash;
  }

  @Override
  public String toString() {
    return Maps.toString(this);
  }

  @Override
  public List<LinearMap<K, V>> split(int parts) {
    parts = Math.min(parts, size);
    List<LinearMap<K, V>> list = new List<LinearMap<K, V>>().linear();
    if (parts <= 1) {
      return list.addLast(this).forked();
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

    return list.forked();
  }

  @Override
  public LinearMap<K, V> union(IMap<K, V> m) {
    return merge(m, Maps.MERGE_LAST_WRITE_WINS);
  }

  @Override
  public LinearMap<K, V> merge(IMap<K, V> m, BinaryOperator<V> mergeFn) {
    if (m.size() == 0) {
      return this.clone();
    } else if (m instanceof LinearMap && Maps.equivEquality(this, m)) {
      return merge((LinearMap<K, V>) m, mergeFn);
    } else {
      LinearMap<K, V> result = this.clone();
      for (IEntry<K, V> e : m.entries()) {
        result.put(e.key(), e.value(), mergeFn);
      }
      return result;
    }
  }

  @Override
  public LinearMap<K, V> difference(IMap<K, ?> m) {
    if (m instanceof LinearMap && Maps.equivEquality(this, m)) {
      return difference((LinearMap<K, ?>) m);
    } else {
      return (LinearMap<K, V>) Maps.difference(this.clone(), m.keys());
    }
  }

  @Override
  public IMap<K, V> difference(ISet<K> keys) {
    if (keys instanceof LinearSet && Maps.equivEquality(this, keys)) {
      return difference(((LinearSet<K>) keys).map);
    } else {
      return Maps.difference(this.clone(), keys);
    }
  }

  @Override
  public LinearMap<K, V> intersection(IMap<K, ?> m) {
    if (m instanceof LinearMap && Maps.equivEquality(this, m)) {
      return intersection((LinearMap<K, ?>) m);
    } else {
      return (LinearMap<K, V>) Maps.intersection(new LinearMap<>(), this, m.keys());
    }
  }

  @Override
  public LinearMap<K, V> intersection(ISet<K> keys) {
    if (keys instanceof LinearSet && Maps.equivEquality(this, keys)) {
      return intersection(((LinearSet<K>) keys).map);
    } else {
      return (LinearMap<K, V>) Maps.intersection(new LinearMap<>(), this, keys);
    }
  }

  @Override
  public boolean containsAll(ISet<K> set) {
    if (set instanceof LinearSet) {
      return isSubset(((LinearSet<K>) set).map);
    } else {
      return set.elements().stream().allMatch(this::contains);
    }
  }

  @Override
  public boolean containsAll(IMap<K, ?> map) {
    if (map instanceof LinearMap) {
      return isSubset((LinearMap<K, ?>) map);
    } else {
      return map.keys().stream().allMatch(this::contains);
    }
  }

  @Override
  public boolean containsAny(ISet<K> set) {

    if (size() < set.size()) {
      return set.containsAny(this);
    }

    if (set instanceof LinearSet) {
      return intersects(((LinearSet<K>) set).map);
    } else {
      return set.elements().stream().anyMatch(this::contains);
    }
  }

  @Override
  public boolean containsAny(IMap<K, ?> map) {

    if (size() < map.size()) {
      return map.containsAny(this);
    }

    if (map instanceof LinearMap) {
      return intersects((LinearMap<K, ?>) map);
    } else {
      return map.keys().stream().anyMatch(this::contains);
    }
  }

  /// Bookkeeping functions

  LinearMap<K, V> merge(LinearMap<K, V> m, BinaryOperator<V> mergeFn) {
    if (m.size > size) {
      return m.merge(this, (x, y) -> mergeFn.apply(y, x));
    }

    LinearMap<K, V> result = this.clone();

    result.resize(result.size + m.size);
    for (long row : m.table) {
      if (Row.populated(row)) {
        int keyIndex = Row.keyIndex(row);
        result.put(Row.hash(row), (K) m.entries[keyIndex], (V) m.entries[keyIndex + 1], mergeFn);
      }
    }
    return result;
  }

  LinearMap<K, V> difference(LinearMap<K, ?> m) {
    LinearMap<K, V> result = new LinearMap<>(size);
    combine(m, result, i -> i == -1);
    return result;
  }

  LinearMap<K, V> intersection(LinearMap<K, ?> m) {
    LinearMap<K, V> result = new LinearMap<>(Math.min(size, (int) m.size()));
    combine(m, result, i -> i != -1);
    return result;
  }

  private boolean isSubset(LinearMap<K, ?> m) {
    for (long row : m.table) {
      if (Row.populated(row)) {
        int currKeyIndex = Row.keyIndex(row);
        K currKey = (K) m.entries[currKeyIndex];
        if (m.tableIndex(Row.hash(row), currKey) == -1) {
          return false;
        }
      }
    }

    return true;
  }

  private boolean intersects(LinearMap<K, ?> m) {
    for (long row : m.table) {
      if (Row.populated(row)) {
        int currKeyIndex = Row.keyIndex(row);
        K currKey = (K) m.entries[currKeyIndex];
        if (m.tableIndex(Row.hash(row), currKey) != -1) {
          return true;
        }
      }
    }

    return true;
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

  private void resize(int tableLength, int entriesLength) {
    indexMask = tableLength - 1;

    // update table
    if (table == null) {
      table = new long[tableLength];
    } else if (table.length != tableLength) {
      long[] oldTable = table;
      this.table = new long[tableLength];
      for (long row : oldTable) {
        if (Row.populated(row)) {
          int hash = Row.hash(row);
          putTable(hash, Row.keyIndex(row), estimatedIndex(hash));
        }
      }
    }

    // update entries
    if (entries == null) {
      entries = new Object[entriesLength];
    } else {
      Object[] nEntries = new Object[entriesLength];
      arraycopy(entries, 0, nEntries, 0, size << 1);
      entries = nEntries;
    }
  }

  private void resize(int capacity) {

    if (capacity > MAX_CAPACITY) {
      throw new IllegalStateException("the map cannot be larger than " + MAX_CAPACITY);
    }

    capacity = Math.max(4, capacity);
    int tableLength = 1 << log2Ceil((long) Math.ceil(capacity / LOAD_FACTOR));
    resize(tableLength, capacity << 1);
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

  private void putTable(int hash, int keyIndex, int tableIndex) {
    int tombstoneIdx = -1;

    // TODO: remove abs
    for (int idx = tableIndex, dist = probeDistance(hash, tableIndex), abs = 0; ; idx = nextIndex(idx), dist++, abs++) {
      long row = table[idx];
      int currHash = Row.hash(row);
      boolean isTombstone = Row.tombstone(row);

      if (abs > table.length) {
        throw new IllegalStateException();
      }

      if (currHash == NONE) {
        table[idx] = Row.construct(hash, keyIndex);
        break;
      }

      int currDist = probeDistance(currHash, idx);
      if (!isTombstone && currDist > dist) {
        tombstoneIdx = -1;
      } else if (isTombstone && tombstoneIdx == -1) {
        tombstoneIdx = idx;
      }

      if (dist > currDist) {
        long nRow = Row.construct(hash, keyIndex);

        if (tombstoneIdx >= 0) {
          table[tombstoneIdx] = nRow;
          break;
        }

        table[idx] = nRow;
        if (isTombstone) {
          break;
        }

        dist = currDist;
        keyIndex = Row.keyIndex(row);
        hash = currHash;
      }
    }
  }

  private void putEntry(int keyIndex, K key, V value) {
    entries[keyIndex] = key;
    entries[keyIndex + 1] = value;
  }

  private void putTable(int hash, int keyIndex) {
    putTable(hash, keyIndex, estimatedIndex(hash));
  }

  // factored out for better inlining
  private boolean putCheckEquality(int idx, K key, V value, BinaryOperator<V> mergeFn) {
    long row = table[idx];
    int keyIndex = Row.keyIndex(row);
    K currKey = (K) entries[keyIndex];
    if (equalsFn.test(key, currKey)) {
      entries[keyIndex + 1] = mergeFn.apply((V) entries[keyIndex + 1], value);
      return true;
    } else {
      return false;
    }
  }

  private void put(int hash, K key, V value, BinaryOperator<V> mergeFn) {
    int tombstoneIdx = -1;
    for (int idx = estimatedIndex(hash), dist = 0; ; idx = nextIndex(idx), dist++) {
      long row = table[idx];
      int currHash = Row.hash(row);
      boolean isNone = currHash == NONE;
      boolean isTombstone = Row.tombstone(row);

      if (currHash == hash && !isTombstone && putCheckEquality(idx, key, value, mergeFn)) {
        break;
      }

      int currDist = probeDistance(currHash, idx);
      if (!isTombstone && currDist > dist) {
        tombstoneIdx = -1;
      } else if (isTombstone && tombstoneIdx == -1) {
        tombstoneIdx = idx;
      }

      if (isNone || dist > currDist) {

        // we know there isn't any collision, so add it to the end
        int keyIndex = size << 1;
        putEntry(keyIndex, key, value);
        size++;

        long nRow = Row.construct(hash, keyIndex);
        if (tombstoneIdx >= 0) {
          table[tombstoneIdx] = nRow;
        } else if (isNone || isTombstone) {
          table[idx] = nRow;
        } else {
          putTable(hash, keyIndex, idx);
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
    long hash64 = hashFn.applyAsLong(key);
    int hash = (int) ((hash64 >> 32) ^ hash64);

    // make sure we don't have too many collisions in the lower bits
    hash ^= (hash >>> 20) ^ (hash >>> 12);
    hash ^= (hash >>> 7) ^ (hash >>> 4);
    return hash == NONE ? FALLBACK : hash;
  }
}
