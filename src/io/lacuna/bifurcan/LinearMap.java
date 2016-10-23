package io.lacuna.bifurcan;

import io.lacuna.bifurcan.Maps.Entry;
import io.lacuna.bifurcan.utils.BitVector;
import io.lacuna.bifurcan.utils.Bits;

import java.util.Objects;
import java.util.Optional;
import java.util.function.*;

import static io.lacuna.bifurcan.utils.Bits.maskBelow;

/**
 * A hash-map implementation which uses Robin Hood hashing for placement, and provides flexibility as to hashing and
 * equality mechanisms. The number of bits used in the hash can also be configured, allowing a tradeoff between memory
 * usage and number of spurious equality checks on writes and reads.
 *
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearMap<K, V> implements IMap<K, V> {

  private static final float MAX_LOAD_FACTOR = 0.85f;

  private static final long NONE = 0;
  private static final long FALLBACK = 1;

  private final ToLongFunction<K> hashFn;
  private final BiPredicate<K, K> equalsFn;
  private final byte indexBits;
  private final byte hashBits;

  private final long[] table;
  private final IEntry<K, V>[] entries;
  private int size;

  public LinearMap() {
    this(16, 32, Objects::hashCode, Objects::equals);
  }

  /**
   * Creates a map from an existing {@code java.util.Map}, using the default Java hashing and equality mechanisms.
   *
   * @param m an existing {@code java.util.Map}
   */
  public LinearMap(java.util.Map<K, V> m) {
    this(Bits.log2Ceil(m.size()), 32, Objects::hashCode, Objects::equals);
    m.entrySet().forEach(e -> put(e.getKey(), e.getValue()));
  }

  /**
   * Creates a map from an existing data structure, using the default Java hashing and equality mechanisms.
   *
   * @param m an existing map
   */
  public LinearMap(IMap<K, V> m) {
    this(m, 32, Objects::hashCode, Objects::equals);
  }

  /**
   * @param m another map
   * @param hashBits the number of significant bits in the hash
   * @param hashFn the hashing function
   * @param equalsFn the equality function
   */
  public LinearMap(IMap<K, V> m, int hashBits, ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    this(Bits.log2Ceil(m.size()), hashBits, hashFn, equalsFn);
    m.entries().stream().forEach(e -> put(e.key(), e.value()));
  }

  private LinearMap(int capacity, int hashBits, ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
    if (!Bits.isPowerOfTwo(capacity)) {
      throw new IllegalArgumentException("capacity must be power of two");
    }

    if (hashBits < 2 || hashBits > 64) {
      throw new IllegalArgumentException("hashBits must be within [2, 64]");
    }

    this.indexBits = (byte) Bits.bitOffset(capacity);
    this.hashBits = (byte) hashBits;
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;

    this.entries = new IEntry[capacity];
    this.table = BitVector.create((indexBits + hashBits + 1) * capacity);
    this.size = 0;
  }

  private LinearMap<K, V> resize(int capacity) {
    LinearMap<K, V> m = new LinearMap<>(capacity, hashBits, hashFn, equalsFn);

    Biterator it = biterator(0);
    int idx = it.next();
    do {
      long hash = tableHash(idx);
      if (hash != NONE && !isTombstone(idx)) {
        m = m.put(hash, entries[tableIndex(idx)]);
      }
      idx = it.next();
    } while (idx != 0);

    return m;
  }

  private int possibleKeyIndex(long hash) {
    Biterator biterator = preferredBiterator(hash);
    for (; ; ) {
      int idx = biterator.next();
      long currHash = tableHash(idx);
      if (currHash == NONE || hash == currHash || pastProbeLength(idx, hash, currHash)) {
        return idx;
      }
    }
  }

  private int keyIndex(K key) {
    long hash = keyHash(key);
    Biterator it = biterator(possibleKeyIndex(hash));

    for (; ; ) {
      int idx = it.next();
      long currHash = tableHash(idx);

      if (isTombstone(idx)) {
        continue;
      } else if (currHash == hash && equalsFn.test(entry(idx).key(), key)) {
        return idx;
      } else if (currHash == NONE || pastProbeLength(idx, hash, currHash)) {
        return -1;
      }
    }
  }

  private LinearMap<K, V> put(long hash, IEntry<K, V> entry) {
    Biterator it = preferredBiterator(hash);
    for (; ; ) {
      int idx = it.next();
      long currHash = tableHash(idx);
      boolean tombstone = isTombstone(idx);

      if (!tombstone && currHash == hash) {
        // potential match, check for set
        int currIndex = tableIndex(idx);
        IEntry<K, V> currEntry = entries[currIndex];
        if (equalsFn.test(entry.key(), currEntry.key())) {
          entries[currIndex] = entry;
          setTombstone(idx, false);
          break;
        }
      } else if (currHash == NONE || pastProbeLength(idx, hash, currHash)) {
        // if it's empty, or it's a tombstone and there's no possible exact match further down, use it
        if (tombstone || currHash == NONE) {
          entries[size] = entry;
          overwriteTable(idx, hash, size);
          size++;
          break;
        }

        // we deserve this location more, so swap them out
        int currIndex = tableIndex(idx);
        IEntry<K, V> currEntry = entries[currIndex];

        entries[currIndex] = entry;
        overwriteTableHash(idx, hash);

        entry = currEntry;
        hash = currHash;
      }
    }

    return this;
  }

  @Override
  public IMap<K, V> put(K key, V value) {
    if (size > (int) (entries.length * MAX_LOAD_FACTOR)) {
      return resize(entries.length << 1).put(key, value);
    }
    return put(keyHash(key), new Entry<>(key, value));
  }

  @Override
  public IMap<K, V> remove(K key) {
    int idx = keyIndex(key);
    if (idx >= 0) {
      size--;
      int entryIndex = tableIndex(idx);
      if (entryIndex != size) {
        IEntry<K, V> lastEntry = entries[size];
        int lastIdx = keyIndex(lastEntry.key());
        overwriteTableIndex(lastIdx, entryIndex);
        entries[entryIndex] = lastEntry;
      }
      setTombstone(idx, true);
      entries[size] = null;
    }

    return this;
  }

  @Override
  public Optional<V> get(K key) {
    int idx = keyIndex(key);
    if (idx < 0) {
      return Optional.empty();
    } else {
      return Optional.of(entries[tableIndex(idx)].value());
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

  /// internal accessors

  private interface Biterator {
    int next();
  }

  private int stride() {
    return hashBits + indexBits + 1;
  }

  private long keyHash(K key) {
    long hash = hashFn.applyAsLong(key) & maskBelow(hashBits);
    return hash == NONE ? FALLBACK : hash;
  }

  private boolean isTombstone(int bitIndex) {
    return BitVector.test(table, bitIndex + hashBits + indexBits);
  }

  private void setTombstone(int bitIndex, boolean flag) {
    BitVector.overwrite(table, bitIndex + hashBits + indexBits, flag);
  }

  private long tableHash(int bitIndex) {
    return BitVector.get(table, bitIndex, hashBits);
  }

  private int tableIndex(int bitIndex) {
    return (int) BitVector.get(table, bitIndex + hashBits, indexBits);
  }

  private IEntry<K, V> entry(int bitIndex) {
    return entries[tableIndex(bitIndex)];
  }

  private void overwriteTable(int bitIndex, long hash, int index) {
    // set hash, index, and zero out tombstone bit
    BitVector.overwrite(table, ((long) index << hashBits) | hash, bitIndex, hashBits + indexBits + 1);
  }

  private void overwriteTableHash(int bitIndex, long hash) {
    BitVector.overwrite(table, hash, bitIndex, hashBits);
  }

  private void overwriteTableIndex(int bitIndex, int index) {
    BitVector.overwrite(table, index, bitIndex + hashBits, indexBits);
  }

  private Biterator biterator(int start) {
    return new Biterator() {
      private final int stride = indexBits + hashBits + 1;
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

  // returns a biterator starting at the preferred location for the given hash
  private Biterator preferredBiterator(long hash) {
    return biterator(stride() * (int) (hash & maskBelow(indexBits)));
  }

  private boolean pastProbeLength(int bitIdx, long a, long b) {
    int idx = bitIdx / stride();
    long mask = maskBelow(indexBits);
    int ap = (int) (a & mask);
    int bp = (int) (b & mask);

    int aDist = ap <= idx ? idx - ap : (entries.length - ap) + idx;
    int bDist = bp <= idx ? idx - bp : (entries.length - bp) + idx;

    return aDist > bDist;
  }

}
