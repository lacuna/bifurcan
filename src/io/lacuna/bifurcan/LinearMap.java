package io.lacuna.bifurcan;

import io.lacuna.bifurcan.Maps.Entry;
import io.lacuna.bifurcan.utils.BitVector;
import io.lacuna.bifurcan.utils.Bits;

import java.util.Objects;
import java.util.Optional;
import java.util.function.*;

import static io.lacuna.bifurcan.utils.Bits.log2Ceil;
import static io.lacuna.bifurcan.utils.Bits.maskBelow;

/**
 * A hash-map implementation which uses Robin Hood hashing for placement, and provides flexibility as to hashing and
 * equality mechanisms. The number of bits used in the hash can also be configured, allowing a trade-off between memory
 * usage and number of spurious equality checks on writes and reads.
 * <p>
 * Unlike {@code HashMap.entrySet()}, the {@code entries()} method is O(1), returning an IList that proxies through to the
 * underlying {@code entries} array, which is guaranteed to be densely packed.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearMap<K, V> implements IMap<K, V> {

  // This is an unusually low load factor, but it only impacts the size of the bit-packed table[], not the size of the
  // entries[] array.  Where normally this might imply a 66% memory overhead, in practice it represents <10% overhead,
  // since table[] consumes an order of magnitude less memory than entries[] even when each Entry only contains a
  // pair of Longs.  In almost any situation, this should only ever be reduced, yielding faster construction.
  public static final float DEFAULT_LOAD_FACTOR = 0.6f;

  public static final int DEFAULT_HASH_BITS = 32;

  private static final long NONE = 0;
  private static final long FALLBACK = 1;

  private final ToLongFunction<K> hashFn;
  private final BiPredicate<K, K> equalsFn;
  private final byte indexBits;
  private final byte hashBits;
  private final long hashMask;
  private final long indexMask;
  private final float loadFactor;
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
    this(initialCapacity, loadFactor, DEFAULT_HASH_BITS, Objects::hashCode, Objects::equals);
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

  public LinearMap(int initialCapacity, float loadFactor, int hashBits, ToLongFunction<K> hashFn, BiPredicate<K, K> equalsFn) {

    initialCapacity = (int) (1L << log2Ceil(initialCapacity));

    if (hashBits < 2 || hashBits > 64) {
      throw new IllegalArgumentException("hashBits must be within [2, 64]");
    }

    if (loadFactor < 0 || loadFactor > 1) {
      throw new IllegalArgumentException("loadFactor must be within [0,1]");
    }

    this.indexBits = (byte) Bits.bitOffset(initialCapacity);
    this.hashBits = (byte) hashBits;
    this.hashFn = hashFn;
    this.equalsFn = equalsFn;
    this.loadFactor = loadFactor;
    this.threshold = (int) (initialCapacity * loadFactor);

    this.entries = new IEntry[threshold + 1];
    this.table = BitVector.create((indexBits + hashBits + 1) * initialCapacity);
    this.size = 0;
    this.hashMask = maskBelow(hashBits);
    this.indexMask = maskBelow(indexBits);
  }

  private static int necessaryCapacity(int size, float loadFactor) {
    return (int) (1L << log2Ceil((long) Math.ceil(size / loadFactor)));
  }

  private LinearMap<K, V> resize(int capacity) {
    LinearMap<K, V> m = new LinearMap<>(capacity, loadFactor, hashBits, hashFn, equalsFn);
    System.arraycopy(entries, 0, m.entries, 0, size);
    m.size = size;

    Biterator it = new Biterator(0);
    do {
      if (it.hash != NONE && !it.tombstone) {
        m.naivePut(it.hash, it.entryIndex());
      }
      it.next();
    } while (it.index != 0);

    return m;
  }

  private Biterator biterateTo(K key) {
    long hash = keyHash(key);
    Biterator it = new Biterator(hash);

    for (; ; ) {
      if (it.hash == hash && !it.tombstone && Objects.equals(it.entry().key(), key)) {
        return it;
      } else if (it.hash == NONE || it.tooFar(hash)) {
        return null;
      }
      it.next();
    }
  }

  private void naivePut(long hash, int index) {
    Biterator it = new Biterator(hash);
    for (; ; ) {
      if (it.hash == NONE) {
        it.overwrite(hash, index);
        break;
      } else if (it.tooFar(hash)) {
        int tmpIndex = it.entryIndex();
        long tmpHash = it.hash;
        it.overwrite(hash, index);

        index = tmpIndex;
        hash = tmpHash;
      }
      it.next();
    }
  }

  // factored out for better inlining
  private boolean putCheckEquality(Biterator it, IEntry<K, V> entry) {
    int currIndex = it.entryIndex();
    IEntry<K, V> currEntry = entries[currIndex];
    if (equalsFn.test(entry.key(), currEntry.key())) {
      entries[currIndex] = entry;
      it.tombstone(false);
      return true;
    } else {
      return false;
    }
  }

  private void put(long hash, IEntry<K, V> entry) {
    Biterator it = new Biterator(hash);
    for (; ; ) {

      boolean isNone = it.hash == NONE;

      if (it.hash == hash && !it.tombstone && putCheckEquality(it, entry)) {
        break;
      } else if (isNone || it.tooFar(hash)) {
        // if it's empty, or it's a tombstone and there's no possible exact match further down, use it
        if (isNone || it.tombstone) {
          entries[size] = entry;
          it.overwrite(hash, size);
          size++;
          break;
        }

        // we deserve this location more, so swap them out
        int currIndex = it.entryIndex();
        IEntry<K, V> currEntry = entries[currIndex];
        entries[currIndex] = entry;
        it.overwrite(hash, currIndex);

        entry = currEntry;
        hash = it.hash;
      }

      it.next();
    }
  }

  @Override
  public IMap<K, V> put(K key, V value) {
    if (size >= threshold) {
      return resize(capacity() << 1).put(key, value);
    } else {
      put(keyHash(key), new Entry<>(key, value));
      return this;
    }
  }

  @Override
  public IMap<K, V> remove(K key) {
    Biterator it = biterateTo(key);
    if (it != null) {
      size--;
      int entryIdx = it.entryIndex();
      if (entryIdx != size) {
        IEntry<K, V> lastEntry = entries[size];
        Biterator lastIt = biterateTo(lastEntry.key());
        lastIt.overwrite(lastIt.hash, entryIdx);
        entries[entryIdx] = lastEntry;
      }
      it.tombstone(true);
      entries[size] = null;
    }

    return this;
  }

  @Override
  public Optional<V> get(K key) {
    long hash = keyHash(key);
    Biterator it = new Biterator(hash);

    for (; ; ) {
      if (it.hash == hash && !it.tombstone) {
        IEntry<K, V> entry = it.entry();
        if (equalsFn.test(entry.key(), key)) {
          return Optional.of(entry.value());
        }
      } else if (it.hash == NONE || it.tooFar(hash)) {
        return Optional.empty();
      }
      it.next();
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

  private class Biterator {

    int index;
    long hash;
    boolean tombstone;

    Biterator(long hash) {
      this.index = (int) (hash & indexMask);
      this.hash = BitVector.get(table, bitIndex(), hashBits);
      this.tombstone = BitVector.test(table, bitIndex() + hashBits + indexBits);
    }

    void next() {
      index = (index + 1) & (int) indexMask;
      hash = BitVector.get(table, bitIndex(), hashBits);
      tombstone = BitVector.test(table, bitIndex() + hashBits + indexBits);
    }

    int bitIndex() {
      return index * stride();
    }

    int entryIndex() {
      return (int) BitVector.get(table, bitIndex() + hashBits, indexBits);
    }

    void tombstone(boolean flag) {
      BitVector.overwrite(table, bitIndex() + hashBits + indexBits, flag);
    }

    void overwrite(long hash, int index) {
      BitVector.overwrite(table, ((long) index << hashBits) | hash, bitIndex(), hashBits + indexBits + 1);
    }

    IEntry<K, V> entry() {
      return entries[entryIndex()];
    }

    boolean tooFar(long initHash) {
      return probeDistance(initHash, index) > probeDistance(hash, index);
    }
  }

  private int capacity() {
    return 1 << indexBits;
  }

  private int probeDistance(long hash, int index) {
    return (int) ((index + capacity() - (hash & indexMask)) & indexMask);
  }

  private int stride() {
    return hashBits + indexBits + 1;
  }

  private long keyHash(K key) {
    long hash = hashFn.applyAsLong(key) & hashMask;

    // make sure we don't have too many collisions in the lower bits
    hash ^= (hash >>> 20) ^ (hash >>> 12);
    hash ^= (hash >>> 7) ^ (hash >>> 4);
    return hash == NONE ? FALLBACK : hash;
  }

}
