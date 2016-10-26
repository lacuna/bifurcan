package io.lacuna.bifurcan.utils;

import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.IMap;
import io.lacuna.bifurcan.IPartitionable;
import io.lacuna.bifurcan.LinearList;

import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

import static io.lacuna.bifurcan.utils.Bits.log2Ceil;

/**
 * @author ztellman
 */
public class HashIndex<V> implements IPartitionable<HashIndex<V>> {

    public static final float DEFAULT_LOAD_FACTOR = 0.95f;

    private static final int NONE = 0;
    private static final int FALLBACK = 1;

    private final ToIntFunction<V> hashFn;
    private final BiPredicate<V, V> equalsFn;
    private final int tableMask;
    private final float loadFactor;

    private long[] table;
    private Object[] values;
    private int size;

    public HashIndex(int initialCapacity, float loadFactor, ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {

        if (loadFactor < 0 || loadFactor > 1) {
            throw new IllegalArgumentException("loadFactor must be within (0,1)");
        }

        initialCapacity = Math.max(4, initialCapacity);
        int tableLength = (int) (1L << log2Ceil((long) Math.ceil(initialCapacity / loadFactor)));

        this.loadFactor = loadFactor;
        this.tableMask = tableLength - 1;
        this.hashFn = hashFn;
        this.equalsFn = equalsFn;

        this.values = new IMap.IEntry[initialCapacity];
        this.table = new long[tableLength];
        this.size = 0;
    }

    private HashIndex<V> resize(int capacity) {
        HashIndex<V> h = new HashIndex<>(capacity, loadFactor, hashFn, equalsFn);
        System.arraycopy(values, 0, h.values, 0, size);
        h.size = size;

        for (int idx = 0; idx < table.length; idx++) {
            long row = table[idx];
            if (Row.populated(row)) {
                h.constructAdd(Row.hash(row), Row.valueIndex(row));
            }
        }

        return h;
    }

    private int indexFor(V val) {
        int hash = valHash(val);

        for (int idx = indexFor(hash), dist = 0; ; idx = nextIndex(idx), dist++) {
            long row = table[idx];
            int currHash = Row.hash(row);
            if (currHash == hash && !Row.tombstone(row) && equalsFn.test(val, rowValue(row))) {
                return idx;
            } else if (currHash == NONE || dist > probeDistance(currHash, idx)) {
                return -1;
            }
        }
    }

    private void constructAdd(int hash, int valIndex) {
        for (int idx = indexFor(hash), dist = 0; ; idx = nextIndex(idx), dist++) {
            long row = table[idx];
            int currHash = Row.hash(row);
            if (currHash == NONE) {
                table[idx] = Row.construct(hash, valIndex);
                break;
            } else if (dist > probeDistance(currHash, idx)) {
                int currValIndex = Row.valueIndex(row);
                table[idx] = Row.construct(hash, valIndex);

                dist = probeDistance(currHash, idx);
                valIndex = currValIndex;
                hash = currHash;
            }
        }
    }

    // factored out for better inlining
    private boolean putCheckEquality(int idx, V val) {
        long row = table[idx];
        int currIndex = Row.valueIndex(row);
        V currVal = value(currIndex);
        if (equalsFn.test(val, currVal)) {
            values[currIndex] = val;
            table[idx] = Row.removeTombstone(row);
            return true;
        } else {
            return false;
        }
    }

    private void put(int hash, V val) {
        for (int idx = indexFor(hash), dist = 0; ; idx = nextIndex(idx), dist++) {
            long row = table[idx];
            int currHash = Row.hash(row);
            boolean isNone = currHash == NONE;
            boolean currTombstone = Row.tombstone(row);

            if (currHash == hash && !currTombstone && putCheckEquality(idx, val)) {
                break;
            } else if (isNone || dist > probeDistance(currHash, idx)) {
                // if it's empty, or it's a tombstone and there's no possible exact match further down, use it
                if (isNone || currTombstone) {
                    values[size] = val;
                    table[idx] = Row.construct(hash, size);
                    size++;
                    break;
                }

                // we deserve this location more, so swap them out
                int currValIndex = Row.valueIndex(row);
                V currVal = value(currValIndex);
                values[currValIndex] = val;
                table[idx] = Row.construct(hash, currValIndex);

                dist = probeDistance(currHash, idx);
                val = currVal;
                hash = currHash;
            }
        }
    }

    public HashIndex<V> put(V val) {
        if (size == this.values.length) {
            return resize(this.values.length << 1).put(val);
        } else {
            put(valHash(val), val);
            return this;
        }
    }

    public void remove(V val) {
        int idx = indexFor(val);
        if (idx >= 0) {
            long row = table[idx];
            size--;
            int valIndex = Row.valueIndex(row);
            if (valIndex != size) {
                V lastVal = value(size);
                int lastIdx = indexFor(lastVal);
                table[lastIdx] = Row.construct(Row.hash(table[lastIdx]), valIndex);
                values[valIndex] = lastVal;
            }
            table[idx] = Row.addTombstone(row);
            values[size] = null;
        }
    }

    public <K> Optional<V> get(V val) {
        int hash = valHash(val);

        for (int idx = indexFor(hash), dist = 0; ; idx = nextIndex(idx), dist++) {
            long row = table[idx];
            int currHash = Row.hash(row);

            if (currHash == hash && !Row.tombstone(row)) {
                V currVal = rowValue(row);
                if (equalsFn.test(val, currVal)) {
                    return Optional.of(currVal);
                }
            } else if (currHash == NONE || dist > probeDistance(currHash, idx)) {
                return Optional.empty();
            }
        }
    }

    public long size() {
        return size;
    }

    @Override
    public IList<HashIndex<V>> partition(int parts) {
        parts = Math.min(parts, size);
        IList<HashIndex<V>> list = new LinearList<>(parts);
        if (parts == 0) {
            return list.append(this);
        }

        int partSize = table.length / parts;
        for (int p = 0; p < parts; p++) {
            int start = p * partSize;
            int finish = (p == (parts - 1)) ? table.length : start + partSize;

            HashIndex<V> coll = new HashIndex<>(finish - start, loadFactor, hashFn, equalsFn);
            list.append(coll);

            for (int i = start; i < finish; i++) {
                long row = table[i];
                if (Row.populated(row)) {
                    coll.values[coll.size] = this.values[Row.valueIndex(row)];
                    coll.constructAdd(Row.hash(row), coll.size++);
                }
            }
        }

        return list;
    }

    @Override
    public HashIndex<V> merge(HashIndex<V> o) {
        if (o.size() <= size()) {
            HashIndex<V> coll = resize((int) (size + o.size()));
            for (int i = 0; i < o.table.length; i++) {
                long row = o.table[i];
                if (Row.populated(row)) {
                    coll.put(Row.hash(row), o.rowValue(row));
                }
            }
            return coll;
        } else {
            return o.merge(this);
        }
    }

    public void debug() {
        for (int i = 0; i < table.length; i++) {
            long row = table[i];
            System.out.println(i + " " + Row.tombstone(row) + " " + Row.hash(row) + " | " + rowValue(row));
        }
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

        static boolean populated(long row) {
            return (row & HASH_MASK) != NONE && (row & TOMBSTONE_MASK) == 0;
        }

        static int valueIndex(long row) {
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

    private V rowValue(long row) {
        return (V) values[Row.valueIndex(row)];
    }

    public V value(int index) {
        return (V) values[index];
    }

    private int indexFor(int hash) {
        return hash & tableMask;
    }

    private int nextIndex(int idx) {
        return (idx + 1) & tableMask;
    }

    private int probeDistance(int hash, int index) {
        return (index + table.length - (hash & tableMask)) & tableMask;
    }

    private int valHash(V val) {
        return mixHash(hashFn.applyAsInt(val));
    }

    public static int mixHash(int hash) {
        hash ^= (hash >>> 20) ^ (hash >>> 12);
        hash ^= (hash >>> 7) ^ (hash >>> 4);
        return hash == NONE ? FALLBACK : hash;
    }
}
