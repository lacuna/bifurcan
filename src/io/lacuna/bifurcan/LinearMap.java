package io.lacuna.bifurcan;

import io.lacuna.bifurcan.Maps.Entry;
import io.lacuna.bifurcan.utils.HashIndex;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

/**
 * A hash-map implementation which uses Robin Hood hashing for placement, and allows for customized hashing and equality
 * semantics.  Performance is moderately faster than {@code java.util.HashMap}, and much better in the worst case of
 * poor hash distribution.
 * <p>
 * The {@code entries()} method is O(1), returning an IList that proxies through to the underlying {@code entries},
 * which are in a densely packed array.  Partitioning this list is the most efficient way to process the collection in
 * parallel.
 * <p>
 * However, {@code LinearMap} also exposes O(N) {@code partition()} and {@code merge()} methods, which despite their
 * asymptotic complexity can be quite fast in practice.  The appropriate way to partition this collection will depend
 * on the use case.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearMap<K, V> implements IMap<K, V>, IPartitionable<LinearMap<K, V>> {

    public HashIndex<IEntry<K, V>> index;

    public LinearMap() {
        this(8);
    }

    public LinearMap(int initialCapacity) {
        this(initialCapacity, HashIndex.DEFAULT_LOAD_FACTOR);
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
        this(m.size());
        m.entrySet().forEach(e -> put(e.getKey(), e.getValue()));
    }

    public LinearMap(IMap<K, V> m) {
        this((int) m.size());
        m.entries().stream().forEach(e -> put(e.key(), e.value()));
    }

    public LinearMap(int initialCapacity, float loadFactor, ToIntFunction<K> hashFn, BiPredicate<K, K> equalsFn) {
        this(new HashIndex<>(
                        initialCapacity,
                        loadFactor,
                        e -> hashFn.applyAsInt(e.key()),
                        (a, b) -> equalsFn.test(a.key(), b.key())));
    }

    private LinearMap(HashIndex<IEntry<K, V>> index) {
        this.index = index;
    }


    @Override
    public IMap<K, V> put(K key, V value) {
        index = index.put(new Entry<>(key, value));
        return this;
    }

    @Override
    public IMap<K, V> remove(K key) {
        index.remove(new Entry<>(key, null));
        return this;
    }

    @Override
    public Optional<V> get(K key) {
        return index.get(new Entry<>(key, null)).map(IEntry::value);
    }

    @Override
    public IList<IEntry<K, V>> entries() {
        return Lists.from(size(), i -> index.value((int) i));
    }

    @Override
    public IMap<K, V> forked() {
        throw new UnsupportedOperationException("a LinearMap cannot be efficiently transformed into a forked representation");
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

    @Override
    public IList<LinearMap<K, V>> partition(int parts) {
        return index.partition(parts).stream().map(idx -> new LinearMap<>(idx)).collect(Lists.linearCollector());
    }

    @Override
    public LinearMap<K, V> merge(LinearMap<K, V> o) {
        return new LinearMap<>(index.merge(o.index));
    }

    public ISet<K> keys() {
        return new KeySet();
    }

    @Override
    public long size() {
        return index.size();
    }

    /// key set

    private class KeySet implements ISet<K> {

        public KeySet() {
        }

        @Override
        public ISet<K> add(K value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ISet<K> remove(K value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(K value) {
            return get(value).isPresent();
        }

        @Override
        public long size() {
            return LinearMap.this.size();
        }

        @Override
        public IList<K> elements() {
            return Lists.from(size(), i -> index.value((int) i).key());
        }

        @Override
        public ISet<K> union(ISet<K> s) {
            return null;
        }

        @Override
        public ISet<K> difference(ISet<K> s) {
            return null;
        }

        @Override
        public ISet<K> intersection(ISet<K> s) {
            return null;
        }

        @Override
        public Set<K> toSet() {
            return null;
        }

        @Override
        public ISet<K> forked() {
            throw new UnsupportedOperationException("Cannot efficiently create a forked version of this collection");
        }

        @Override
        public ISet<K> linear() {
            return this;
        }

        @Override
        public IList<ISet<K>> partition(int parts) {
            return LinearMap.this.partition(parts).stream().map(LinearMap::keys).collect(Lists.linearCollector());
        }

        @Override
        public ISet<K> merge(ISet<K> collection) {
            return union(collection);
        }
    }
}
