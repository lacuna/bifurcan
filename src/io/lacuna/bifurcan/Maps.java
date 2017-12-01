package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IMap.IEntry;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

import static io.lacuna.bifurcan.Lists.lazyMap;

/**
 * * Utility functions for classes implementing {@code IMap}.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class Maps {

  private static final Object DEFAULT_VALUE = new Object();

  public static BinaryOperator MERGE_LAST_WRITE_WINS = (a, b) -> b;

  public static final IMap EMPTY = new IMap() {
    @Override
    public Object get(Object key, Object defaultValue) {
      return defaultValue;
    }

    @Override
    public boolean contains(Object key) {
      return false;
    }

    @Override
    public IList<IEntry> entries() {
      return Lists.EMPTY;
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public IMap put(Object key, Object value, BinaryOperator merge) {
      return new Map().put(key, value, merge);
    }

    @Override
    public IMap remove(Object key) {
      return this;
    }

    @Override
    public IMap forked() {
      return this;
    }

    @Override
    public IMap linear() {
      return new Map().linear();
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IMap) {
        return ((IMap) obj).size() == 0;
      }
      return false;
    }

    @Override
    public String toString() {
      return Maps.toString(this);
    }
  };

  public static class Entry<K, V> implements IEntry<K, V> {
    public final K key;
    public final V value;

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

    @Override
    public String toString() {
      return key + " = " + value;
    }

    @Override
    public int hashCode() {
      return (Objects.hash(key) * 31) + Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IEntry) {
        IEntry<K, V> e = (IEntry<K, V>) obj;
        return Objects.equals(key, e.key()) && Objects.equals(value, e.value());
      }
      return false;
    }
  }

  static class VirtualMap<K, V> implements IMap<K, V> {

    private IMap<K, V> canonical = null;

    private IMap<K, V> base, added;
    private ISet<K> removed, shadowed;
    private final boolean linear;

    public VirtualMap(IMap<K, V> base) {
      this(base, Maps.EMPTY, Sets.EMPTY, Sets.EMPTY, false);
    }

    private VirtualMap(IMap<K, V> base, IMap<K, V> added, ISet<K> removed, ISet<K> shadowed, boolean linear) {
      this.base = base;
      this.added = added;
      this.removed = removed;
      this.shadowed = shadowed;
      this.linear = linear;
    }

    private void canonicalize() {
      if (canonical != null) {
        return;
      }

      canonical = Map.from(base).union(added).difference(removed);
      if (linear) {
        canonical = canonical.linear();
      }

      // don't hold onto more memory than we have to
      base = null;
      added = null;
      shadowed = null;
      removed = null;
    }

    private boolean altered() {
      return shadowed.size() > 0 || removed.size() > 0;
    }

    @Override
    public synchronized V get(K key, V defaultValue) {
      if (canonical != null) {
        return canonical.get(key, defaultValue);
      } else if (removed.contains(key)) {
        return defaultValue;
      } else {
        V val = added.get(key, defaultValue);
        return val == defaultValue ? base.get(key, defaultValue) : val;
      }
    }

    @Override
    public synchronized IMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
      if (canonical != null) {
        return canonical.put(key, value, merge);
      } else if (added.contains(key) || !base.contains(key)) {
        IMap<K, V> addedPrime = added.put(key, value, merge);
        return linear ? this : new VirtualMap<K, V>(base, addedPrime, removed, shadowed, false);
      } else {
        IMap<K, V> addedPrime = added.put(key, merge.apply(added.get(key).orElse(null), value));
        ISet<K> shadowedPrime = shadowed.add(key);
        ISet<K> removedPrime = removed.remove(key);

        return linear ? this : new VirtualMap<K, V>(base, addedPrime, removedPrime, shadowedPrime, false);
      }
    }

    @Override
    public synchronized IMap<K, V> remove(K key) {
      if (canonical != null) {
        return canonical.remove(key);
      } else if (!contains(key)) {
        return this;
      } else {
        IMap<K, V> addedPrime = added.remove(key);

        if (shadowed.contains(key)) {
          ISet<K> shadowedPrime = shadowed.remove(key);
          ISet<K> removedPrime = removed.add(key);
          return linear ? this : new VirtualMap<K, V>(base, addedPrime, removedPrime, shadowedPrime, false);
        } else {
          return linear ? this : new VirtualMap<K, V>(base, addedPrime, removed, shadowed, false);
        }
      }
    }

    @Override
    public synchronized IMap<K, V> forked() {
      if (canonical != null) {
        return canonical.forked();
      } else {
        return linear ? new VirtualMap<K, V>(base, added.forked(), removed.forked(), shadowed.forked(), false) : this;
      }
    }

    @Override
    public synchronized IMap<K, V> linear() {
      if (canonical != null) {
        return canonical.linear();
      } else {
        return linear ? this : new VirtualMap<K, V>(base, added.linear(), removed.linear(), shadowed.linear(), true);
      }
    }

    @Override
    public synchronized boolean contains(K key) {
      if (canonical != null) {
        return canonical.contains(key);
      } else {
        return added.contains(key) || (!removed.contains(key) && base.contains(key));
      }
    }

    @Override
    public synchronized Iterator<IEntry<K, V>> iterator() {
      if (canonical != null) {
        return canonical.iterator();
      } else if (!altered()) {
        return Iterators.concat(added.iterator(), base.iterator());
      } else {
        return Iterators.concat(
            added.iterator(),
            Iterators.filter(base.iterator(), e -> !shadowed.contains(e.key()) && !removed.contains(e.key())));
      }
    }

    @Override
    public synchronized IList<IEntry<K, V>> entries() {
      if (!altered()) {
        return Lists.concat(added.entries(), base.entries());
      } else {
        canonicalize();
        return canonical.entries();
      }
    }

    @Override
    public synchronized long size() {
      return canonical != null
          ? canonical.size()
          : base.size() + (added.size() - shadowed.size()) - removed.size();
    }

    @Override
    public int hashCode() {
      return (int) Maps.hash(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IMap) {
        return Maps.equals(this, (IMap<K, V>) obj);
      }
      return false;
    }

    @Override
    public String toString() {
      return Maps.toString(this);
    }
  }

  public static <K, V> String toString(IMap<K, V> m) {
    return toString(m, Objects::toString, Objects::toString);
  }

  public static <K, V> String toString(IMap<K, V> m, Function<K, String> keyPrinter, Function<V, String> valPrinter) {
    StringBuilder sb = new StringBuilder("{");

    Iterator<IEntry<K, V>> it = m.entries().iterator();
    while (it.hasNext()) {
      IEntry<K, V> entry = it.next();
      sb.append(keyPrinter.apply(entry.key()));
      sb.append(" ");
      sb.append(valPrinter.apply(entry.value()));

      if (it.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append("}");

    return sb.toString();
  }

  public static <K, V> long hash(IMap<K, V> m) {
    return hash(m, e -> (Objects.hash(e.key()) * 31) ^ Objects.hash(e.value()), (a, b) -> a + b);
  }

  public static <K, V> long hash(IMap<K, V> m, ToLongFunction<IEntry<K, V>> hash, LongBinaryOperator combiner) {
    return m.entries().stream().mapToLong(hash).reduce(combiner).orElse(0);
  }

  public static <K, V> boolean equals(IMap<K, V> a, IMap<K, V> b) {
    return equals(a, b, Objects::equals);
  }

  public static <K, V> boolean equals(IMap<K, V> a, IMap<K, V> b, BiPredicate<V, V> valEquals) {
    if (a.size() != b.size()) {
      return false;
    } else if (a == b) {
      return true;
    }

    return a.entries().stream().allMatch(e -> {
      IMap m = b;
      Object val = m.get(e.key(), DEFAULT_VALUE);
      return val != DEFAULT_VALUE && valEquals.test((V) val, e.value());
    });
  }

  public static <K, V> IMap<K, V> from(java.util.Map<K, V> map) {
    return from(
        Sets.from(map.keySet()),
        k -> map.get(k),
        () -> Iterators.map(map.entrySet().iterator(), e -> new Maps.Entry<>(e.getKey(), e.getValue())));
  }

  public static <K, V> IMap<K, V> from(ISet<K> keys, Function<K, V> lookup) {
    return from(keys, lookup, () -> Iterators.map(keys.iterator(), k -> new Maps.Entry<>(k, lookup.apply(k))));
  }

  public static <K, V> IMap<K, V> from(ISet<K> keys, Function<K, V> lookup, Supplier<Iterator<IEntry<K, V>>> iterator) {
    return new IMap<K, V>() {
      @Override
      public V get(K key, V defaultValue) {
        if (keys.contains(key)) {
          return lookup.apply(key);
        } else {
          return defaultValue;
        }
      }

      @Override
      public Optional<V> get(K key) {
        if (keys.contains(key)) {
          return Optional.ofNullable(lookup.apply(key));
        } else {
          return Optional.empty();
        }
      }

      @Override
      public boolean contains(K key) {
        return keys.contains(key);
      }

      @Override
      public Iterator<IEntry<K, V>> iterator() {
        return iterator.get();
      }

      @Override
      public IList<IEntry<K, V>> entries() {
        return keys.elements().stream()
            .map(k -> (IEntry<K, V>) new Entry(k, lookup.apply(k)))
            .collect(Lists.collector());
      }

      @Override
      public ISet<K> keys() {
        return keys;
      }

      @Override
      public long size() {
        return keys.size();
      }

      @Override
      public int hashCode() {
        return (int) Maps.hash(this);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof IMap) {
          return Maps.equals(this, (IMap<K, V>) obj);
        }
        return false;
      }

      @Override
      public String toString() {
        return Maps.toString(this);
      }
    };
  }

  public static <K, V> java.util.Map<K, V> toMap(IMap<K, V> map) {
    return new java.util.Map<K, V>() {

      @Override
      public int size() {
        return (int) map.size();
      }

      @Override
      public boolean isEmpty() {
        return map.size() == 0;
      }

      @Override
      public boolean containsKey(Object key) {
        return map.get((K) key).isPresent();
      }

      @Override
      public boolean containsValue(Object value) {
        return map.entries().stream().anyMatch(e -> Objects.equals(value, e.value()));
      }

      @Override
      public V get(Object key) {
        return map.get((K) key).orElse(null);
      }

      @Override
      public V put(K key, V value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public V remove(Object key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void putAll(java.util.Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }

      @Override
      public java.util.Set<K> keySet() {
        return Sets.toSet(
            lazyMap(map.entries(), IEntry::key),
            k -> map.get(k).isPresent());
      }

      @Override
      public Collection<V> values() {
        return Lists.toList(lazyMap(map.entries(), IEntry::value));
      }

      @Override
      public java.util.Set<Entry<K, V>> entrySet() {
        return Sets.toSet(
            lazyMap(map.entries(), Maps::toEntry),
            e -> map.get(e.getKey()).map(v -> Objects.equals(v, e.getValue())).orElse(false));
      }

      @Override
      public String toString() {
        return Maps.toString(map);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof java.util.Map) {
          return Maps.equals(map, from((java.util.Map<K, V>) obj));
        }
        return false;
      }

      @Override
      public int hashCode() {
        return (int) Maps.hash(map, e -> Objects.hashCode(e.key()) ^ Objects.hashCode(e.value()), (a, b) -> a + b);
      }
    };
  }

  public static <K, V> java.util.Map.Entry<K, V> toEntry(IEntry<K, V> entry) {
    return new java.util.Map.Entry<K, V>() {
      @Override
      public K getKey() {
        return entry.key();
      }

      @Override
      public V getValue() {
        return entry.value();
      }

      @Override
      public V setValue(V value) {
        throw new UnsupportedOperationException();
      }
    };
  }

  static <K, V> IMap<K, V> difference(IMap<K, V> map, ISet<K> keys) {
    for (K key : keys) {
      map = map.remove(key);
    }
    return map;
  }

  static <K, V> IMap<K, V> intersection(IMap<K, V> accumulator, IMap<K, V> map, ISet<K> keys) {
    if (map.size() < keys.size()) {
      for (IEntry<K, V> entry : map.entries()) {
        if (keys.contains(entry.key())) {
          accumulator.put(entry.key(), entry.value());
        }
      }
    } else {
      for (K key : keys) {
        Object value = ((IMap) map).get(key, DEFAULT_VALUE);
        if (value != DEFAULT_VALUE) {
          accumulator = accumulator.put(key, (V) value);
        }
      }
    }
    return accumulator;
  }

  static <K, V> IMap<K, V> merge(IMap<K, V> a, IMap<K, V> b, BinaryOperator<V> mergeFn) {
    for (IEntry<K, V> e : b.entries()) {
      a = a.put(e.key(), e.value(), mergeFn);
    }
    return a;
  }

  public static <T, K, V> Collector<T, LinearMap<K, V>, LinearMap<K, V>> linearCollector(Function<T, K> keyFn, Function<T, V> valFn, int capacity) {
    return linearCollector(keyFn, valFn, Maps.MERGE_LAST_WRITE_WINS, capacity);
  }

  public static <T, K, V> Collector<T, LinearMap<K, V>, LinearMap<K, V>> linearCollector(Function<T, K> keyFn, Function<T, V> valFn) {
    return linearCollector(keyFn, valFn, Maps.MERGE_LAST_WRITE_WINS, 8);
  }

  public static <T, K, V> Collector<T, LinearMap<K, V>, LinearMap<K, V>> linearCollector(
      Function<T, K> keyFn,
      Function<T, V> valFn,
      BinaryOperator<V> mergeFn,
      int capacity) {
    return new Collector<T, LinearMap<K, V>, LinearMap<K, V>>() {
      @Override
      public Supplier<LinearMap<K, V>> supplier() {
        return () -> new LinearMap<K, V>(capacity);
      }

      @Override
      public BiConsumer<LinearMap<K, V>, T> accumulator() {
        return (m, e) -> m.put(keyFn.apply(e), valFn.apply(e));
      }

      @Override
      public BinaryOperator<LinearMap<K, V>> combiner() {
        return (a, b) -> a.merge(b, mergeFn);
      }

      @Override
      public Function<LinearMap<K, V>, LinearMap<K, V>> finisher() {
        return x -> x;
      }

      @Override
      public java.util.Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.IDENTITY_FINISH);
      }
    };
  }

  public static <T, K, V> Collector<T, Map<K, V>, Map<K, V>> collector(Function<T, K> keyFn, Function<T, V> valFn) {
    return collector(keyFn, valFn, Maps.MERGE_LAST_WRITE_WINS);
  }

  public static <T, K, V> Collector<T, Map<K, V>, Map<K, V>> collector(Function<T, K> keyFn, Function<T, V> valFn, BinaryOperator<V> mergeFn) {
    return new Collector<T, Map<K, V>, Map<K, V>>() {
      @Override
      public Supplier<Map<K, V>> supplier() {
        return () -> new Map<K, V>().linear();
      }

      @Override
      public BiConsumer<Map<K, V>, T> accumulator() {
        return (m, e) -> m.put(keyFn.apply(e), valFn.apply(e));
      }

      @Override
      public BinaryOperator<Map<K, V>> combiner() {
        return (a, b) -> a.merge(b, mergeFn);
      }

      @Override
      public Function<Map<K, V>, Map<K, V>> finisher() {
        return Map::forked;
      }

      @Override
      public java.util.Set<Characteristics> characteristics() {
        return EnumSet.noneOf(Characteristics.class);
      }
    };
  }
}
