package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

import static io.lacuna.bifurcan.Lists.lazyMap;

/**
 * Utility functions for classes implementing {@code IMap}.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class Maps {

  private static final Object DEFAULT_VALUE = new Object();

  public static final BinaryOperator MERGE_LAST_WRITE_WINS = (a, b) -> b;

  public static final ToIntFunction DEFAULT_HASH_CODE = Objects::hashCode;

  public static final BiPredicate DEFAULT_EQUALS = Objects::equals;

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
    public long indexOf(Object key) {
      return -1;
    }

    @Override
    public IEntry nth(long index) {
      throw new IndexOutOfBoundsException();
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
    public ToIntFunction keyHash() {
      return Maps.DEFAULT_HASH_CODE;
    }

    @Override
    public BiPredicate keyEquality() {
      return Maps.DEFAULT_EQUALS;
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
    public IMap clone() {
      return this;
    }

    @Override
    public String toString() {
      return Maps.toString(this);
    }
  };

  public static class Entry<K, V> implements IEntry<K, V> {
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

  public static class HashEntry<K, V> implements IEntry.WithHash<K, V> {
    private final int keyHash;
    private final K key;
    private final V value;

    public HashEntry(int keyHash, K key, V value) {
      this.keyHash = keyHash;
      this.key = key;
      this.value = value;
    }

    public int keyHash() {
      return keyHash;
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
      if (obj instanceof IEntry.WithHash) {
        IEntry.WithHash<K, V> e = (IEntry.WithHash<K, V>) obj;
        return keyHash == e.keyHash() && Objects.equals(key, e.key()) && Objects.equals(value, e.value());
      }
      return false;
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
    ToIntFunction hashFn = m.keyHash();
    return hash(m, e -> (hashFn.applyAsInt(e.key()) * 31) ^ Objects.hashCode(e.value()), (a, b) -> a + b);
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

  public static <K, V> IMap<K, V> from(ISet<K> keys, Function<K, V> lookup) {
    return from(keys, lookup, () -> Iterators.map(keys.iterator(), k -> IEntry.of(k, lookup.apply(k))));
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
      public long indexOf(K key) {
        return keys.indexOf(key);
      }

      @Override
      public IEntry<K, V> nth(long index) {
        K key = keys.nth(index);
        return new Entry<>(key, lookup.apply(key));
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
      public ToIntFunction<K> keyHash() {
        return keys.valueHash();
      }

      @Override
      public BiPredicate<K, K> keyEquality() {
        return keys.valueEquality();
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
      public IMap<K, V> clone() {
        return this;
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
          java.util.Map<K, V> m = (java.util.Map<K, V>) obj;
          if (size() != m.size()) {
            return false;
          } else if (this == m) {
            return true;
          }

          return m.entrySet().stream().allMatch(e -> {
            Object val = ((Map) map).get(e.getKey(), DEFAULT_VALUE);
            return val != DEFAULT_VALUE && Objects.equals((V) val, e.getValue());
          });
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

  public static <K, V> boolean equivEquality(IMap<K, ?> a, IMap<K, ?> b) {
    return a.keyHash() == b.keyHash() && a.keyEquality() == b.keyEquality();
  }

  public static <K, V> boolean equivEquality(IMap<K, ?> a, ISet<K> b) {
    return a.keyHash() == b.valueHash() && a.keyEquality() == b.valueEquality();
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
