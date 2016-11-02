package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IReadMap.IEntry;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.*;

import static io.lacuna.bifurcan.Lists.lazyMap;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class Maps {

  public static IReadMap.EntryMerger MERGE_LAST_WRITE_WINS = (a, b) -> b;

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

  public static <K, V> String toString(IReadMap<K, V> m) {
    return toString(m, Objects::toString, Objects::toString);
  }

  public static <K, V> String toString(IReadMap<K, V> m, Function<K, String> keyPrinter, Function<V, String> valPrinter) {
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

  public static <K, V> long hash(IReadMap<K, V> m, ToLongFunction<IEntry<K, V>> hash, LongBinaryOperator combiner) {
    return m.entries().stream().mapToLong(hash).reduce(combiner).orElse(0);
  }

  public static <K, V> IReadMap<K, V> merge(IReadMap<K, V> a, IReadMap<K, V> b, IReadMap.EntryMerger<K, V> mergeFn) {
    if (a.size() <= b.size()) {
      return merge(b, a, mergeFn);
    }

    LinearMap<K, V> m = new LinearMap<>(a);
    for (IEntry<K, V> e : b.entries()) {
      m = (LinearMap<K, V>) m.put(e.key(), e.value(), mergeFn);
    }
    return m;
  }

  public static <K, V> boolean equals(IReadMap<K, V> a, IReadMap<K, V> b) {
    return equals(a, b, Objects::equals);
  }

  public static <K, V> boolean equals(IReadMap<K, V> a, IReadMap<K, V> b, BiPredicate<V, V> valEquals) {
    if (a.size() != b.size()) {
      return false;
    }

    return a.entries().stream().allMatch(e -> {
      Optional<V> val = b.get(e.key());
      return val.isPresent() && valEquals.test(val.get(), e.value());
    });
  }

  public static <K, V> IReadMap<K, V> from(java.util.Map map) {
    return new IReadMap<K, V>() {

      @Override
      public Optional<V> get(K key) {
        return Optional.ofNullable((V) map.get(key));
      }

      @Override
      public boolean contains(K key) {
        return map.containsKey(key);
      }

      @Override
      public IReadList<IEntry<K, V>> entries() {
        Set<Map.Entry<K, V>> entries = map.entrySet();
        return entries.stream()
                .map(e -> (IEntry<K, V>) new Entry(e.getKey(), e.getValue()))
                .collect(Lists.linearCollector());
      }

      @Override
      public IReadSet<K> keys() {
        return new LinearSet<>(lazyMap(entries(), IEntry::key));
      }

      @Override
      public long size() {
        return map.size();
      }
    };
  }

  public static <K, V> java.util.Map<K, V> toMap(IReadMap<K, V> map) {
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
      public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<K> keySet() {
        return Sets.toSet(
                lazyMap(map.entries(), IEntry::key),
                k -> map.get(k).isPresent());
      }

      @Override
      public Collection<V> values() {
        return Lists.toList(lazyMap(map.entries(), IEntry::value));
      }

      @Override
      public Set<Entry<K, V>> entrySet() {
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

  public static <K, V> Map.Entry<K, V> toEntry(IEntry<K, V> entry) {
    return new Map.Entry<K, V>() {
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
}
