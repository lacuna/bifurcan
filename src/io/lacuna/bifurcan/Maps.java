package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IMap.IEntry;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class Maps {

  static class Entry<K, V> implements IEntry<K, V> {
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
  }

  public static <K, V> String toString(IMap<K, V> m) {
    return toString(m, Objects::toString, Objects::toString);
  }

  public static <K, V> String toString(IMap<K, V> m, Function<K, String> keyPrinter, Function<V, String> valPrinter) {
    StringBuilder sb = new StringBuilder("{");

    Iterator<IEntry<K, V>> it = m.entries().iterator();
    while (it.hasNext()){
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
    }

    return a.entries().stream().allMatch(e -> {
      Optional<V> val = b.get(e.key());
      return val.isPresent() && valEquals.test(val.get(), e.value());
    });
  }

  public static <K, V> IMap<K, V> from(java.util.Map map) {
    Object defaultValue = new Object();
    return new IMap<K, V>() {
      @Override
      public IMap<K, V> put(K key, V value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public IMap<K, V> remove(K key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Optional<V> get(K key) {
        Object value = map.getOrDefault(key, defaultValue);
        if (value == defaultValue) {
          return Optional.empty();
        } else {
          return Optional.of((V) value);
        }
      }

      @Override
      public IList<IEntry<K, V>> entries() {
        Set<Map.Entry<K, V>> entries = map.entrySet();
        return entries.stream()
                .map(e -> (IEntry<K, V>) new Entry(e.getKey(), e.getValue()))
                .collect(Lists.collector());
      }

      @Override
      public long size() {
        return map.size();
      }

      @Override
      public IMap<K, V> forked() {
        return null;
      }

      @Override
      public IMap<K, V> linear() {
        return new LinearMap<>(map);
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
      public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<K> keySet() {
        return map.entries().stream().map(e -> e.key()).collect(Collectors.toSet());
      }

      @Override
      public Collection<V> values() {
        return map.entries().stream().map(e -> e.value()).collect(Collectors.toList());
      }

      @Override
      public Set<Entry<K, V>> entrySet() {
        return map.entries().stream()
                .map(e ->
                        new Map.Entry<K, V>() {
                          @Override
                          public K getKey() {
                            return e.key();
                          }

                          @Override
                          public V getValue() {
                            return e.value();
                          }

                          @Override
                          public V setValue(V value) {
                            throw new UnsupportedOperationException();
                          }
                        }
                ).collect(Collectors.toSet());
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
}
