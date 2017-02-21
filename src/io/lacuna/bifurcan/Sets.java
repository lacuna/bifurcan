package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.IntStream;

/**
 * @author ztellman
 */
public class Sets {

  public static <V> long hash(ISet<V> s) {
    return hash(s, Objects::hashCode, (a, b) -> a + b);
  }

  public static <V> long hash(ISet<V> set, ToLongFunction<V> hash, LongBinaryOperator combiner) {
    return set.elements().stream().mapToLong(hash).reduce(combiner).orElse(0);
  }

  public static <V> boolean equals(ISet<V> a, ISet<V> b) {
    if (a.size() != b.size()) {
      return false;
    }
    return a.elements().stream().allMatch(b::contains);
  }

  public static <V> ISet<V> difference(ISet<V> a, ISet<V> b) {
    LinearSet<V> s = LinearSet.from(a);
    for (V e : b) {
      s = s.remove(e);
    }
    return s;
  }

  public static <V> ISet<V> union(ISet<V> a, ISet<V> b) {
    if (b.size() > a.size()) {
      return union(b, a);
    }
    LinearSet<V> s = LinearSet.from(a);
    for (V e : b) {
      s = s.add(e);
    }
    return s;
  }

  public static <V> ISet<V> intersection(ISet<V> a, ISet<V> b) {
    if (b.size() < a.size()) {
      return intersection(b, a);
    }
    LinearSet<V> s = new LinearSet<>((int) a.size());
    for (V e : a) {
      if (b.contains(e)) {
        s = s.add(e);
      }
    }
    return s;
  }

  public static <V> java.util.Set<V> toSet(IList<V> elements, Predicate<V> contains) {
    return new Set<V>() {
      @Override
      public int size() {
        return (int) elements.size();
      }

      @Override
      public boolean isEmpty() {
        return elements.size() == 0;
      }

      @Override
      public boolean contains(Object o) {
        return contains.test((V) o);
      }

      @Override
      public Iterator<V> iterator() {
        return elements.iterator();
      }

      @Override
      public Object[] toArray() {
        return elements.toArray();
      }

      @Override
      public <T> T[] toArray(T[] a) {
        T[] ary = (T[]) Array.newInstance(a.getClass().getComponentType(), size());
        IntStream.range(0, size()).forEach(i -> ary[i] = (T) elements.nth(i));
        return ary;
      }

      @Override
      public boolean add(V v) {
        return false;
      }

      @Override
      public boolean remove(Object o) {
        return false;
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        return c.stream().allMatch(e -> contains(e));
      }

      @Override
      public boolean addAll(Collection<? extends V> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public static <V> ISet<V> from(IList<V> elements, Predicate<V> contains) {
    return new ISet<V>() {
      @Override
      public boolean contains(V value) {
        return contains.test(value);
      }

      @Override
      public long size() {
        return elements.size();
      }

      @Override
      public IList<V> elements() {
        return elements;
      }
    };
  }

  public static <V> ISet<V> from(java.util.Set<V> s) {
    return new ISet<V>() {
      @Override
      public boolean contains(V value) {
        return s.contains(value);
      }

      @Override
      public long size() {
        return s.size();
      }

      @Override
      public IList<V> elements() {
        return (IList<V>) Lists.from(s.toArray());
      }
    };
  }

  public static <V> IList<ISet<V>> split(ISet<V> set, int parts) {
    return set.elements().split(parts).stream().map(LinearSet::from).collect(Lists.collector());
  }

  public static <V> String toString(ISet<V> set) {
    return toString(set, Objects::toString);
  }

  public static <V> String toString(ISet<V> set, Function<V, String> elementPrinter) {
    StringBuilder sb = new StringBuilder("{");

    Iterator<V> it = set.elements().iterator();
    while (it.hasNext()) {
      sb.append(elementPrinter.apply(it.next()));
      if (it.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append("}");

    return sb.toString();
  }

  public static <V> Collector<V, ISet<V>, ISet<V>> collector() {
    return new Collector<V, ISet<V>, ISet<V>>() {
      @Override
      public Supplier<ISet<V>> supplier() {
        return LinearSet::new;
      }

      @Override
      public BiConsumer<ISet<V>, V> accumulator() {
        return (s, e) -> ((LinearSet<V>) s).add(e);
      }

      @Override
      public BinaryOperator<ISet<V>> combiner() {
        return ISet::union;
      }

      @Override
      public Function<ISet<V>, ISet<V>> finisher() {
        return a -> a;
      }

      @Override
      public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.IDENTITY_FINISH);
      }
    };
  }
}
