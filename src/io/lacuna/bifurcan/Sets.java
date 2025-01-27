package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.IntStream;

/**
 * Utility functions for classes implementing {@code ISet}.
 *
 * @author ztellman
 */
public class Sets {

  public static <V> long hash(ISet<V> s) {
    return hash(s, x -> s.valueHash().applyAsLong(x), Long::sum);
  }

  public static <V> long hash(ISet<V> set, ToLongFunction<V> hash, LongBinaryOperator combiner) {
    return set.elements().stream().mapToLong(hash).reduce(combiner).orElse(0);
  }

  public static <V> boolean equals(ISet<V> a, ISet<V> b) {
    if (a.size() != b.size()) {
      return false;
    } else if (a == b) {
      return true;
    }
    return a.elements().stream().allMatch(b::contains);
  }

  static <V> ISet<V> difference(ISet<V> a, ISet<V> b) {
    for (V e : b) {
      a = a.remove(e);
    }
    return a;
  }

  static <V> ISet<V> union(ISet<V> a, ISet<V> b) {
    for (V e : b) {
      a = a.add(e);
    }
    return a;
  }

  static <V> ISet<V> intersection(ISet<V> accumulator, ISet<V> a, ISet<V> b) {
    if (b.size() < a.size()) {
      return intersection(accumulator, b, a);
    }
    for (V e : a) {
      if (b.contains(e)) {
        accumulator = accumulator.add(e);
      }
    }
    return accumulator;
  }

  public static <V> java.util.Set<V> toSet(IList<V> elements, Predicate<V> contains) {
    return new java.util.Set<V>() {
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
        T[] ary = a.length < size() ? (T[]) Array.newInstance(a.getClass().getComponentType(), size()) : a;
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
        return c.stream().allMatch(this::contains);
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

  public static <V> ISortedSet<V> from(
      IList<V> elements,
      Comparator<V> comparator,
      Function<V, OptionalLong> inclusiveFloorIndex
  ) {
    return new ISortedSet.Mixin<V>() {
      @Override
      public Comparator<V> comparator() {
        return comparator;
      }

      @Override
      public OptionalLong inclusiveFloorIndex(V val) {
        return inclusiveFloorIndex.apply(val);
      }

      @Override
      public long size() {
        return elements.size();
      }

      @Override
      public V nth(long idx) {
        return elements.nth(idx);
      }

      @Override
      public IList<V> elements() {
        return elements;
      }

      @Override
      public Iterator<V> iterator(long startIndex) {
        return elements.iterator(startIndex);
      }

      @Override
      public ISortedSet<V> add(V value) {
        return new SortedSet<>(comparator).union(this).add(value);
      }

      @Override
      public ISortedSet<V> remove(V value) {
        return new SortedSet<>(comparator).union(this).remove(value);
      }

      @Override
      public ISortedSet<V> linear() {
        return new SortedSet<>(comparator).union(this).linear();
      }
    };
  }

  public static <V> ISet<V> from(IList<V> elements, Function<V, OptionalLong> indexOf) {
    return from(elements, indexOf, elements::iterator);
  }

  public static <V> ISet<V> from(IList<V> elements, Function<V, OptionalLong> indexOf, Supplier<Iterator<V>> iterator) {
    return new ISet.Mixin<V>() {
      @Override
      public boolean contains(V value) {
        return indexOf(value).isPresent();
      }

      @Override
      public long size() {
        return elements.size();
      }

      @Override
      public IList<V> elements() {
        return elements;
      }

      @Override
      public OptionalLong indexOf(V element) {
        return indexOf.apply(element);
      }

      @Override
      public V nth(long idx) {
        return elements.nth(idx);
      }

      @Override
      public Iterator<V> iterator() {
        return iterator.get();
      }

      @Override
      public ToLongFunction<V> valueHash() {
        return Maps.DEFAULT_HASH_CODE;
      }

      @Override
      public BiPredicate<V, V> valueEquality() {
        return Maps.DEFAULT_EQUALS;
      }

      @Override
      public ISet<V> add(V value) {
        return Set.from(this).add(value);
      }

      @Override
      public ISet<V> remove(V value) {
        return Set.from(this).remove(value);
      }

      @Override
      public ISet<V> linear() {
        return Set.from(this).linear();
      }
    };
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

  public static <V> Collector<V, LinearSet<V>, LinearSet<V>> linearCollector() {
    return linearCollector(8);
  }

  public static <V> Collector<V, LinearSet<V>, LinearSet<V>> linearCollector(int capacity) {
    return new Collector<V, LinearSet<V>, LinearSet<V>>() {
      @Override
      public Supplier<LinearSet<V>> supplier() {
        return () -> new LinearSet<V>(capacity);
      }

      @Override
      public BiConsumer<LinearSet<V>, V> accumulator() {
        return LinearSet::add;
      }

      @Override
      public BinaryOperator<LinearSet<V>> combiner() {
        return LinearSet::union;
      }

      @Override
      public Function<LinearSet<V>, LinearSet<V>> finisher() {
        return s -> s;
      }

      @Override
      public java.util.Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.IDENTITY_FINISH, Characteristics.UNORDERED);
      }
    };
  }

  public static <V> Collector<V, Set<V>, Set<V>> collector() {
    return new Collector<V, Set<V>, Set<V>>() {
      @Override
      public Supplier<Set<V>> supplier() {
        return () -> new Set<V>().linear();
      }

      @Override
      public BiConsumer<Set<V>, V> accumulator() {
        return Set::add;
      }

      @Override
      public BinaryOperator<Set<V>> combiner() {
        return Set::union;
      }

      @Override
      public Function<Set<V>, Set<V>> finisher() {
        return Set::forked;
      }

      @Override
      public java.util.Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.UNORDERED);
      }
    };
  }
}
