package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author ztellman
 */
public class Lists {

  public static <V, U> IReadList<U> lazyMap(IReadList<V> l, Function<V, U> f) {
    return Lists.from(l.size(), i -> f.apply(l.nth(i)));
  }

  public static <V> boolean equals(IReadList<V> a, IReadList<V> b) {
    return equals(a, b, Objects::equals);
  }

  public static <V> boolean equals(IReadList<V> a, IReadList<V> b, BiPredicate<V, V> equals) {
    if (a.size() != b.size()) {
      return false;
    }
    Iterator<V> ia = a.iterator();
    Iterator<V> ib = b.iterator();
    while (ia.hasNext()) {
      if (!equals.test(ia.next(), ib.next())) {
        return false;
      }
    }
    return true;
  }

  public static <V> long hash(IReadList<V> l) {
    return hash(l, Objects::hashCode, (a, b) -> (a * 31) + b);
  }

  public static <V> long hash(IReadList<V> l, ToLongFunction<V> hash, LongBinaryOperator combiner) {
    return l.stream().mapToLong(hash).reduce(combiner).orElse(0);
  }

  public static <V> String toString(IReadList<V> l) {
    return toString(l, Objects::toString);
  }

  public static <V> String toString(IReadList<V> l, Function<V, String> printer) {
    StringBuilder sb = new StringBuilder("[");

    Iterator<V> it = l.iterator();
    while (it.hasNext()) {
      sb.append(printer.apply(it.next()));
      if (it.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append("]");

    return sb.toString();
  }

  public static <V> java.util.List<V> toList(IReadList<V> list) {
    return new java.util.List<V>() {

      @Override
      public int size() {
        return (int) list.size();
      }

      @Override
      public boolean isEmpty() {
        return list.size() == 0;
      }

      @Override
      public boolean contains(Object o) {
        return list.stream().anyMatch(e -> Objects.equals(o, e));
      }

      @Override
      public Iterator<V> iterator() {
        return list.iterator();
      }

      @Override
      public Object[] toArray() {
        return list.toArray();
      }

      @Override
      @SuppressWarnings("unchecked")
      public <T> T[] toArray(T[] a) {
        T[] ary = (T[]) Array.newInstance(a.getClass().getComponentType(), size());
        IntStream.range(0, size()).forEach(i -> ary[i] = (T) get(i));
        return ary;
      }

      @Override
      public boolean add(V v) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
        throw new UnsupportedOperationException();
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
      public boolean addAll(int index, Collection<? extends V> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }

      @Override
      public V get(int index) {
        return list.nth(index);
      }

      @Override
      public V set(int index, V element) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void add(int index, V element) {
        throw new UnsupportedOperationException();
      }

      @Override
      public V remove(int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int indexOf(Object o) {
        return IntStream.range(0, size())
                .filter(idx -> Objects.equals(get(idx), o))
                .findFirst()
                .orElse(-1);
      }

      @Override
      public int lastIndexOf(Object o) {
        return size() -
                IntStream.range(0, size())
                        .filter(idx -> Objects.equals(get(size() - (idx + 1)), o))
                        .findFirst()
                        .orElse(size() + 1);
      }

      @Override
      public ListIterator<V> listIterator() {
        return listIterator(0);
      }

      @Override
      public ListIterator<V> listIterator(int index) {
        return new ListIterator<V>() {

          int idx = index;

          @Override
          public boolean hasNext() {
            return idx < size();
          }

          @Override
          public V next() {
            return get(idx++);
          }

          @Override
          public boolean hasPrevious() {
            return idx > index;
          }

          @Override
          public V previous() {
            return get(--idx);
          }

          @Override
          public int nextIndex() {
            return idx;
          }

          @Override
          public int previousIndex() {
            return idx - 1;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void set(V v) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void add(V v) {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public java.util.List<V> subList(int fromIndex, int toIndex) {
        return IntStream.range(0, toIndex - fromIndex)
                .mapToObj(idx -> get(idx))
                .collect(Collectors.toList());
      }

      @Override
      public int hashCode() {
        return (int) Lists.hash(list, Objects::hashCode, (a, b) -> (a * 31) + b);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof java.util.List) {
          return Lists.equals(list, Lists.from(this));
        }
        return false;
      }

      @Override
      public String toString() {
        return Lists.toString(list);
      }
    };
  }

  public static <V> IReadList<V> subList(IReadList<V> list, long start, long end) {
    long size = end - start;
    return Lists.from(size, idx -> {
      if (0 <= idx && idx < size) {
        return list.nth(start + idx);
      } else {
        throw new IndexOutOfBoundsException();
      }
    });
  }

  public static <V> IReadList<V> from(V[] array) {
    return Lists.from(array.length, idx -> {
      if (idx > Integer.MAX_VALUE) {
        throw new IndexOutOfBoundsException();
      }
      return array[(int) idx];
    });
  }

  public static <V> IReadList<V> from(java.util.List<V> list) {
    return Lists.from(list.size(), idx -> {
      if (idx > Integer.MAX_VALUE) {
        throw new IndexOutOfBoundsException();
      }
      return list.get((int) idx);
    });
  }

  public static <V> IReadList<V> from(long size, LongFunction<V> elementFn) {
    return new IReadList<V>() {
      @Override
      public int hashCode() {
        return (int) Lists.hash(this);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof IReadList) {
          return Lists.equals(this, (IReadList<V>) obj);
        }
        return false;
      }

      @Override
      public String toString() {
        return Lists.toString(this);
      }
      @Override
      public V nth(long idx) {
        return elementFn.apply(idx);
      }

      @Override
      public long size() {
        return size;
      }
    };
  }

  public static <V> Collector<V, IList<V>, IList<V>> linearCollector() {
    return new Collector<V, IList<V>, IList<V>>() {

      @Override
      public Supplier<IList<V>> supplier() {
        return LinearList::new;
      }

      @Override
      public BiConsumer<IList<V>, V> accumulator() {
        return IList::append;
      }

      @Override
      public BinaryOperator<IList<V>> combiner() {
        // todo implement concat
        throw new UnsupportedOperationException();
      }

      @Override
      public Function<IList<V>, IList<V>> finisher() {
        return a -> a;
      }

      @Override
      public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.IDENTITY_FINISH);
      }
    };
  }
}
