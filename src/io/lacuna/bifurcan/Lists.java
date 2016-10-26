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

  private static abstract class LazyList<V> implements IList<V> {
    @Override
    public int hashCode() {
      return (int) Lists.hash(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IList) {
        return Lists.equals(this, (IList<V>) obj);
      }
      return false;
    }

    @Override
    public String toString() {
      return Lists.toString(this);
    }

    @Override
    public IList<V> forked() {
      // todo build tree list
      return null;
    }

    @Override
    public IList<V> linear() {
      return new LinearList<V>(this);
    }
  }

  public static <V> boolean equals(IList<V> a, IList<V> b) {
    return equals(a, b, Objects::equals);
  }

  public static <V> boolean equals(IList<V> a, IList<V> b, BiPredicate<V, V> equals) {
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

  public static <V> long hash(IList<V> l) {
    return hash(l, Objects::hashCode, (a, b) -> (a * 31) + b);
  }

  public static <V> long hash(IList<V> l, ToLongFunction<V> hash, LongBinaryOperator combiner) {
    return l.stream().mapToLong(hash).reduce(combiner).orElse(0);
  }

  public static <V> String toString(IList<V> l) {
    return toString(l, Objects::toString);
  }

  public static <V> String toString(IList<V> l, Function<V, String> printer) {
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

  public static <V> java.util.List<V> toList(IList<V> list) {
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

  public static <V> IList<V> subList(IList<V> list, long start, long end) {
    return new LazyList<V>() {
      @Override
      public IList<V> append(V value) {
        return forked().append(value);
      }

      @Override
      public IList<V> set(long idx, V value) {
        return forked().set(idx, value);
      }

      @Override
      public V nth(long idx) {
        if (idx < size()) {
          return list.nth(start + idx);
        } else {
          throw new IndexOutOfBoundsException();
        }
      }

      @Override
      public long size() {
        return end - start;
      }
    };
  }

  public static <V> IList<V> from(V[] array) {
    return new LazyList<V>() {
      @Override
      public IList<V> append(V value) {
        return forked().append(value);
      }

      @Override
      public IList<V> set(long idx, V value) {
        return forked().set(idx, value);
      }

      @Override
      public V nth(long idx) {
        if (idx > Integer.MAX_VALUE) {
          throw new IndexOutOfBoundsException();
        }
        return array[(int) idx];
      }

      @Override
      public long size() {
        return array.length;
      }
    };
  }

  public static <V> IList<V> from(java.util.List<V> list) {
    return new LazyList<V>() {
      @Override
      public IList<V> append(V value) {
        return forked().append(value);
      }

      @Override
      public IList<V> set(long idx, V value) {
        return forked().set(idx, value);
      }

      @Override
      public V nth(long idx) {
        if (idx > Integer.MAX_VALUE) {
          throw new IndexOutOfBoundsException();
        }
        return list.get((int) idx);
      }

      @Override
      public long size() {
        return list.size();
      }
    };
  }

  public static <V> IList<V> from(long size, LongFunction<V> elementFn) {
    return new LazyList<V>() {
      @Override
      public IList<V> append(V value) {
        return forked().append(value);
      }

      @Override
      public IList<V> set(long idx, V value) {
        return forked().set(idx, value);
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

  public static <V> IList<V> readOnly(IList<V> list) {
    return new LazyList<V>() {
      @Override
      public IList<V> append(V value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public IList<V> set(long idx, V value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public V nth(long idx) {
        return list.nth(idx);
      }

      @Override
      public long size() {
        return list.size();
      }

      @Override
      public IList<V> forked() {
        return this;
      }

      @Override
      public IList<V> linear() {
        return this;
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
        return IPartitionable::merge;
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
