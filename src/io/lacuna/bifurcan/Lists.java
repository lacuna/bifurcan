package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.ConcatList;
import io.lacuna.bifurcan.diffs.DiffList;
import io.lacuna.bifurcan.utils.Iterators;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Set;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.lang.Math.min;

/**
 * Utility functions for classes implementing {@link IList}.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class Lists {

  private static class JavaList<V> implements java.util.List<V>, RandomAccess {

    private final IList<V> list;

    public JavaList(IList<V> list) {
      this.list = list;
    }

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
      T[] ary = a.length < size() ? (T[]) Array.newInstance(a.getClass().getComponentType(), size()) : a;
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
          return idx > 0;
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
      return Lists.toList(list.slice(fromIndex, toIndex));
    }

    @Override
    public int hashCode() {
      return (int) Lists.hash(list, Objects::hashCode, (a, b) -> (a * 31) + b);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof java.util.List) {
        return Lists.equals(list, Lists.from((java.util.List) obj));
      }
      return false;
    }

    @Override
    public String toString() {
      return Lists.toString(list);
    }
  }

  /**
   * Returns a list which will lazily, and repeatedly, transform each element of the input list on lookup.
   *
   * @param l   a list
   * @param f   a transform function for the elements of the list
   * @param <V> the element type for the input list
   * @param <U> the element type for the result list
   * @return the result list
   */
  public static <V, U> IList<U> lazyMap(IList<V> l, Function<V, U> f) {
    return Lists.from(
        l.size(),
        i -> f.apply(l.nth(i)),
        idx -> Iterators.map(l.iterator(idx), f)
    );
  }

  /**
   * @return true if the two lists are equal, otherwise false
   */
  public static <V> boolean equals(IList<V> a, IList<V> b) {
    return equals(a, b, Objects::equals);
  }

  /**
   * @param equals a comparison predicate for the lists of the element
   * @return true if the two lists are equal, otherwise false
   */
  public static <V> boolean equals(IList<V> a, IList<V> b, BiPredicate<V, V> equals) {
    if (a == b) {
      return true;
    } else if (a.size() != b.size()) {
      return false;
    }

    return Iterators.equals(a.iterator(), b.iterator(), equals);
  }

  /**
   * @return a hash for the list, which mimics the standard Java hash calculation
   */
  public static <V> long hash(IList<V> l) {
    return hash(l, Objects::hashCode, (a, b) -> (a * 31) + b);
  }

  /**
   * @param hash     a function which provides a hash for each element
   * @param combiner a function which combines the accumulated hash and element hash
   * @return a hash for the list
   */
  public static <V> long hash(IList<V> l, ToLongFunction<V> hash, LongBinaryOperator combiner) {
    return l.stream().mapToLong(hash).reduce(combiner).orElse(0);
  }

  /**
   * @return a string representation of the list, using toString() to represent each element
   */
  public static <V> String toString(IList<V> l) {
    return toString(l, Objects::toString);
  }

  /**
   * @param printer a function which returns a string representation of an element
   * @return a string representation fo the list
   */
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

  /**
   * @return a shim around the input list, presenting it as a standard Java List object
   */
  public static <V> java.util.List<V> toList(IList<V> list) {
    return new JavaList(list);
  }

  /**
   * @param start the inclusive start index of the slice
   * @param end   the exclusive end index of the slice
   * @return a subset view of the list, which holds onto a reference to the original
   */
  public static <V> IList<V> slice(IList<V> list, long start, long end) {
    IList<V> result;

    if (end <= start) {
      result = List.EMPTY;
    } else if (start < 0 || end > list.size()) {
      throw new IndexOutOfBoundsException("[" + start + "," + end + ") isn't a subset of [0,"+ list.size() + ")");
    } else if (end - start == list.size()) {
      result = list;
    } else {
      result = new DiffList<>(list).slice(start, end);
    }

    return list.isLinear() ? result.linear() : result;
  }

  /**
   * @return a view of the array as an IList
   */
  public static <V> IList<V> from(V[] array) {
    return Lists.from(array.length, idx -> array[(int) idx]);
  }

  /**
   * @return a view of the Java list as an IList
   */
  public static <V> IList<V> from(java.util.List<V> list) {
    LongFunction<V> nth = idx -> list.get((int) idx);
    return Lists.from(
        list.size(),
        nth,
        idx -> idx == 0 ? list.iterator() : Iterators.range(idx, list.size(), nth)
    );
  }

  /**
   * Creates a list which repeatedly uses the element function for each lookup.
   *
   * @param size      the size of the list
   * @param elementFn a function which returns the list for the given element
   * @return a list
   */
  public static <V> IList<V> from(long size, LongFunction<V> elementFn) {
    return from(size, elementFn, idx -> Iterators.range(idx, size, elementFn));
  }

  /**
   * Creates a list which repeatedly uses the element function for each lookup.
   *
   * @param size       the size of the list
   * @param elementFn  a function which returns the list for the given element
   * @param iteratorFn a function which generates an iterator for the list
   * @return a list
   */
  public static <V> IList<V> from(long size, LongFunction<V> elementFn, LongFunction<Iterator<V>> iteratorFn) {
    return new IList.Mixin<V>() {
      @Override
      public V nth(long idx) {
        if (idx < 0 || size <= idx) {
          throw new IndexOutOfBoundsException(idx + " must be within [0," + size + ")");
        }
        return elementFn.apply(idx);
      }

      @Override
      public Iterator<V> iterator(long startIndex) {
        return iteratorFn.apply(startIndex);
      }

      @Override
      public long size() {
        return size;
      }
    };
  }

  /**
   * @return a Java stream collector which can be used to construct a LinearList
   */
  public static <V> Collector<V, LinearList<V>, LinearList<V>> linearCollector() {
    return linearCollector(8);
  }

  /**
   * @param capacity the initial capacity of the list which collects values.
   * @return a Java stream collector which can be used to construct a LinearList
   */
  public static <V> Collector<V, LinearList<V>, LinearList<V>> linearCollector(int capacity) {
    return new Collector<V, LinearList<V>, LinearList<V>>() {
      @Override
      public Supplier<LinearList<V>> supplier() {
        return () -> new LinearList(capacity);
      }

      @Override
      public BiConsumer<LinearList<V>, V> accumulator() {
        return LinearList::addLast;
      }

      @Override
      public BinaryOperator<LinearList<V>> combiner() {
        return LinearList::linearConcat;
      }

      @Override
      public Function<LinearList<V>, LinearList<V>> finisher() {
        return x -> x;
      }

      @Override
      public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.IDENTITY_FINISH);
      }
    };
  }

  /**
   * @return a Java stream collector which can be used to construct a List
   */
  public static <V> Collector<V, List<V>, List<V>> collector() {
    return new Collector<V, List<V>, List<V>>() {
      @Override
      public Supplier<List<V>> supplier() {
        return () -> new List().linear();
      }

      @Override
      public BiConsumer<List<V>, V> accumulator() {
        return List::addLast;
      }

      @Override
      public BinaryOperator<List<V>> combiner() {
        return (a, b) -> (List<V>) a.concat(b);
      }

      @Override
      public Function<List<V>, List<V>> finisher() {
        return List::forked;
      }

      @Override
      public java.util.Set<Characteristics> characteristics() {
        return EnumSet.noneOf(Characteristics.class);
      }
    };
  }

  /**
   * @return an iterator over the list which repeatedly calls nth()
   */
  public static <V> Iterator<V> iterator(IList<V> list, long startIndex) {
    return Iterators.range(startIndex, list.size(), list::nth);
  }

  /**
   * @return a concatenation of the two lists, which is linear if {@code a} is linear
   */
  public static <V> IList<V> concat(IList<V> a, IList<V> b) {
    IList<V> result;
    if (a.size() == 0) {
      result = b;
    } else if (b.size() == 0) {
      result = a;
    } else if (a instanceof ConcatList) {
      result = a.concat(b);
    } else if (b instanceof ConcatList) {
      result = new ConcatList<>(a).concat(b);
    } else {
      result = new ConcatList<V>(a, b);
    }
    return a.isLinear() ? result.linear() : result.forked();
  }

  public static <V> IList<V> reverse(IList<V> l) {
    return Lists.from(l.size(), i -> l.nth(l.size() - (i + 1)));
  }

  public static <V> IList<V> sort(IList<V> l, Comparator<V> comparator) {
    Object[] array = l.toArray();
    Arrays.sort(array, (a, b) -> comparator.compare((V) a, (V) b));
    return (IList<V>) Lists.from(array);
  }

  public static <V> IList<V> sort(IList<V> l) {
    return sort(l, (Comparator<V>) Comparator.naturalOrder());
  }

  /**
   * @return a concatenation of all the lists
   */
  public static <V> IList<V> concat(IList<V>... lists) {
    return Arrays.stream(lists).reduce(Lists::concat).orElseGet(List::new);
  }
}
