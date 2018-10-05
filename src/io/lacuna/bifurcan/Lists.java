package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.lang.Math.min;

/**
 * Utility functions for classes implementing {@code IList}.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class Lists {

  public static final IList EMPTY = new IList() {
    @Override
    public Object nth(long idx) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public IList set(long idx, Object value) {
      if (idx == 0) {
        return addLast(value);
      } else {
        throw new IndexOutOfBoundsException();
      }
    }

    @Override
    public IList addLast(Object value) {
      return new List().addLast(value);
    }

    @Override
    public IList addFirst(Object value) {
      return new List().addFirst(value);
    }

    @Override
    public IList removeLast() {
      return this;
    }

    @Override
    public IList removeFirst() {
      return this;
    }

    @Override
    public IList forked() {
      return this;
    }

    @Override
    public IList linear() {
      return new List().linear();
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IList) {
        return ((IList) obj).size() == 0;
      }
      return false;
    }

    @Override
    public IList clone() {
      return this;
    }

    @Override
    public String toString() {
      return Lists.toString(this);
    }
  };

  /**
   * A concatenation wrapper that doesn't blow up the stack due to left-leaning trees.
   */
  private static class Concat<V> implements IList<V> {

    final IntMap<IList<V>> lists;
    final long size;

    // both constructors assume the lists are non-empty
    Concat(IList<V> a, IList<V> b) {
      lists = new IntMap<IList<V>>().linear().put(0, a).put(a.size(), b).forked();
      size = a.size() + b.size();
    }

    Concat(IList<V> list) {
      lists = new IntMap<IList<V>>().linear().put(0, list).linear();
      size = list.size();
    }

    private Concat(IntMap<IList<V>> lists, long size) {
      this.lists = lists;
      this.size = size;
    }

    @Override
    public V nth(long idx) {
      if (idx < 0 || size <= idx) {
        throw new IndexOutOfBoundsException(idx + " must be within [0," + size + ")");
      }
      IEntry<Long, IList<V>> entry = lists.floor(idx);
      return entry.value().nth(idx - entry.key());
    }

    @Override
    public Iterator<V> iterator() {
      return Iterators.flatMap(lists.iterator(), e -> e.value().iterator());
    }

    @Override
    public long size() {
      return size;
    }

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
    public IList<V> slice(long start, long end) {
      if (end > size() || start < 0) {
        throw new IndexOutOfBoundsException();
      } else if (start == 0 && end == size()) {
        return this;
      } else if (start == end) {
        return EMPTY;
      }

      IntMap<IList<V>> m = new IntMap<IList<V>>().linear();
      long pos = start;
      while (pos < end) {
        IEntry<Long, IList<V>> e = lists.floor(pos);
        IList<V> l = e.value().slice(pos - e.key(), min(end - e.key(), e.value().size()));
        m = m.put(pos - start, l);
        pos = e.key() + e.value().size();
      }
      return new Concat<V>(m.forked(), end - start);
    }

    Concat<V> concat(Concat<V> o) {
      IntMap<IList<V>> m = lists.linear();
      long nSize = size;
      for (IList<V> l : o.lists.values()) {
        if (l.size() > 0) {
          m = m.put(nSize, l);
          nSize += l.size();
        }
      }
      return new Concat<V>(m.forked(), nSize);
    }

    @Override
    public IList<V> clone() {
      return this;
    }
  }

  private static class Slice<V> implements IList<V> {
    private final IList<V> list;
    private final long offset;
    private final long size;

    Slice(IList<V> list, long offset, long size) {
      this.list = list;
      this.offset = offset;
      this.size = size;
    }

    @Override
    public V nth(long idx) {
      if (idx < 0 || size <= idx) {
        throw new IndexOutOfBoundsException(idx + " must be within [0," + size + ")");
      }
      return list.nth(offset + idx);
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public IList<V> slice(long start, long end) {
      if (start == end) {
        return EMPTY;
      } else if (start < 0 || end <= start || end > size) {
        throw new IllegalArgumentException();
      }
      return new Slice<V>(list, offset + start, end - start);
    }

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
    public IList<V> clone() {
      return this;
    }

    @Override
    public String toString() {
      return Lists.toString(this);
    }
  }

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
      return Lists.toList(list.slice(fromIndex, toIndex));
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
  }

  static class VirtualList<V> implements IList<V> {

    private IList<V> prefix, base, suffix;
    private final boolean linear;

    public VirtualList(IList<V> base) {
      this(Lists.EMPTY, base, Lists.EMPTY, false);
    }

    private VirtualList(IList<V> prefix, IList<V> base, IList<V> suffix, boolean linear) {
      this.prefix = prefix;
      this.base = base;
      this.suffix = suffix;
      this.linear = linear;
    }

    @Override
    public V nth(long idx) {
      long prefixSize = prefix.size();
      long listSize = base.size();

      if (idx < prefixSize) {
        return prefix.nth(idx);
      } else if (idx < (prefixSize + listSize)) {
        return base.nth(idx - prefixSize);
      } else {
        return suffix.nth(idx - (prefixSize + listSize));
      }
    }

    @Override
    public Iterator<V> iterator() {
      if (prefix == Lists.EMPTY && suffix == Lists.EMPTY) {
        return base.iterator();
      } else {
        return Lists.iterator(this);
      }
    }

    @Override
    public long size() {
      return prefix.size() + base.size() + suffix.size();
    }

    @Override
    public IList<V> addLast(V value) {
      IList<V> suffixPrime = suffix.addLast(value);
      return linear ? this : new VirtualList<V>(prefix, base, suffixPrime, false);
    }

    @Override
    public IList<V> addFirst(V value) {
      IList<V> prefixPrime = prefix.addFirst(value);
      return linear ? this : new VirtualList<V>(prefixPrime, base, suffix, false);
    }

    @Override
    public IList<V> removeLast() {
      if (suffix.size() > 0) {
        IList<V> suffixPrime = suffix.removeLast();
        return linear ? this : new VirtualList<V>(prefix, base, suffixPrime, false);
      }

      if (base.size() > 0) {
        IList<V> basePrime = base.slice(0, base.size() - 1);
        if (linear) {
          base = basePrime;
          return this;
        } else {
          return new VirtualList<V>(prefix, basePrime, suffix, false);
        }
      }

      IList<V> prefixPrime = prefix.removeLast();
      return linear ? this : new VirtualList<V>(prefixPrime, base, suffix, false);
    }

    @Override
    public IList<V> removeFirst() {
      if (prefix.size() > 0) {
        IList<V> prefixPrime = prefix.removeFirst();
        return linear ? this : new VirtualList<V>(prefixPrime, base, suffix, false);
      }

      if (base.size() > 0) {
        IList<V> basePrime = base.slice(1, base.size());
        if (linear) {
          base = basePrime;
          return this;
        } else {
          return new VirtualList<V>(prefix, basePrime, suffix, false);
        }
      }

      IList<V> suffixPrime = suffix.removeFirst();
      return linear ? this : new VirtualList<V>(prefix, base, suffixPrime, false);
    }

    @Override
    public IList<V> set(long idx, V value) {
      long prefixSize = prefix.size();
      long baseSize = base.size();

      if (idx < 0 || idx > size()) {
        throw new IndexOutOfBoundsException();
      } else if (idx == size()) {
        return addLast(value);
      } else if (idx < prefixSize) {
        IList<V> prefixPrime = prefix.set(idx, value);
        return linear ? this : new VirtualList<V>(prefixPrime, base, suffix, false);
      } else if (idx < (prefixSize + baseSize)) {
        idx -= prefixSize;
        IList<V> basePrime = Lists.concat(
          base.slice(0, idx),
          new LinearList(1).addLast(value),
          base.slice(idx + 1, base.size()));
        return new VirtualList<V>(prefix, basePrime, suffix, linear);
      } else {
        idx -= prefixSize + baseSize;
        IList<V> suffixPrime = suffix.set(idx, value);
        return linear ? this : new VirtualList<V>(prefix, base, suffixPrime, false);
      }
    }

    @Override
    public IList<V> forked() {
      return linear ? new VirtualList<V>(prefix.forked(), base, suffix.forked(), false) : this;
    }

    @Override
    public IList<V> linear() {
      return linear ? this : new VirtualList<V>(prefix.linear(), base, suffix.linear(), true);
    }

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
    public VirtualList<V> clone() {
      return new VirtualList<V>(prefix.clone(), base, suffix.clone(), isLinear());
    }

    @Override
    public String toString() {
      return Lists.toString(this);
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
    return Lists.from(l.size(), i -> f.apply(l.nth(i)));
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

    long size = end - start;
    if (size == 0) {
      result = Lists.EMPTY;
    } else if (start < 0 || end > list.size()) {
      throw new IllegalArgumentException();
    } else {
      result = new Slice<V>(list, start, size);
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
    return Lists.from(list.size(), idx -> list.get((int) idx), list::iterator);
  }

  /**
   * Creates a list which repeatedly uses the element function for each lookup.
   *
   * @param size      the size of the list
   * @param elementFn a function which returns the list for the given element
   * @return a list
   */
  public static <V> IList<V> from(long size, LongFunction<V> elementFn) {
    return from(size, elementFn, () -> Iterators.range(size, elementFn));
  }

  /**
   * Creates a list which repeatedly uses the element function for each lookup.
   *
   * @param size       the size of the list
   * @param elementFn  a function which returns the list for the given element
   * @param iteratorFn a function which generates an iterator for the list
   * @return a list
   */
  public static <V> IList<V> from(long size, LongFunction<V> elementFn, Supplier<Iterator<V>> iteratorFn) {
    return new IList<V>() {
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
      public V nth(long idx) {
        if (idx < 0 || size <= idx) {
          throw new IndexOutOfBoundsException(idx + " must be within [0," + size + ")");
        }
        return elementFn.apply(idx);
      }

      @Override
      public Iterator<V> iterator() {
        return iteratorFn.get();
      }

      @Override
      public IList<V> clone() {
        return this;
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
  public static <V> Iterator<V> iterator(IList<V> list) {
    return Iterators.range(list.size(), list::nth);
  }

  /**
   * @return a concatenation of the two lists, which is linear if {@code a} is linear
   */
  public static <V> IList<V> concat(IList<V> a, IList<V> b) {
    IList<V> result;
    if (a.size() == 0) {
      result = b.forked();
    } else if (b.size() == 0) {
      result = a.forked();
    } else if (a instanceof Concat && b instanceof Concat) {
      result = ((Concat<V>) a).concat((Concat<V>) b);
    } else if (a instanceof Concat) {
      result = ((Concat<V>) a).concat(new Concat<>(b));
    } else if (b instanceof Concat) {
      result = new Concat<>(a).concat((Concat<V>) b);
    } else {
      result = new Concat<V>(a, b);
    }
    return a.isLinear() ? result.linear() : result;
  }

  public static <V> IList<V> reverse(IList<V> l) {
    return Lists.from(l.size(), i -> l.nth(l.size() - (i + 1)));
  }

  public static <V> IList<V> sort(IList<V> l, Comparator<V> comparator) {
    Object[] array = l.toArray();
    Arrays.sort(array, (a, b) -> comparator.compare((V) a, (V) b));
    return (IList<V>) Lists.from(array);
  }

  /**
   * @return a concatenation of all the lists
   */
  public static <V> IList<V> concat(IList<V>... lists) {
    return Arrays.stream(lists).reduce(Lists::concat).orElseGet(List::new);
  }
}
