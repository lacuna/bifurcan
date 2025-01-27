package io.lacuna.bifurcan;

import java.util.*;

import static io.lacuna.bifurcan.utils.Bits.log2Ceil;

/**
 * A simple implementation of a mutable list combining the best characteristics of {@link java.util.ArrayList} and
 * {@link java.util.ArrayDeque}, allowing elements to be added and removed from both ends of the collection <i>and</i>
 * allowing random-access reads and updates.  Unlike {@link List}, it can only hold {@code Integer.MAX_VALUE} elements.
 * <p>
 * Calls to {@link #concat(IList)}, {@link #slice(long, long)}, and {@link #split(int)} create virtual collections which
 * retain a reference to the whole underlying collection, and are somewhat less efficient than {@code LinearList}.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearList<V> extends IList.Mixin<V> implements Cloneable {

  private static final int DEFAULT_CAPACITY = 4;

  private Object[] elements;
  private int mask;
  private int size, offset;

  public static <V> LinearList<V> of(V... elements) {
    LinearList<V> list = new LinearList<V>(elements.length);
    for (V e : elements) {
      list.addLast(e);
    }
    return list;
  }

  /**
   * @return a list containing the entries of {@code collection}
   */
  public static <V> LinearList<V> from(Collection<V> collection) {
    return collection.stream().collect(Lists.linearCollector(collection.size()));
  }

  /**
   * @return a list containing the elements of {@code iterable}
   */
  public static <V> LinearList<V> from(Iterable<V> iterable) {
    return from(iterable.iterator());
  }

  /**
   * @return a list containing all remaining elements of {@code iterator}
   */
  public static <V> LinearList<V> from(Iterator<V> iterator) {
    LinearList<V> list = new LinearList<V>();
    iterator.forEachRemaining(list::addLast);
    return list;
  }

  /**
   * @return a list containing the elements of {@code list}
   */
  public static <V> LinearList<V> from(IList<V> list) {
    if (list.size() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("LinearList cannot hold more than 1 << 30 entries");
    } else if (list instanceof LinearList) {
      return ((LinearList<V>) list).clone();
    } else {
      return list.stream().collect(Lists.linearCollector((int) list.size()));
    }
  }

  public LinearList() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * @param capacity the initial capacity of the list
   */
  public LinearList(int capacity) {
    this(0, new Object[Math.max(1, 1 << log2Ceil(capacity))]);
  }

  private LinearList(int size, Object[] elements) {
    this.size = size;
    this.offset = 0;
    this.mask = elements.length - 1;
    this.elements = elements;
  }

  ///

  private void resize(int newCapacity) {

    Object[] nElements = new Object[newCapacity];

    int truncatedSize = Math.min(size, elements.length - offset);
    System.arraycopy(elements, offset, nElements, 0, truncatedSize);
    if (size != truncatedSize) {
      System.arraycopy(elements, 0, nElements, truncatedSize, size - truncatedSize);
    }

    mask = nElements.length - 1;
    elements = nElements;
    offset = 0;
  }

  @Override
  public boolean isLinear() {
    return true;
  }

  @Override
  public LinearList<V> addLast(V value) {
    if (size == elements.length) {
      resize(size << 1);
    }
    elements[(offset + size++) & mask] = value;
    super.hash = -1;

    return this;
  }

  @Override
  public LinearList<V> addFirst(V value) {
    if (size == elements.length) {
      resize(size << 1);
    }
    offset = (offset - 1) & mask;
    elements[offset] = value;
    size++;
    super.hash = -1;

    return this;
  }

  @Override
  public LinearList<V> removeFirst() {
    if (size == 0) {
      return this;
    }
    offset = (offset + 1) & mask;
    size--;
    super.hash = -1;

    return this;
  }

  @Override
  public LinearList<V> removeLast() {
    if (size == 0) {
      return this;
    }
    elements[(offset + --size) & mask] = null;
    super.hash = -1;

    return this;
  }

  public LinearList<V> clear() {
    Arrays.fill(elements, null);
    offset = 0;
    size = 0;
    super.hash = -1;

    return this;
  }

  @Override
  public LinearList<V> set(long idx, V value) {
    if (idx == size) {
      return addLast(value);
    } else if (idx > Integer.MAX_VALUE) {
      throw new IndexOutOfBoundsException();
    }

    elements[(int) (offset + (int) idx) & mask] = value;
    super.hash = -1;

    return this;
  }

  LinearList<V> linearConcat(IList<V> l) {
    long newSize = size() + l.size();
    if (newSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("cannot hold more than 1 << 31 entries");
    }

    if (l instanceof LinearList) {
      LinearList<V> list = (LinearList<V>) l;

      if (offset != 0 || newSize > elements.length) {
        resize(1 << log2Ceil(newSize));
      }

      int truncatedListSize = Math.min(list.size, list.elements.length - list.offset);
      System.arraycopy(list.elements, list.offset, elements, size, truncatedListSize);
      if (list.size != truncatedListSize) {
        System.arraycopy(list.elements, 0, elements, size + truncatedListSize, list.size - truncatedListSize);
      }
      size += list.size();
      super.hash = -1;

    } else {
      for (V e : l) {
        addLast(e);
      }
    }

    return this;
  }

  @Override
  public V nth(long idx) {
    if (idx < 0 || idx >= size) {
      throw new IndexOutOfBoundsException(idx + " must be within [0," + size + ")");
    }
    return (V) elements[(offset + (int) idx) & mask];
  }

  /**
   * Removes, and returns, the first element of the list.
   *
   * @return the first element of the list
   * @throws IndexOutOfBoundsException if the list is empty
   */
  public V popFirst() {
    V val = first();
    removeFirst();
    return val;
  }

  /**
   * Removes, and returns, the last element of the list.
   *
   * @return the last element of the list
   * @throws IndexOutOfBoundsException if the list is empty
   */
  public V popLast() {
    V val = last();
    removeLast();
    return val;
  }

  @Override
  public Iterator<V> iterator() {

    return new Iterator<V>() {

      final int limit = offset + size;
      int idx = offset;

      @Override
      public boolean hasNext() {
        return idx != limit;
      }

      @Override
      public V next() {
        if (idx == limit) {
          throw new NoSuchElementException();
        }

        V val = (V) elements[idx++ & mask];
        return val;
      }
    };
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IList<V> forked() {
    return List.from(this);
  }

  @Override
  public IList<V> linear() {
    return this;
  }

  @Override
  public LinearList<V> clone() {
    LinearList<V> l = new LinearList<V>(size, elements.clone());
    l.offset = offset;
    return l;
  }
}
