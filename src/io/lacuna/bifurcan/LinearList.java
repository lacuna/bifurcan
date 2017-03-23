package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Bits;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import static io.lacuna.bifurcan.utils.Bits.log2Ceil;

/**
 * A simple implementation of a list, mimicking most behaviors of Java's ArrayDeque.
 *
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearList<V> implements IList<V>, Cloneable {

  private static final int DEFAULT_CAPACITY = 8;

  public Object[] elements;
  public int mask;
  public int size, offset;

  public LinearList() {
    this(DEFAULT_CAPACITY);
  }

  public LinearList(int capacity) {
    this(0, new Object[Math.max(1, 1 << log2Ceil(capacity))]);
  }

  private LinearList(int size, Object[] elements) {
    this.size = size;
    this.offset = 0;
    this.mask = elements.length - 1;
    this.elements = elements;
  }

  public static <V> LinearList<V> from(Collection<V> collection) {
    return collection.stream().collect(Lists.linearCollector(collection.size()));
  }

  public static <V> LinearList<V> from(Iterable<V> iterable) {
    return from(iterable.iterator());
  }

  public static <V> LinearList<V> from(Iterator<V> iterator) {
    LinearList<V> list = new LinearList<V>();
    while (iterator.hasNext()) {
      list.addLast(iterator.next());
    }
    return list;
  }

  public static <V> LinearList<V> from(IList<V> list) {
    if (list.size() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("LinearList cannot hold more than 1 << 30 entries");
    }
    return list.stream().collect(Lists.linearCollector((int) list.size()));
  }

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
  public LinearList<V> addLast(V value) {
    if (size == elements.length) {
      resize(size << 1);
    }
    elements[(offset + size++) & mask] = value;
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
    return this;
  }

  @Override
  public LinearList<V> removeFirst() {
    if (size == 0) {
      return this;
    }
    elements[offset] = null;
    offset = (offset + 1) & mask;
    size--;
    return this;
  }

  @Override
  public LinearList<V> removeLast() {
    if (size == 0) {
      return this;
    }
    elements[(offset + --size) & mask] = null;
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
    return this;
  }

  @Override
  public LinearList<V> concat(IList<V> l) {
    long newSize = size() + l.size();
    if (newSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("cannot hold more than 1 << 31 entries");
    }

    if (l instanceof LinearList) {
      if (offset != 0 || newSize > elements.length) {
        resize(1 << log2Ceil(newSize));
      }

      LinearList<V> list = (LinearList<V>) l;
      int truncatedListSize = Math.min(list.size, list.elements.length - list.offset);

      System.arraycopy(list.elements, list.offset, elements, size, truncatedListSize);
      if (list.size != truncatedListSize) {
        System.arraycopy(list.elements, 0, elements, size + truncatedListSize, list.size - truncatedListSize);
      }
      size += list.size();
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

  public V popFirst() {
    V val = first();
    removeFirst();
    return val;
  }

  public V popLast() {
    V val = last();
    removeLast();
    return val;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IList<V> forked() {
    throw new UnsupportedOperationException("a LinearList cannot be efficiently transformed into a forked representation");
  }

  @Override
  public IList<V> linear() {
    return this;
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
  public LinearList<V> clone() {
    LinearList<V> l = new LinearList<V>(size, elements.clone());
    l.offset = offset;
    return l;
  }
}
