package io.lacuna.bifurcan;

import java.util.Collection;
import java.util.Optional;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public class LinearList<V> implements IList<V> {

  private static final int DEFAULT_CAPACITY = 8;

  private final Object[] elements;
  private int size;

  public LinearList() {
    this(DEFAULT_CAPACITY);
  }

  public LinearList(Collection<V> collection) {
    this.elements = collection.toArray();
    this.size = elements.length;
  }

  public LinearList(IList<V> list) {
    this.elements = list.toArray();
    this.size = elements.length;
  }

  public LinearList(int capacity) {
    this.elements = new Object[capacity];
  }

  public LinearList<V> resize(int newSize) {
    if (newSize == elements.length) {
      return this;
    } else if (newSize < size) {
      throw new IllegalArgumentException("new size smaller than current size");
    }

    LinearList list = new LinearList(newSize);
    System.arraycopy(elements, 0, list.elements, 0, size);
    list.size = size;
    return list;
  }

  /**
   * @return returns the most recently pushed/appended value, or nothing if the list is empty
   */
  public Optional<V> peek() {
    return size > 0 ? Optional.of((V) elements[size - 1]) : Optional.empty();
  }

  /**
   * @return a list with {@code value} appended
   */
  public IList<V> push(V value) {
    return append(value);
  }

  /**
   * @return a list with the most recently pushed/appended value removed, or the same list if there are no elements
   */
  public IList<V> pop() {
    size = Math.max(0, size - 1);
    return this;
  }

  @Override
  public IList<V> append(V value) {
    if (size == elements.length) {
      return resize(size << 1).append(value);
    }

    elements[size++] = value;
    return this;
  }

  @Override
  public IList<V> set(long idx, V value) {
    if (idx == size) {
      return append(value);
    } else if (idx > Integer.MAX_VALUE) {
      throw new IndexOutOfBoundsException();
    }

    elements[(int) idx] = value;
    return this;
  }

  @Override
  public V nth(long idx) {
    if (idx < 0 || idx >= size) {
      throw new IndexOutOfBoundsException();
    }
    return (V) elements[(int) idx];
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IList<V> forked() {
    // todo return tree list
    return null;
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
}
