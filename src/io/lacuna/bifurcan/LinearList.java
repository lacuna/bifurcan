package io.lacuna.bifurcan;

import java.util.Collection;
import java.util.Optional;

/**
 * A simple implementation of a list, mimicking most behaviors of Java's ArrayList, and providing {@code peek()},
 * {@code push()}, and {@code pop()} methods for use as a stack.
 *
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
    this.elements = new Object[Math.max(1, capacity)];
  }

  /**
   * @param newCapacity the updated capacity of the list, which cannot be smaller than the current size
   * @return a resized list
   * @throws IllegalArgumentException when {@code newCapacity} is smaller than {@code size()}
   */
  public LinearList<V> resize(int newCapacity) {
    if (newCapacity == elements.length) {
      return this;
    } else if (newCapacity < size) {
      throw new IllegalArgumentException("new capacity (" + newCapacity + ") smaller than current size: " + size);
    }

    LinearList list = new LinearList(newCapacity);
    System.arraycopy(elements, 0, list.elements, 0, size);
    list.size = size;
    return list;
  }

  /**
   * @return returns the most recently pushed/appended rowValue, or nothing if the list is empty
   */
  public Optional<V> peek() {
    return size > 0 ? Optional.of((V) elements[size - 1]) : Optional.empty();
  }

  /**
   * @return a list with {@code rowValue} appended
   */
  public IList<V> push(V value) {
    return append(value);
  }

  /**
   * @return a list with the most recently pushed/appended rowValue removed, or the same list if there are no elements
   */
  public IList<V> pop() {
    size = Math.max(0, size - 1);
    elements[size] = null;
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
}
