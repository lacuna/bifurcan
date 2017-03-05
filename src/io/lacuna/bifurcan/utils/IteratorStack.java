package io.lacuna.bifurcan.utils;

import io.lacuna.bifurcan.LinearList;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author ztellman
 */
public class IteratorStack<V> implements Iterator<V> {

  LinearList<Iterator<V>> iterators = new LinearList<>();

  public IteratorStack() {
  }

  public IteratorStack(Iterator<V>... its) {
    for (Iterator<V> it : its) {
      iterators.addFirst(it);
    }
  }

  private void primeIterator() {
    while (iterators.size() > 0 && !iterators.first().hasNext()) {
      iterators.removeFirst();
    }
  }

  @Override
  public boolean hasNext() {
    primeIterator();
    return iterators.size() > 0 && iterators.first().hasNext();
  }

  @Override
  public V next() {
    primeIterator();
    if (iterators.size() == 0) {
      throw new NoSuchElementException();
    }
    return iterators.first().next();
  }

  public void addFirst(Iterator<V> it) {
    iterators.addFirst(it);
  }

  public void addLast(Iterator<V> it) {
    iterators.addLast(it);
  }
}
