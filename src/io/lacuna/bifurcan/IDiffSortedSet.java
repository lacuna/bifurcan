package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalLong;

public interface IDiffSortedSet<V> extends IDiff<ISortedSet<V>>, ISortedSet<V> {

  interface Durable<V> extends IDiffSortedSet<V>, IDurableCollection {
  }

  IDiffSortedMap<V, Void> diffMap();

  IDiffSortedSet<V> rebase(ISortedSet<V> newUnderlying);

  @Override
  default ISortedSet<V> underlying() {
    return diffMap().underlying().keys();
  }

  @Override
  default Comparator<V> comparator() {
    return diffMap().comparator();
  }

  @Override
  default long size() {
    return diffMap().size();
  }

  @Override
  default OptionalLong inclusiveFloorIndex(V value) {
    return diffMap().inclusiveFloorIndex(value);
  }

  @Override
  default V nth(long idx) {
    return diffMap().nth(idx).key();
  }

  @Override
  default Iterator<V> iterator() {
    return Iterators.map(diffMap().iterator(), IEntry::key);
  }
}
