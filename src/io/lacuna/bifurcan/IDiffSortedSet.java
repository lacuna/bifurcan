package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;

public interface IDiffSortedSet<V> extends ISortedSet<V> {

  IDiffSortedMap<V, Void> diffMap();

  @Override
  default Comparator<V> comparator() {
    return diffMap().comparator();
  }
  @Override
  default ToLongFunction<V> valueHash() {
    return diffMap().keyHash();
  }

  @Override
  default BiPredicate<V, V> valueEquality() {
    return diffMap().keyEquality();
  }

  @Override
  default long size() {
    return diffMap().size();
  }

  @Override
  default OptionalLong floorIndex(V value) {
    return diffMap().floorIndex(value);
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
