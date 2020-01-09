package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.Util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public interface IDiffSortedMap<K, V> extends ISortedMap<K, V> {

  /**
   * The baseline data structure.
   */
  ISortedMap<K, V> underlying();

  /**
   * Entries which have been added to the underlying data structure, some of which may shadow underlying entries.
   */
  ISortedMap<K, V> added();

  /**
   * The indices of the added keys within the combined data structure.
   */
  ISortedMap<Long, K> addedKeys();

  /**
   * Indices which have been removed or shadowed from the underlying data structure.
   */
  ISortedSet<Long> removedIndices();

  @Override
  default ToLongFunction<K> keyHash() {
    return underlying().keyHash();
  }

  @Override
  default BiPredicate<K, K> keyEquality() {
    return underlying().keyEquality();
  }

  @Override
  default Comparator<K> comparator() {
    return underlying().comparator();
  }

  @Override
  default V get(K key, V defaultValue) {
    V v = added().get(key, defaultValue);
    if (v != defaultValue) {
      return v;
    } else  {
      OptionalLong idx = underlying().indexOf(key);
      if (!idx.isPresent() || removedIndices().contains(idx.getAsLong())) {
        return defaultValue;
      } else {
        return underlying().nth(idx.getAsLong()).value();
      }
    }
  }

  @Override
  default long size() {
    return underlying().size() + added().size() - removedIndices().size();
  }

  @Override
  default IEntry<K, V> floor(K key) {
    return null;
  }

  @Override
  default IEntry<K, V> ceil(K key) {
    return null;
  }

  @Override
  default IEntry<K, V> nth(long index) {
    IEntry<Long, K> addedFloor = addedKeys().floor(index);
    if (addedFloor.key() == index) {
      return IEntry.of(addedFloor.value(), added().get(addedFloor.value()).get());
    } else {
      return underlying().nth(Util.offsetIndex(removedIndices(), index - addedFloor.key()));
    }
  }

  @Override
  default Iterator<IEntry<K, V>> iterator() {
    return null;
  }
}
