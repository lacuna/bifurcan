package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.Util;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public interface IDiffMap<K, V> extends IMap<K, V>, IDiff<IMap<K, V>, IEntry<K, V>> {

  /**
   * The baseline data structure.
   */
  IMap<K, V> underlying();

  /**
   * Entries which have been added to the underlying data structure, some of which may shadow underlying entries.
   */
  IMap<K, V> added();

  /**
   * Indices which have been removed or shadowed from the underlying data structure.
   */
  ISortedSet<Long> removedIndices();

  @Override
  default ToIntFunction<K> keyHash() {
    return underlying().keyHash();
  }

  @Override
  default BiPredicate<K, K> keyEquality() {
    return underlying().keyEquality();
  }

  @Override
  default V get(K key, V defaultValue) {
    V v = added().get(key, defaultValue);
    if (v != defaultValue) {
      return v;
    } else  {
      long idx = underlying().indexOf(key);
      if (idx < 0 || Util.removedPredecessors(removedIndices(), idx) < 0) {
        return defaultValue;
      } else {
        return underlying().nth(idx).value();
      }
    }
  }

  @Override
  default long indexOf(K key) {
    long addedIdx = added().indexOf(key);
    if (addedIdx >= 0) {
      return underlying().size() - removedIndices().size() + addedIdx;
    }

    long underlyingIdx = underlying().indexOf(key);
    if (underlyingIdx < 0) {
      return -1;
    }

    long removed = Util.removedPredecessors(removedIndices(), underlyingIdx);
    if (removed == -1) {
      return -1;
    }

    return underlyingIdx - removed;
  }

  @Override
  default long size() {
    return underlying().size() + added().size() - removedIndices().size();
  }

  @Override
  default IEntry<K, V> nth(long index) {
    long underlyingSize = underlying().size() - removedIndices().size();
    if (index < underlyingSize) {
      return underlying().nth(Util.offsetIndex(removedIndices(), index));
    } else {
      return added().nth(index - underlyingSize);
    }
  }

  @Override
  default IList<IEntry<K, V>> entries() {
    return Lists.from(size(), this::nth, () ->
        Iterators.concat(
            Util.skipIndices(underlying().entries().iterator(), removedIndices().iterator()),
            added().entries().iterator()));
  }
}
