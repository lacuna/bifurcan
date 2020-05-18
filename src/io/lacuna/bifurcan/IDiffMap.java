package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.Util;
import io.lacuna.bifurcan.utils.Iterators;

import javax.swing.text.html.Option;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

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
  default ToLongFunction<K> keyHash() {
    return underlying().keyHash();
  }

  @Override
  default BiPredicate<K, K> keyEquality() {
    return underlying().keyEquality();
  }

  @Override
  default OptionalLong indexOf(K key) {
    OptionalLong addedIdx = added().indexOf(key);
    if (addedIdx.isPresent()) {
      return OptionalLong.of(underlying().size() - removedIndices().size() + addedIdx.getAsLong());
    }

    OptionalLong underlyingIdx = underlying().indexOf(key);
    if (!underlyingIdx.isPresent()) {
      return underlyingIdx;
    }

    OptionalLong predecessors = Util.removedPredecessors(removedIndices(), underlyingIdx.getAsLong());
    if (!predecessors.isPresent()) {
      return predecessors;
    }

    return OptionalLong.of(underlyingIdx.getAsLong() - predecessors.getAsLong());
  }

  @Override
  default long size() {
    return underlying().size() + added().size() - removedIndices().size();
  }

  @Override
  default IEntry<K, V> nth(long idx) {
    long underlyingSize = underlying().size() - removedIndices().size();
    if (idx < underlyingSize) {
      return underlying().nth(Util.offsetIndex(removedIndices(), idx));
    } else {
      return added().nth(idx - underlyingSize);
    }
  }

  @Override
  default Iterator<IEntry<K, V>> iterator() {
    return Iterators.concat(
        Util.skipIndices(underlying().entries().iterator(), removedIndices().iterator()),
        added().entries().iterator());
  }
}
