package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.Util;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.function.BiPredicate;
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

  static <K, V> IList<IDiffMap<K, V>> diffStack(IDiffMap<K, V> m) {
    List<IDiffMap<K, V>> result = new List<IDiffMap<K, V>>().linear();
    IDiffMap<K, V> curr = m;
    for (;;) {
      result.addFirst(curr);
      if (curr.underlying() instanceof IDiffMap) {
        curr = (IDiffMap<K, V>) curr.underlying();
      } else {
        break;
      }
    }
    return result;
  }

  static PrimitiveIterator.OfLong compactedRemovedIndices(IList<IDiffMap<?, ?>> diffStack) {
    IList<Iterator<Long>> iterators = new LinearList<>();
    long underlyingSize = diffStack.first().underlying().size();
    long removed = 0;
    for (IDiffMap<?, ?> m : diffStack) {
      long underlyingRemainingSize = underlyingSize - removed;
      ISortedSet<Long> s = m.removedIndices().slice(0L, underlyingRemainingSize - 1);
      iterators.addLast(s.iterator());
      removed += s.size();
    }
    return Util.mergedRemovedIndices(iterators);
  }

  static <K, V> Iterator<IEntry<K, V>> compactedAddedEntries(IList<IDiffMap<K, V>> diffStack) {
    IList<Iterator<Long>> iterators = new LinearList<>();
    long underlyingSize = diffStack.first().underlying().size();
    long removed = 0;
    for (IDiffMap<?, ?> m : diffStack) {
      long underlyingRemainingSize = underlyingSize - removed;
      ISortedSet<Long> underlyingIndices = m.removedIndices().slice(0L, underlyingRemainingSize - 1);
      ISortedSet<Long> addedIndices = m.removedIndices().slice(underlyingRemainingSize, Long.MAX_VALUE);
      iterators.addLast(Iterators.map(addedIndices.iterator(), n -> n - underlyingRemainingSize));
      removed += underlyingIndices.size();
    }

    PrimitiveIterator.OfLong removedFromAdded = Util.mergedRemovedIndices(iterators);
    IList<IEntry<K, V>> added = diffStack.stream().map(m -> m.added().entries()).reduce(Lists::concat).get();
    return Util.skipIndices(added.iterator(), removedFromAdded);
  }
}
