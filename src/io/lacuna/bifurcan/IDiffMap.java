package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.Util;
import io.lacuna.bifurcan.durable.codecs.SkipTable;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
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

  static PrimitiveIterator.OfLong mergedRemovedIndices(IList<IDiffMap<?, ?>> diffStack) {
    assert diffStack.stream().allMatch(m -> m instanceof IMap.Durable);

    // isolate the removed indices which only apply to the underlying collection (which is potentially shrinking with
    // each new stacked diff)
    IList<Iterator<Long>> iterators = new LinearList<>();
    long underlyingSize = diffStack.first().underlying().size();
    long removed = 0;
    for (IDiffMap<?, ?> m : diffStack) {
      long remainingUnderlyingSize = underlyingSize - removed;
      ISortedSet<Long> s = m.removedIndices().slice(0L, remainingUnderlyingSize - 1);
      iterators.addLast(s.iterator());
      removed += s.size();
    }

    return Util.mergedRemovedIndices(iterators);
  }

  static <K, V> Iterator<IEntry.WithHash<K, V>> mergedAddedEntries(IList<IDiffMap<K, V>> diffStack) {
    assert diffStack.stream().allMatch(m -> m instanceof IMap.Durable);

    // isolate the removed indices which only apply to the added entries
    IList<Iterator<Long>> iterators = new LinearList<>();
    long underlyingSize = diffStack.first().underlying().size();
    long removed = 0;
    for (IDiffMap<K, V> m : diffStack) {
      long remainingUnderlyingSize = underlyingSize - removed;
      ISortedSet<Long> underlyingIndices = m.removedIndices().slice(0L, remainingUnderlyingSize - 1);
      ISortedSet<Long> addedIndices = m.removedIndices().slice(remainingUnderlyingSize, Long.MAX_VALUE);
      iterators.addLast(Iterators.map(addedIndices.iterator(), n -> n - remainingUnderlyingSize));
      removed += underlyingIndices.size();
    }

    SkipTable.Writer writer = new SkipTable.Writer();
    Util.mergedRemovedIndices(iterators).forEachRemaining((long idx) -> writer.append(idx, 0));

    // for this to consume too much memory would require >100M entries being repeatedly overwritten within the stack
    // of diffs, which implies that many entries being in-memory at once, which seems far-fetched enough that I'm not
    // going to worry about it for now
    // TODO: worry about it
    ISortedSet<Long> removedIndices = writer.toOffHeapMap().keys();

    // get the hash-sorted entries (which are in the same order as entries() because it's a durable map) from each
    // added() and filter out the removed entries from each
    IList<Iterator<IEntry.WithHash<K, V>>> sortedEntries = new LinearList<>();
    long offset = 0;
    for (IDiffMap<K, V> m : diffStack) {
      long size = m.added().size();
      long currOffset = offset;
      sortedEntries.addLast(
          Util.skipIndices(
              m.added().hashSortedEntries(),
              Iterators.map(removedIndices.slice(currOffset, currOffset + size - 1).iterator(), n -> n - currOffset)));
      offset += size;
    }

    return Iterators.mergeSort(sortedEntries, Comparator.comparing(IEntry.WithHash::keyHash));
  }
}
