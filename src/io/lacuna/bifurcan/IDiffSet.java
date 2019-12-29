package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.Util;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public interface IDiffSet<V> extends ISet<V>, IDiff<ISet<V>, V> {

  /**
   * The baseline data structure.
   */
  ISet<V> underlying();

  /**
   * Entries which have been added to the underlying data structure
   */
  ISet<V> added();

  /**
   * Indices which have been removed from the underlying data structure.
   */
  ISortedSet<Long> removedIndices();

  @Override
  default ToIntFunction<V> valueHash() {
    return underlying().valueHash();
  }

  @Override
  default BiPredicate<V, V> valueEquality() {
    return underlying().valueEquality();
  }

  @Override
  default OptionalLong indexOf(V element) {
    OptionalLong addedIdx = added().indexOf(element);
    if (addedIdx.isPresent()) {
      return OptionalLong.of(underlying().size() - removedIndices().size() + addedIdx.getAsLong());
    }

    OptionalLong underlyingIdx = underlying().indexOf(element);
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
  default V nth(long index) {
    long underlyingSize = underlying().size() - removedIndices().size();
    if (index < underlyingSize) {
      return underlying().nth(Util.offsetIndex(removedIndices(), index));
    } else {
      return added().nth(index - underlyingSize);
    }
  }

  @Override
  default IList<V> elements() {
    return Lists.from(size(), this::nth, () ->
        Iterators.concat(
            Util.skipIndices(underlying().elements().iterator(), removedIndices().iterator()),
            added().elements().iterator()));
  }

  @Override
  default ISet<V> clone() {
    return this;
  }
}
