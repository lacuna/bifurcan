package io.lacuna.bifurcan;

import java.util.Comparator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public interface ISortedSet<V> extends ISet<V> {

  enum Bound {
    INCLUSIVE,
    EXCLUSIVE
  }

  abstract class Mixin<V> extends ISet.Mixin<V> implements ISortedSet<V> {
    @Override
    public ISortedSet<V> clone() {
      return this;
    }
  }

  Comparator<V> comparator();

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  OptionalLong inclusiveFloorIndex(V val);

  @Override
  default ToLongFunction<V> valueHash() {
    throw new UnsupportedOperationException("ISortedSet does not use hashes");
  }

  @Override
  default BiPredicate<V, V> valueEquality() {
    return (a, b) -> comparator().compare(a, b) == 0;
  }

  default OptionalLong floorIndex(V val, Bound bound) {
    OptionalLong oIdx = inclusiveFloorIndex(val);
    if (bound == Bound.INCLUSIVE) {
      return oIdx;
    } else {
      if (oIdx.isPresent()) {
        long idx = oIdx.getAsLong();
        if (comparator().compare(nth(idx), val) == 0) {
          return idx == 0 ? OptionalLong.empty() : OptionalLong.of(idx - 1);
        }
      }
    }
    return oIdx;
  }

  default OptionalLong ceilIndex(V val, Bound bound) {
    OptionalLong oIdx = inclusiveFloorIndex(val);
    if (oIdx.isPresent()) {
      long idx = oIdx.getAsLong();
      if (bound == Bound.INCLUSIVE && comparator().compare(nth(idx), val) == 0) {
        return oIdx;
      } else if (idx == size() - 1) {
        return OptionalLong.empty();
      } else {
        return OptionalLong.of(idx + 1);
      }
    }
    return size() == 0 ? OptionalLong.empty() : OptionalLong.of(0);
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  default OptionalLong ceilIndex(V val) {
    return ceilIndex(val, Bound.INCLUSIVE);
  }

  default OptionalLong floorIndex(V val) {
    return floorIndex(val, Bound.INCLUSIVE);
  }

  @Override
  default OptionalLong indexOf(V element) {
    OptionalLong idx = floorIndex(element);
    return idx.isPresent() && comparator().compare(nth(idx.getAsLong()), element) == 0
        ? idx
        : OptionalLong.empty();
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  default V floor(V val) {
    OptionalLong idx = floorIndex(val);
    return idx.isPresent()
        ? nth(idx.getAsLong())
        : null;
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  default V ceil(V val) {
    OptionalLong idx = ceilIndex(val);
    return idx.isPresent()
        ? nth(idx.getAsLong())
        : null;
  }

  @Override
  default <U> ISortedMap<V, U> zip(Function<V, U> f) {
    return Maps.from(this, f);
  }

  ISortedSet<V> add(V value);

  ISortedSet<V> remove(V value);

  default ISortedSet<V> union(ISet<V> s) {
    return (ISortedSet<V>) Sets.union(this, s);
  }

  default ISortedSet<V> difference(ISet<V> s) {
    return (ISortedSet<V>) Sets.difference(this, s);
  }

  default ISortedSet<V> intersection(ISet<V> s) {
    return (ISortedSet<V>) Sets.intersection(new SortedSet<>(), this, s);
  }

  default ISortedSet<V> forked() {
    return this;
  }

  ISortedSet<V> linear();

  default V first() {
    return nth(0);
  }

  default V last() {
    return nth(size() - 1);
  }
}
