package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.DiffSortedSet;

import java.util.Comparator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;

public interface ISortedSet<V> extends ISet<V> {

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
  OptionalLong floorIndex(V val);

  @Override
  default ToLongFunction<V> valueHash() {
    throw new UnsupportedOperationException("ISortedSet does not use hashes");
  }

  @Override
  default BiPredicate<V, V> valueEquality() {
    return (a, b) -> comparator().compare(a, b) == 0;
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  default OptionalLong ceilIndex(V val) {
    OptionalLong idx = floorIndex(val);
    if (!idx.isPresent()) {
      return size() > 0 ? OptionalLong.of(0) : idx;
    } else {
      long i = idx.getAsLong();
      if (comparator().compare(nth(i), val) == 0) {
        return idx;
      } else if (i < size() - 1) {
        return OptionalLong.of(i + 1);
      } else {
        return OptionalLong.empty();
      }
    }
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

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within {@code [min, max]}
   */
  default ISortedSet<V> slice(V min, V max) {
    OptionalLong oMinIdx = ceilIndex(min);
    OptionalLong oMaxIdx = floorIndex(max);
    if (!oMinIdx.isPresent() || !oMaxIdx.isPresent()) {
      return Sets.from(List.EMPTY, comparator(), k -> OptionalLong.empty());
    } else {
      long minIdx = oMinIdx.getAsLong();
      long maxIdx = oMaxIdx.getAsLong();
      return Sets.from(elements().slice(minIdx, maxIdx + 1), comparator(), v -> {
        OptionalLong oIdx = floorIndex(v);
        if (oIdx.isPresent()) {
          long idx = oIdx.getAsLong();
          return idx >= minIdx && idx <= maxIdx ? OptionalLong.of(idx - minIdx) : OptionalLong.empty();
        } else {
          return OptionalLong.empty();
        }
      });
    }
  }

  default ISortedSet<V> add(V value) {
    return new DiffSortedSet<V>(this).add(value);
  }

  default ISortedSet<V> remove(V value) {
    return new DiffSortedSet<V>(this).remove(value);
  }

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

  default ISortedSet<V> linear() {
    return new DiffSortedSet<V>(this).linear();
  }

  default V first() {
    return nth(0);
  }

  default V last() {
    return nth(size() - 1);
  }
}
