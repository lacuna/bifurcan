package io.lacuna.bifurcan;

import io.lacuna.bifurcan.ISortedSet.Bound;
import io.lacuna.bifurcan.diffs.ConcatSortedMap;
import io.lacuna.bifurcan.diffs.Slice;

import java.util.Comparator;
import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

/**
 * @author ztellman
 */
public interface ISortedMap<K, V> extends IMap<K, V> {

  abstract class Mixin<K, V> extends IMap.Mixin<K, V> implements ISortedMap<K, V> {
    @Override
    public ISortedMap<K, V> clone() {
      return this;
    }
  }

  Comparator<K> comparator();

  OptionalLong inclusiveFloorIndex(K key);

  default OptionalLong floorIndex(K key) {
    return floorIndex(key, Bound.INCLUSIVE);
  }

  default OptionalLong floorIndex(K key, Bound bound) {
    return keys().floorIndex(key, bound);
  }

  default OptionalLong ceilIndex(K key) {
    return ceilIndex(key, Bound.INCLUSIVE);
  }

  default OptionalLong ceilIndex(K key, Bound bound) {
    return keys().ceilIndex(key, bound);
  }

  @Override
  default ToLongFunction<K> keyHash() {
    throw new UnsupportedOperationException("ISortedMap does not use hashes");
  }

  @Override
  default BiPredicate<K, K> keyEquality() {
    return (a, b) -> comparator().compare(a, b) == 0;
  }

  @Override
  default ISortedSet<K> keys() {
    return Sets.from(Lists.lazyMap(this.entries(), IEntry::key), comparator(), this::inclusiveFloorIndex);
  }

  @Override
  default OptionalLong indexOf(K key) {
    OptionalLong idx = inclusiveFloorIndex(key);
    return idx.isPresent() && comparator().compare(key, nth(idx.getAsLong()).key()) == 0
        ? idx
        : OptionalLong.empty();
  }

  default IEntry<K, V> floor(K key) {
    return floor(key, Bound.INCLUSIVE);
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  default IEntry<K, V> floor(K key, Bound bound) {
    OptionalLong idx = floorIndex(key, bound);
    return idx.isPresent()
        ? nth(idx.getAsLong())
        : null;
  }

  default IEntry<K, V> ceil(K key) {
    return ceil(key, Bound.INCLUSIVE);
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  default IEntry<K, V> ceil(K key, Bound bound) {
    OptionalLong idx = ceilIndex(key, bound);
    return idx.isPresent()
        ? nth(idx.getAsLong())
        : null;
  }

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within [{@code} min, {@code} max]
   */
  default IDiffSortedMap<K, V> slice(K min, K max) {
    return slice(min, Bound.INCLUSIVE, max, Bound.INCLUSIVE);
  }

  default IDiffSortedMap<K, V> slice(K min, Bound minBound, K max, Bound maxBound) {
    return new Slice.SortedMap<>(this, min, minBound, max, maxBound);
  }

  default ISortedMap<K, V> sliceIndices(long startIndex, long endIndex) {
    return keys().sliceIndices(startIndex, endIndex).zip(this);
  }

  default ISortedMap<K, V> merge(IMap<K, V> b, BinaryOperator<V> mergeFn) {
    ISortedMap<K, V> result = forked().linear();
    b.forEach(e -> result.put(e.key(), e.value(), mergeFn));
    return isLinear() ? result : result.forked();
  }

  default ISortedMap<K, V> difference(ISet<K> keys) {
    ISortedMap<K, V> result = forked().linear();
    keys.forEach(result::remove);
    return isLinear() ? result : result.forked();
  }

  default ISortedMap<K, V> intersection(ISet<K> keys) {
    SortedMap<K, V> result = (SortedMap<K, V>) Maps.intersection(new SortedMap<K, V>().linear(), this, keys);
    return isLinear() ? result : result.forked();
  }

  default ISortedMap<K, V> union(IMap<K, V> m) {
    return merge(m, Maps.MERGE_LAST_WRITE_WINS);
  }

  default ISortedMap<K, V> difference(IMap<K, ?> m) {
    return difference(m.keys());
  }

  default ISortedMap<K, V> put(K key, V value, BinaryOperator<V> merge) {
    return diffSorted().put(key, value, merge);
  }

  default ISortedMap<K, V> update(K key, UnaryOperator<V> update) {
    return this.put(key, update.apply(this.get(key, null)));
  }

  default ISortedMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  default ISortedMap<K, V> remove(K key) {
    return diffSorted().remove(key);
  }

  default ISortedMap<K, V> forked() {
    return this;
  }

  default ISortedMap<K, V> linear() {
    return diffSorted().linear();
  }

  /**
   * @return a diff wrapper around this collection which preserves the sorting
   */
  default IDiffSortedMap<K, V> diffSorted() {
    ConcatSortedMap<K, V> result = ConcatSortedMap.from(this);
    return isLinear() ? result.linear() : result;
  }

  default IEntry<K, V> first() {
    return nth(0);
  }

  default IEntry<K, V> last() {
    return nth(size() - 1);
  }
}
