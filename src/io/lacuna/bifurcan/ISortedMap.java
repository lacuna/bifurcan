package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.ConcatSortedMap;

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

  Comparator<K> comparator();

  OptionalLong floorIndex(K key);

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
    return Sets.from(Lists.lazyMap(this.entries(), IEntry::key), comparator(), this::floorIndex);
  }

  default OptionalLong ceilIndex(K key) {
    OptionalLong idx = floorIndex(key);
    if (!idx.isPresent()) {
      return size() > 0 ? OptionalLong.of(0) : idx;
    } else {
      long i = idx.getAsLong();
      if (comparator().compare(nth(i).key(), key) == 0) {
        return idx;
      } else if (i < size() - 1) {
        return OptionalLong.of(i + 1);
      } else {
        return OptionalLong.empty();
      }
    }
  }

  @Override
  default OptionalLong indexOf(K key) {
    OptionalLong idx = floorIndex(key);
    return idx.isPresent() && comparator().compare(key, nth(idx.getAsLong()).key()) == 0
        ? idx
        : OptionalLong.empty();
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  default IEntry<K, V> floor(K key) {
    OptionalLong idx = floorIndex(key);
    return idx.isPresent()
        ? nth(idx.getAsLong())
        : null;
  }

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  default IEntry<K, V> ceil(K key) {
    OptionalLong idx = ceilIndex(key);
    return idx.isPresent()
        ? nth(idx.getAsLong())
        : null;
  }

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within [{@code} min, {@code} max]
   */
  default ISortedMap<K, V> slice(K min, K max) {
    return Maps.from(keys().slice(min, max), this);
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
    return ConcatSortedMap.from(comparator(), this).put(key, value, merge);
  }

  default ISortedMap<K, V> update(K key, UnaryOperator<V> update) {
    return this.put(key, update.apply(this.get(key, null)));
  }

  default ISortedMap<K, V> put(K key, V value) {
    return put(key, value, Maps.MERGE_LAST_WRITE_WINS);
  }

  default ISortedMap<K, V> remove(K key) {
    return ConcatSortedMap.from(comparator(), this).remove(key);
  }

  default ISortedMap<K, V> forked() {
    return this;
  }

  default ISortedMap<K, V> linear() {
    return ConcatSortedMap.from(comparator(), this).linear();
  }

  default IEntry<K, V> first() {
    return nth(0);
  }

  default IEntry<K, V> last() {
    return nth(size() - 1);
  }
}
