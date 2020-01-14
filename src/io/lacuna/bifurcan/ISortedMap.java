package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IMap;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * @author ztellman
 */
public interface ISortedMap<K, V> extends IMap<K, V> {

  Comparator<K> comparator();

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  IEntry<K, V> floor(K key);

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  default IEntry<K, V> ceil(K key) {
    IEntry<K, V> f = floor(key);
    if (f == null) {
      return entries().nth(0, null);
    } else if (keyEquality().test(key, f.key())) {
      return f;
    } else {
      return nth(indexOf(f.key()).getAsLong() + 1, null);
    }
  }

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within [{@code} min, {@code} max]
   */
  ISortedMap<K, V> slice(K min, K max);

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

  ISortedMap<K, V> put(K key, V value, BinaryOperator<V> merge);

  ISortedMap<K, V> update(K key, UnaryOperator<V> update);

  ISortedMap<K, V> put(K key, V value);

  ISortedMap<K, V> remove(K key);

  ISortedMap<K, V> forked();

  ISortedMap<K, V> linear();

  default IEntry<K, V> first() {
    return nth(0);
  }

  default IEntry<K, V> last() {
    return nth(size() - 1);
  }
}
