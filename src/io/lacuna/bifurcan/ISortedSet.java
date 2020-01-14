package io.lacuna.bifurcan;

import java.util.Comparator;

public interface ISortedSet<V> extends ISet<V> {

  Comparator<V> comparator();

  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  V floor(V val);

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  default V ceil(V val) {
    V f = floor(val);
    if (f == null) {
      return nth(0, null);
    } else if (valueEquality().test(val, f)) {
      return f;
    } else {
      return nth(indexOf(f).getAsLong() + 1, null);
    }
  }

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within {@code [min, max]}
   */
  ISortedSet<V> slice(V min, V max);

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

  ISortedSet<V> forked();

  ISortedSet<V> linear();

  default V first() {
    return nth(0);
  }

  default V last() {
    return nth(size() - 1);
  }
}
