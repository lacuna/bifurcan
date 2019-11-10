package io.lacuna.bifurcan;

public interface ISortedSet<V> extends ISet<V> {
  /**
   * @return the entry whose key is either equal to {@code key}, or just below it. If {@code key} is less than the
   * minimum value in the map, returns {@code null}.
   */
  V floor(V val);

  /**
   * @return the entry whose key is either equal to {@code key}, or just above it. If {@code key} is greater than the
   * maximum value in the map, returns {@code null}.
   */
  V ceil(V val);

  /**
   * @param min the inclusive minimum key value
   * @param max the inclusive maximum key value
   * @return a map representing all entries within [{@code} min, {@code} max]
   */
  ISortedSet<V> slice(V min, V max);

  ISortedSet<V> add(V value);

  ISortedSet<V> remove(V value);

  ISortedSet<V> union(ISet<V> s);

  ISortedSet<V> difference(ISet<V> s);

  ISortedSet<V> intersection(ISet<V> s);

  ISortedSet<V> forked();

  ISortedSet<V> linear();

  default V first() {
    return nth(0);
  }

  default V last() {
    return nth(size() - 1);
  }
}
