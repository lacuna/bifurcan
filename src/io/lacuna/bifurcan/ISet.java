package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * @author ztellman
 */
public interface ISet<V> extends
        IReadSet<V>,
        ILinearizable<ISet<V>>,
        IForkable<ISet<V>> {

  /**
   * @return the set, containing {@code rowValue}
   */
  ISet<V> add(V value);

  /**
   * @return the set, without {@code rowValue}
   */
  ISet<V> remove(V value);

  ISet<V> union(ISet<V> s);

  ISet<V> difference(ISet<V> s);

  ISet<V> intersection(ISet<V> s);
}
