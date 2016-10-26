package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface ISet<V> extends
        ILinearizable<ISet<V>>,
        IForkable<ISet<V>>,
        IPartitionable<ISet<V>> {

  /**
   * @return the set, containing {@code rowValue}
   */
  ISet<V> add(V value);

  /**
   * @return the set, without {@code rowValue}
   */
  ISet<V> remove(V value);

  /**
   * @return true, if the set contains {@code rowValue}
   */
  boolean contains(V value);

  /**
   * @return the number of elements in the set
   */
  long size();

  /**
   * @return an {@code IList} containing all the elements in the set
   */
  IList<V> elements();

  ISet<V> union(ISet<V> s);

  ISet<V> difference(ISet<V> s);

  ISet<V> intersection(ISet<V> s);

  /**
   * @return the collection, represented as a normal Java {@code Set}, without support for writes
   */
  java.util.Set<V> toSet();
}
