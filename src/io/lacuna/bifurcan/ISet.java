package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface ISet<V> {

  ISet<V> add(V value);

  ISet<V> remove(V value);

  boolean contains(V value);

  long size();

  IList<V> elements();
}
