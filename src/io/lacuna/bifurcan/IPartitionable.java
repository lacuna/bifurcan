package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface IPartitionable<V> {

  IList<V> partition(int parts);

  V merge(V collection);
}
