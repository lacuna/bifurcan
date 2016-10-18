package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface IMergeable<V> {

  IList<V> split(int parts);

  V merge(V collection);
}
