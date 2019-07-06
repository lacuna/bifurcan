package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface IEdge<V, E> {

  V from();

  V to();

  E value();

  boolean isDirected();
}
