package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface IEdge<V, E> {

  /**
   * @return the source vertex
   */
  V from();

  /**
   * @return the destination vertex
   */
  V to();

  /**
   * @return the value associated with the edge
   */
  E value();

  /**
   * @return true if the underlying graph is directed, otherwise false
   */
  boolean isDirected();
}
