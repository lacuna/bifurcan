package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.lacuna.bifurcan.Graphs.MERGE_LAST_WRITE_WINS;

/**
 * @author ztellman
 */
public interface IGraph<V, E> extends ILinearizable<IGraph<V, E>>, IForkable<IGraph<V, E>> {

  /**
   * @return the set of all vertices in the graph
   */
  ISet<V> vertices();

  /**
   * @return an iterator over every edge in the graph
   */
  Iterable<IEdge<V, E>> edges();

  /**
   * @return the value of the edge between {@code from} and {@code to}
   * @throws IllegalArgumentException if no such edge exists
   */
  E edge(V from, V to);

  /**
   * In an undirected graph, this is equivalent to {@code out()}.
   *
   * @return the set of all incoming edges to {@code vertex}
   * @throws IllegalArgumentException if no such vertex exists
   */
  ISet<V> in(V vertex);

  /**
   * In an undirected graph, this is equivalent to {@code in()}.
   *
   * @return the set of all outgoing edges from {@code vertex}
   * @throws IllegalArgumentException if no such vertex exists
   */
  ISet<V> out(V vertex);

  /**
   * @param from  the source of the edge
   * @param to    the destination of the edge
   * @param edge  the value of the edge
   * @param merge the merge function for the edge values, if an edge already exists
   * @return a graph containing the new edge
   */
  IGraph<V, E> link(V from, V to, E edge, BinaryOperator<E> merge);

  /**
   * @return a graph without any edge between {@code from} and {@code to}
   */
  IGraph<V, E> unlink(V from, V to);

  /**
   * @return a graph with {@code vertex} added
   */
  IGraph<V, E> add(V vertex);

  /**
   * @return a graph with {@code vertex} removed, as well as all incoming and outgoing edges
   */
  IGraph<V, E> remove(V vertex);

  <U> IGraph<V, U> mapEdges(Function<IEdge<V, E>, U> f);

  default long indexOf(V vertex) {
    return vertices().indexOf(vertex);
  }

  default V nth(long index) {
    return vertices().nth(index);
  }

  /**
   * @return a graph containing only the specified vertices and the edges between them
   */
  IGraph<V, E> select(ISet<V> vertices);

  /**
   * @return
   */
  default IGraph<V, E> replace(V a, V b) {
    return replace(a, b, (BinaryOperator<E>) Graphs.MERGE_LAST_WRITE_WINS);
  }

  /**
   * @param a
   * @param b
   * @param merge
   * @return
   */
  default IGraph<V, E> replace(V a, V b, BinaryOperator<E> merge) {
    if (vertexEquality().test(a, b) || !vertices().contains(a)) {
      return this;
    }

    IGraph<V, E> g = this.linear();
    for (V v : this.out(a)) {
      g = g.link(b, v, this.edge(a, v));
    }

    for (V v : this.in(a)) {
      g = g.link(v, b, this.edge(v, a), merge);
    }
    g.remove(a);

    return this.isLinear() ? this : g.forked();
  }

  /**
   * @return
   */
  boolean isLinear();

  /**
   * @return
   */
  boolean isDirected();

  /**
   * @return
   */
  ToIntFunction<V> vertexHash();

  /**
   * @return
   */
  BiPredicate<V, V> vertexEquality();

  /**
   * @return
   */
  IGraph<V, E> transpose();

  IGraph<V, E> clone();

  // polymorphic utility methods

  default IGraph<V, E> add(IEdge<V, E> edge) {
    return link(edge.from(), edge.to(), edge.value());
  }

  default IGraph<V, E> remove(IEdge<V, E> edge) {
    return unlink(edge.from(), edge.to());
  }

  default IGraph<V, E> link(V from, V to, E edge) {
    return link(from, to, edge, (BinaryOperator<E>) MERGE_LAST_WRITE_WINS);
  }

  default IGraph<V, E> link(V from, V to) {
    return link(from, to, null, (BinaryOperator<E>) MERGE_LAST_WRITE_WINS);
  }

  default IGraph<V, E> merge(IGraph<V, E> graph, BinaryOperator<E> merge) {
    return Graphs.merge(this, graph, merge);
  }

  default IGraph<V, E> merge(IGraph<V, E> graph) {
    return merge(graph, (BinaryOperator<E>) MERGE_LAST_WRITE_WINS);
  }

}
