package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.function.*;

import static io.lacuna.bifurcan.Graphs.MERGE_LAST_WRITE_WINS;

/**
 * A directed graph which will throw a {@link DirectedAcyclicGraph.CycleException} if any new edge creates a cycle.
 *
 * @author ztellman
 */
public class DirectedAcyclicGraph<V, E> implements IGraph<V, E> {

  public static class CycleException extends IllegalArgumentException {
    public CycleException() {
      super("the graph contains a cycle");
    }
  }

  private DirectedGraph<V, E> graph;
  // derived from the edges on demand, and null whenever stale
  private Set<V> top;
  private Set<V> bottom;

  private DirectedAcyclicGraph(DirectedGraph<V, E> graph) {
    this.graph = graph;
  }

  public DirectedAcyclicGraph() {
    this(new DirectedGraph<>());
  }

  public DirectedAcyclicGraph(ToLongFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    this(new DirectedGraph<>(hashFn, equalsFn));
  }

  /**
   * @return a directed acyclic graph equivalent to {@code graph}
   * @throws CycleException if {@code graph} contains a cycle
   */
  public static <V, E> DirectedAcyclicGraph<V, E> from(DirectedGraph<V, E> graph) {
    if (Graphs.stronglyConnectedComponents(graph, false).size() > 0) {
      throw new CycleException();
    }

    return new DirectedAcyclicGraph<>(graph);
  }

  private Set<V> verticesWithNoEdges(Function<V, ISet<V>> edges) {
    Set<V> result = new Set<V>(vertexHash(), vertexEquality()).linear();
    graph.vertices().forEach(v -> {
      if (edges.apply(v).size() == 0) {
        result.add(v);
      }
    });
    return result.forked();
  }

  /**
   * @return the vertices with no inbound edges
   */
  public Set<V> top() {
    if (top == null) {
      top = verticesWithNoEdges(graph::in);
    }
    return top;
  }

  /**
   * @return the vertices with no outbound edges
   */
  public Set<V> bottom() {
    if (bottom == null) {
      bottom = verticesWithNoEdges(graph::out);
    }
    return bottom;
  }

  public DirectedGraph<V, E> directedGraph() {
    return graph.clone();
  }

  @Override
  public DirectedAcyclicGraph<V, E> add(IEdge<V, E> edge) {
    return link(edge.from(), edge.to(), edge.value());
  }

  @Override
  public DirectedAcyclicGraph<V, E> remove(IEdge<V, E> edge) {
    return unlink(edge.from(), edge.to());
  }

  @Override
  public DirectedAcyclicGraph<V, E> link(V from, V to, E edge) {
    return link(from, to, edge, (BinaryOperator<E>) MERGE_LAST_WRITE_WINS);
  }

  @Override
  public DirectedAcyclicGraph<V, E> link(V from, V to) {
    return link(from, to, null, (BinaryOperator<E>) MERGE_LAST_WRITE_WINS);
  }

  @Override
  public Set<V> vertices() {
    return graph.vertices();
  }

  @Override
  public Iterable<IEdge<V, E>> edges() {
    return graph.edges();
  }

  @Override
  public E edge(V src, V dst) {
    return graph.edge(src, dst);
  }

  @Override
  public E edge(V src, V dst, E notFound) {
    return graph.edge(src, dst, notFound);
  }

  @Override
  public Set<V> in(V vertex) {
    return graph.in(vertex);
  }

  @Override
  public Set<V> out(V vertex) {
    return graph.out(vertex);
  }

  /**
   * @param from  the source of the edge
   * @param to    the destination of the edge
   * @param edge  the value of the edge
   * @param merge the merge function for the edge values, if an edge already exists
   * @return a graph containing the new edge
   * @throws CycleException if the new edge creates a cycle
   */
  @Override
  public DirectedAcyclicGraph<V, E> link(V from, V to, E edge, BinaryOperator<E> merge) {
    boolean
        newFrom = !vertices().contains(from),
        newTo = !vertices().contains(to);

    if (!newFrom && !newTo && !out(from).contains(to) && createsCycle(from, to)) {
      throw new CycleException();
    }

    DirectedGraph<V, E> graphPrime = graph.link(from, to, edge, merge);

    if (isLinear()) {
      graph = graphPrime;
      top = bottom = null;
      return this;
    } else {
      return new DirectedAcyclicGraph<>(graphPrime);
    }
  }

  @Override
  public DirectedAcyclicGraph<V, E> unlink(V from, V to) {

    DirectedGraph<V, E> graphPrime = graph.unlink(from, to);

    if (isLinear()) {
      graph = graphPrime;
      top = bottom = null;
      return this;
    } else if (graph == graphPrime) {
      return this;
    } else {
      return new DirectedAcyclicGraph<>(graphPrime);
    }
  }

  @Override
  public DirectedAcyclicGraph<V, E> merge(IGraph<V, E> graph, BinaryOperator<E> merge) {
    return from(this.graph.merge(graph, merge));
  }

  @Override
  public DirectedAcyclicGraph<V, E> select(ISet<V> vertices) {
    return from(graph.select(vertices));
  }

  @Override
  public DirectedAcyclicGraph<V, E> add(V vertex) {
    if (graph.vertices().contains(vertex)) {
      return this;
    } else {

      DirectedGraph<V, E> graphPrime = graph.add(vertex);

      if (isLinear()) {
        graph = graphPrime;
        top = bottom = null;
        return this;
      } else {
        return new DirectedAcyclicGraph<>(graphPrime);
      }
    }
  }

  @Override
  public DirectedAcyclicGraph<V, E> remove(V vertex) {
    if (graph.vertices().contains(vertex)) {
      DirectedGraph<V, E> graphPrime = graph.remove(vertex);

      if (isLinear()) {
        graph = graphPrime;
        top = bottom = null;
        return this;
      } else {
        return new DirectedAcyclicGraph<>(graphPrime);
      }
    } else {
      return this;
    }
  }

  @Override
  public DirectedAcyclicGraph<V, E> forked() {
    return graph.isLinear() ? new DirectedAcyclicGraph<>(graph.forked()) : this;
  }

  @Override
  public DirectedAcyclicGraph<V, E> linear() {
    return graph.isLinear() ? this : new DirectedAcyclicGraph<>(graph.linear());
  }

  @Override
  public boolean isLinear() {
    return graph.isLinear();
  }

  @Override
  public boolean isDirected() {
    return true;
  }

  @Override
  public <U> DirectedAcyclicGraph<V, U> mapEdges(Function<IEdge<V, E>, U> f) {
    return new DirectedAcyclicGraph<>(graph.mapEdges(f));
  }

  @Override
  public DirectedAcyclicGraph<V, E> transpose() {
    return new DirectedAcyclicGraph<>(graph.transpose());
  }

  @Override
  public ToLongFunction<V> vertexHash() {
    return graph.vertexHash();
  }

  @Override
  public BiPredicate<V, V> vertexEquality() {
    return graph.vertexEquality();
  }

  @Override
  public int hashCode() {
    return graph.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return graph.equals(obj);
  }

  @Override
  public String toString() {
    return graph.toString();
  }

  @Override
  public DirectedAcyclicGraph<V, E> clone() {
    return new DirectedAcyclicGraph<V, E>(graph.clone());
  }

  ///

  private boolean createsCycle(V from, V to) {
    Iterator<V> upstreamIterator = Graphs.bfsVertices(LinearList.of(from), this::in).iterator();
    Iterator<V> downstreamIterator = Graphs.bfsVertices(LinearList.of(to), this::out).iterator();

    if (!upstreamIterator.hasNext() || !downstreamIterator.hasNext()) {
      return false;
    }

    LinearSet<V> upstream = new LinearSet<V>(vertexHash(), vertexEquality());
    LinearSet<V> downstream = new LinearSet<V>(vertexHash(), vertexEquality());

    while (upstreamIterator.hasNext() && downstreamIterator.hasNext()) {
      V a = upstreamIterator.next();
      if (downstream.contains(a)) {
        return true;
      }
      upstream.add(a);

      V b = downstreamIterator.next();
      if (upstream.contains(b)) {
        return true;
      }
      downstream.add(b);
    }

    return false;
  }
}
