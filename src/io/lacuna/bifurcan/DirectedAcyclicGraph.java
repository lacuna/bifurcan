package io.lacuna.bifurcan;

import java.util.function.BinaryOperator;

/**
 * @author ztellman
 */
public class DirectedAcyclicGraph<V, E> implements IGraph<V, E> {

  private final DirectedGraph<V, E> graph;
  private final Set<V> top;
  private final Set<V> bottom;

  private DirectedAcyclicGraph(DirectedGraph<V, E> graph, Set<V> top, Set<V> bottom) {
    this.graph = graph;
    this.top = top;
    this.bottom = bottom;
  }

  public DirectedAcyclicGraph() {
    this(new DirectedGraph<>(), new Set<>(), new Set<>());
  }

  /**
   * @return a directed acyclic graph equivalent to {@code graph}
   * @throws IllegalArgumentException if {@code graph} contains a cycle
   */
  public static <V, E> DirectedAcyclicGraph<V, E> from(DirectedGraph<V, E> graph) {
    if (Graphs.stronglyConnectedComponents(graph).size() > 0) {
      throw new IllegalArgumentException("graph contains a cycle");
    }
    return new DirectedAcyclicGraph<>(
            graph,
            graph.vertices().stream().filter(v -> graph.in(v).size() == 0).collect(Sets.collector()),
            graph.vertices().stream().filter(v -> graph.out(v).size() == 0).collect(Sets.collector()));
  }

  public ISet<V> top() {
    return top;
  }

  public ISet<V> bottom() {
    return bottom;
  }

  @Override
  public ISet<V> vertices() {
    return graph.vertices();
  }

  @Override
  public E edge(V src, V dst) {
    return graph.edge(src, dst);
  }

  @Override
  public ISet<V> in(V vertex) {
    return graph.in(vertex);
  }

  @Override
  public ISet<V> out(V vertex) {
    return graph.out(vertex);
  }

  @Override
  public DirectedAcyclicGraph<V, E> link(V from, V to, E edge, BinaryOperator<E> merge) {

    // TODO: check for cycle

    return new DirectedAcyclicGraph<>(
            graph.link(from, to, edge, merge),
            top.remove(to),
            bottom.remove(from));
  }

  @Override
  public DirectedAcyclicGraph<V, E> unlink(V from, V to) {
    return new DirectedAcyclicGraph<>(
            graph.unlink(from, to),
            graph.in(to).size() == 1 ? top.add(to) : top,
            graph.out(from).size() == 1 ? bottom.add(from) : bottom);
  }

  @Override
  public DirectedAcyclicGraph<V, E> merge(IGraph<V, E> graph, BinaryOperator<E> merge) {
    return from(this.graph.merge(graph, merge));
  }

  @Override
  public DirectedAcyclicGraph<V, E> add(V vertex) {
    if (graph.vertices().contains(vertex)) {
      return this;
    } else {
      return new DirectedAcyclicGraph<>(graph.add(vertex), top.add(vertex), bottom.add(vertex));
    }
  }

  @Override
  public IGraph<V, E> remove(V vertex) {
    if (graph.vertices().contains(vertex)) {
      return new DirectedAcyclicGraph<>(
              graph.remove(vertex),
              top.union(graph.out(vertex).stream().filter(v -> graph.in(v).size() == 1).collect(Sets.collector())),
              bottom.union(graph.in(vertex).stream().filter(v -> graph.out(v).size() == 1).collect(Sets.collector())));
    } else {
      return this;
    }
  }

  @Override
  public IGraph<V, E> forked() {
    return graph.isLinear() ? new DirectedAcyclicGraph<>(graph.linear(), top.linear(), bottom.linear()) : this;
  }

  @Override
  public IGraph<V, E> linear() {
    return graph.isLinear() ? this : new DirectedAcyclicGraph<>(graph.forked(), top.forked(), bottom.linear());
  }

  @Override
  public boolean isLinear() {
    return graph.isLinear();
  }

  @Override
  public int hashCode() {
    return graph.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DirectedAcyclicGraph) {
      return graph.equals(((DirectedAcyclicGraph<V, E>) obj).graph);
    } else if (obj instanceof DirectedGraph) {
      return graph.equals(obj);
    } else {
      return false;
    }
  }
}
