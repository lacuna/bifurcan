package io.lacuna.bifurcan;

import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class Graph<V, E> implements IGraph<V, E> {

  private static class VertexSet<V> {
    private final V v, w;
    private int hash = -1;

    VertexSet(V v, V w) {
      this.v = v;
      this.w = w;
    }

    int hashCode(ToIntFunction<V> hashFn) {
      if (hash == -1) {
        hash = hashFn.applyAsInt(v) ^ hashFn.applyAsInt(w);
      }
      return hash;
    }

    boolean equals(BiPredicate<V, V> equalsFn, VertexSet<V> t) {
      if (equalsFn.test(v, t.v)) {
        return equalsFn.test(w, t.w);
      } else {
        return equalsFn.test(v, t.w) && equalsFn.test(w, t.v);
      }
    }
  }

  private final Object editor;
  private Map<V, Set<V>> adjacent;
  private Map<VertexSet<V>, E> edges;

  public Graph() {
    this(Objects::hash, Objects::equals);
  }

  public Graph(ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    this(false, new Map<>(hashFn, equalsFn), new Map<>(t -> t.hashCode(hashFn), (a, b) -> a.equals(equalsFn, b)));
  }

  private Graph(boolean linear, Map<V, Set<V>> adjacent, Map<VertexSet<V>, E> edges) {
    this.editor = linear ? new Object() : null;
    this.adjacent = adjacent;
    this.edges = edges;
  }

  @Override
  public Set<V> vertices() {
    return adjacent.keys();
  }

  @Override
  public E edge(V from, V to) {
    return edges.get(new VertexSet<>(from, to)).orElseThrow(() -> new IllegalArgumentException("no such edge"));
  }

  @Override
  public ISet<V> in(V vertex) {
    return out(vertex);
  }

  @Override
  public ISet<V> out(V vertex) {
    return adjacent.get(vertex).orElseThrow(() -> new IllegalArgumentException("no such vertex"));
  }

  @Override
  public IGraph<V, E> link(V from, V to, E edge, BinaryOperator<E> merge) {
    Object editor = isLinear() ? this.editor : new Object();

    Map<V, Set<V>> adjacentPrime = adjacent
            .update(from, s -> (s == null ? new Set<V>() : s).add(to, editor), editor)
            .update(to, s -> (s == null ? new Set<V>() : s).add(from, editor), editor);

    Map<VertexSet<V>, E> edgesPrime = edges.put(new VertexSet<>(from, to), edge, merge, editor);

    if (isLinear()) {
      adjacent = adjacentPrime;
      edges = edgesPrime;
      return this;
    } else {
      return new Graph<>(false, adjacentPrime, edgesPrime);
    }
  }

  @Override
  public IGraph<V, E> unlink(V from, V to) {
    VertexSet<V> t = new VertexSet<>(from, to);

    if (!edges.contains(t)) {
      return this;
    }

    Object editor = isLinear() ? this.editor : new Object();

    Map<VertexSet<V>, E> edgesPrime = edges.remove(t, editor);

    if (isLinear()) {
      edges = edgesPrime;
      return this;
    } else {
      return new Graph<>(false, adjacent, edgesPrime);
    }
  }

  @Override
  public IGraph<V, E> add(V vertex) {
    if (adjacent.contains(vertex)) {
      return this;
    }

    Object editor = isLinear() ? this.editor : new Object();

    Map<V, Set<V>> adjacentPrime = adjacent.put(vertex, new Set<>(), (BinaryOperator<Set<V>>) Graphs.MERGE_LAST_WRITE_WINS, editor);

    if (isLinear()) {
      adjacent = adjacentPrime;
      return this;
    } else {
      return new Graph<>(false, adjacentPrime, edges);
    }
  }

  @Override
  public IGraph<V, E> remove(V vertex) {
    if (!adjacent.contains(vertex)) {
      return this;
    }

    Object editor = isLinear() ? this.editor : new Object();

    Map<V, Set<V>> adjacentPrime = adjacent.linear();
    Map<VertexSet<V>, E> edgesPrime = edges.linear();

    for (V w : adjacent.get(vertex).get()) {
      adjacentPrime.update(w, s -> s.remove(vertex, editor));
      edgesPrime.remove(new VertexSet<>(w, vertex), editor);
    }

    edgesPrime = edgesPrime.forked();
    adjacentPrime = adjacentPrime.remove(vertex).forked();

    if (isLinear()) {
      adjacent = adjacentPrime;
      edges = edgesPrime;
      return this;
    } else {
      return new Graph<>(false, adjacentPrime, edgesPrime);
    }
  }

  @Override
  public boolean isLinear() {
    return editor != null;
  }

  @Override
  public boolean isDirected() {
    return false;
  }

  @Override
  public ToIntFunction<V> vertexHash() {
    return adjacent.keyHash();
  }

  @Override
  public BiPredicate<V, V> vertexEquality() {
    return adjacent.keyEquality();
  }

  @Override
  public IGraph<V, E> merge(IGraph<V, E> graph, BinaryOperator<E> merge) {
    if (graph instanceof Graph) {
      Graph<V, E> g = (Graph<V, E>) graph;
      return new Graph<>(
              isLinear(),
              adjacent.merge(g.adjacent, Set::union),
              edges.merge(g.edges, merge));
    } else {
      return Graphs.merge(this, graph, merge);
    }
  }

  @Override
  public IGraph<V, E> forked() {
    return isLinear() ? new Graph<>(false, adjacent, edges) : this;
  }

  @Override
  public IGraph<V, E> linear() {
    return isLinear() ? this : new Graph<>(true, adjacent, edges);
  }

  @Override
  public int hashCode() {
    return adjacent.keys().hashCode() ^ edges.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Graph) {
      Graph<V, E> g = (Graph<V, E>) obj;
      return edges.equals(g.edges) && vertices().equals(g.vertices());
    } else if (obj instanceof IGraph) {
      return Graphs.equals(this, (IGraph<V, E>) obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return adjacent.toString();
  }
}
