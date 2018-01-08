package io.lacuna.bifurcan;

import java.util.function.BinaryOperator;

/**
 * @author ztellman
 */
public class DirectedGraph<V, E> implements IGraph<V, E> {

  private static final Set EMPTY_SET = new Set();

  private final Object editor;
  private Set<V> vertices;
  private Map<V, Map<V, E>> out;
  private Map<V, Set<V>> in;

  public DirectedGraph() {
    this(false, new Set<>(), new Map<>(), new Map<>());
  }

  private DirectedGraph(boolean linear, Set<V> vertices, Map<V, Map<V, E>> out, Map<V, Set<V>> in) {
    this.editor = linear ? new Object() : null;
    this.vertices = vertices;
    this.out = out;
    this.in = in;
  }

  @Override
  public Set<V> vertices() {
    return vertices;
  }

  @Override
  public E edge(V from, V to) {
    return out.get(from)
            .flatMap(m -> m.get(to))
            .orElseThrow(() -> new IllegalArgumentException("no such edge"));
  }

  @Override
  public ISet<V> in(V vertex) {
    ISet<V> s = in.get(vertex, null);
    if (s == null) {
      if (vertices.contains(vertex)) {
        return EMPTY_SET;
      } else {
        throw new IllegalArgumentException("no such vertex");
      }
    } else {
      return s;
    }
  }

  @Override
  public ISet<V> out(V vertex) {
    IMap<V, E> m = out.get(vertex, null);
    if (m == null) {
      if (vertices.contains(vertex)) {
        return EMPTY_SET;
      } else {
        throw new IllegalArgumentException("no such vertex");
      }
    } else {
      return m.keys();
    }
  }

  @Override
  public DirectedGraph<V, E> link(V from, V to, E edge, BinaryOperator<E> merge) {

    Object editor = isLinear() ? this.editor : new Object();

    Set<V> verticesPrime = vertices.add(from, editor).add(to, editor);
    Map<V, Map<V, E>> outPrime = out.update(from, m -> (m == null ? new Map<V, E>() : m).put(to, edge, merge, editor), editor);
    Map<V, Set<V>> inPrime = in.update(to, s -> (s == null ? new Set<V>() : s).add(from, editor), editor);

    if (isLinear()) {
      vertices = verticesPrime;
      out = outPrime;
      in = inPrime;
      return this;
    } else {
      return new DirectedGraph<>(false, verticesPrime, outPrime, inPrime);
    }
  }

  @Override
  public DirectedGraph<V, E> unlink(V from, V to) {
    if (out.get(from).map(m -> m.contains(to)).orElse(false)) {
      Object editor = isLinear() ? this : new Object();
      Map<V, Map<V, E>> outPrime = out.update(from, m -> m.remove(to, editor), editor);
      Map<V, Set<V>> inPrime = in.update(to, s -> s.remove(from, editor), editor);

      if (isLinear()) {
        out = outPrime;
        in = inPrime;
        return this;
      } else {
        return new DirectedGraph<>(false, vertices, outPrime, inPrime);
      }
    } else {
      return this;
    }
  }

  @Override
  public DirectedGraph<V, E> add(V vertex) {
    if (vertices.contains(vertex)) {
      return this;
    } else {
      Object editor = isLinear() ? this : new Object();
      Set<V> verticesPrime = vertices.add(vertex, editor);
      Map<V, Map<V, E>> outPrime = out.put(vertex, new Map<>(), (BinaryOperator<Map<V, E>>) Maps.MERGE_LAST_WRITE_WINS, editor);

      if (isLinear()) {
        vertices = verticesPrime;
        out = outPrime;
        return this;
      } else {
        return new DirectedGraph<>(false, verticesPrime, outPrime, in);
      }
    }
  }

  @Override
  public DirectedGraph<V, E> remove(V vertex) {
    if (vertices.contains(vertex)) {
      Object editor = isLinear() ? this : new Object();

      Set<V> verticesPrime = vertices.remove(vertex, editor);
      Map<V, Map<V, E>> outPrime = out.remove(vertex, editor);
      Map<V, Set<V>> inPrime = this.in.linear();
      for (V v : out.get(vertex).get().keys()) {
        in = in.update(v, s -> s.remove(vertex, editor), editor);
      }

      if (isLinear()) {
        vertices = verticesPrime;
        out = outPrime;
        in = inPrime;
        return this;
      } else {
        return new DirectedGraph<>(false, verticesPrime, outPrime, inPrime);
      }
    } else {
      return this;
    }
  }

  @Override
  public DirectedGraph<V, E> merge(IGraph<V, E> graph, BinaryOperator<E> merge) {
    if (graph instanceof DirectedGraph) {
      DirectedGraph<V, E> g = (DirectedGraph<V, E>) graph;
      return new DirectedGraph<>(
              isLinear(),
              vertices.union(g.vertices),
              out.merge(g.out, (a, b) -> a.merge(b, merge)),
              in.merge(g.in, Set::union));
    } else {
      return (DirectedGraph<V, E>) Graphs.merge(this, graph, merge);
    }
  }

  @Override
  public DirectedGraph<V, E> select(ISet<V> vertices) {
    Set<V> selected = this.vertices.intersection(vertices);
    return new DirectedGraph<>(
            isLinear(),
            selected,
            vertices.stream().collect(Maps.collector(v -> v, v -> out.get(v).map(x -> x.intersection(selected)).orElseGet(Map::new))),
            vertices.stream().collect(Maps.collector(v -> v, v -> in.get(v).map(x -> x.intersection(selected)).orElseGet(Set::new))));
  }

  @Override
  public DirectedGraph<V, E> forked() {
    return isLinear() ? new DirectedGraph<>(false, vertices, out, in) : this;
  }

  @Override
  public DirectedGraph<V, E> linear() {
    return isLinear() ? this : new DirectedGraph<>(true, vertices, out, in);
  }

  @Override
  public boolean isLinear() {
    return editor != null;
  }

  @Override
  public int hashCode() {
    return out.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DirectedGraph) {
      return ((DirectedGraph) obj).out.equals(out);
    }
    return false;
  }

  @Override
  protected Object clone() {
    return isLinear() ? forked().linear() : this;
  }

  @Override
  public String toString() {
    return out.toString();
  }
}
