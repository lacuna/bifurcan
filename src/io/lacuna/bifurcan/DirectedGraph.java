package io.lacuna.bifurcan;

import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;

/**
 * @author ztellman
 */
public class DirectedGraph<V, E> implements IGraph<V, E> {

  private static final Set EMPTY_SET = new Set();

  private final Object editor;
  private Map<V, Map<V, E>> out;
  private Map<V, Set<V>> in;

  public DirectedGraph() {
    this(false, new Map<>(), new Map<>());
  }

  public DirectedGraph(ToIntFunction<V> hashFn, BiPredicate<V, V> equalsFn) {
    this(false, new Map<>(hashFn, equalsFn), new Map<>(hashFn, equalsFn));
  }

  private DirectedGraph(boolean linear, Map<V, Map<V, E>> out, Map<V, Set<V>> in) {
    this.editor = linear ? new Object() : null;
    this.out = out;
    this.in = in;
  }

  @Override
  public Set<V> vertices() {
    return out.keys();
  }

  @Override
  public E edge(V from, V to) {
    return out.get(from)
            .flatMap(m -> m.get(to))
            .orElseThrow(() -> new IllegalArgumentException("no such edge"));
  }

  @Override
  public Set<V> in(V vertex) {
    Set<V> s = in.get(vertex, null);
    if (s == null) {
      if (out.contains(vertex)) {
        return EMPTY_SET;
      } else {
        throw new IllegalArgumentException("no such vertex");
      }
    } else {
      return s;
    }
  }

  @Override
  public Set<V> out(V vertex) {
    return out.get(vertex).map(Map::keys).orElseThrow(() -> new IllegalArgumentException("no such vertex"));
  }

  @Override
  public DirectedGraph<V, E> link(V from, V to, E edge, BinaryOperator<E> merge) {

    Object editor = isLinear() ? this.editor : new Object();

    Map<V, Map<V, E>> outPrime = out.update(from, m -> {
      if (m == null) {
        m = new Map<>(out.keyHash(), out.keyEquality());
      }
      return m.put(to, edge, merge, editor);
    }, editor);

    outPrime = outPrime.update(to, m -> {
      if (m == null) {
        m = new Map<>(out.keyHash(), out.keyEquality());
      }
      return m;
    }, editor);

    Map<V, Set<V>> inPrime = in.update(to, s -> {
      if (s == null) {
         s = new Set<>(out.keyHash(), out.keyEquality());
      }
      return s.add(from, editor);
    }, editor);

    if (isLinear()) {
      out = outPrime;
      in = inPrime;
      return this;
    } else {
      return new DirectedGraph<>(false, outPrime, inPrime);
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
        return new DirectedGraph<>(false, outPrime, inPrime);
      }
    } else {
      return this;
    }
  }

  @Override
  public DirectedGraph<V, E> add(V vertex) {
    if (out.contains(vertex)) {
      return this;
    } else {
      Object editor = isLinear() ? this : new Object();
      Map<V, Map<V, E>> outPrime = out.put(vertex, new Map<>(), (BinaryOperator<Map<V, E>>) Maps.MERGE_LAST_WRITE_WINS, editor);

      if (isLinear()) {
        out = outPrime;
        return this;
      } else {
        return new DirectedGraph<>(false, outPrime, in);
      }
    }
  }

  @Override
  public DirectedGraph<V, E> remove(V vertex) {
    if (out.contains(vertex)) {
      Object editor = isLinear() ? this : new Object();

      Map<V, Map<V, E>> outPrime = out.remove(vertex, editor);

      Map<V, Set<V>> inPrime = this.in.linear();
      for (V v : out.get(vertex).get().keys()) {
        inPrime.update(v, s -> s.remove(vertex, editor), editor);
      }
      inPrime = inPrime.forked();

      if (isLinear()) {
        out = outPrime;
        in = inPrime;
        return this;
      } else {
        return new DirectedGraph<>(false, outPrime, inPrime);
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
              out.merge(g.out, (a, b) -> a.merge(b, merge)),
              in.merge(g.in, Set::union));
    } else {
      return (DirectedGraph<V, E>) Graphs.merge(this, graph, merge);
    }
  }

  @Override
  public DirectedGraph<V, E> select(ISet<V> vertices) {
    return new DirectedGraph<V, E>(
            isLinear(),
            out.intersection(vertices).mapVals(m -> m.intersection(vertices)),
            in.intersection(vertices).mapVals(s -> s.intersection(vertices)));
  }

  @Override
  public DirectedGraph<V, E> forked() {
    return isLinear() ? new DirectedGraph<>(false, out, in) : this;
  }

  @Override
  public DirectedGraph<V, E> linear() {
    return isLinear() ? this : new DirectedGraph<>(true, out, in);
  }

  @Override
  public boolean isLinear() {
    return editor != null;
  }

  @Override
  public boolean isDirected() {
    return true;
  }

  @Override
  public ToIntFunction<V> vertexHash() {
    return out.keyHash();
  }

  @Override
  public BiPredicate<V, V> vertexEquality() {
    return out.keyEquality();
  }

  @Override
  public int hashCode() {
    return out.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DirectedGraph) {
      return ((DirectedGraph) obj).out.equals(out);
    } else if (obj instanceof IGraph) {
      return Graphs.equals(this, (IGraph<V, E>) obj);
    } else {
      return false;
    }
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
