package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IGraph.IEdge;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static java.lang.Math.min;

/**
 * @author ztellman
 */
public class Graphs {

  static final BinaryOperator MERGE_LAST_WRITE_WINS = (a, b) -> b;

  public static class Edge<V, E> implements IEdge<V, E> {
    public final E value;
    public final V from, to;
    private int hash = -1;

    public Edge(E value, V from, V to) {
      this.value = value;
      this.from = from;
      this.to = to;
    }

    public static <V, E> Edge<V, E> create(IGraph<V, E> graph, V from, V to) {
      return new Edge<>(graph.edge(from, to), from, to);
    }

    @Override
    public V from() {
      return from;
    }

    @Override
    public V to() {
      return to;
    }

    @Override
    public E value() {
      return value;
    }

    @Override
    public int hashCode() {
      if (hash == -1) {
        hash = from.hashCode() ^ to.hashCode() ^ value.hashCode();
      }
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj instanceof Edge) {
        Edge<V, E> e = (Edge<V, E>) obj;
        return Objects.equals(from, e.from) && Objects.equals(to, e.to) && Objects.equals(value, e.value);
      }
      return false;
    }
  }

  /// strongly connected components

  private static class TarjanState {
    final int index;
    int lowlink;
    boolean onStack;

    public TarjanState(int index) {
      this.index = index;
      this.lowlink = index;
      this.onStack = true;
    }
  }

  /**
   * An implementation of Tarjan's strongly connected components algorithm, which only returns sets of vertices which
   * contain more than one vertex.  If an empty set is returned, then no cycles exist in {@code graph}.
   *
   * @return all strongly connected components in the graph containing more than one vertex
   */
  public static <V> ISet<ISet<V>> stronglyConnectedComponents(IGraph<V, ?> graph) {

    // required state for algorithm
    IMap<V, TarjanState> state = new LinearMap<>();
    LinearList<V> stack = new LinearList<>();
    int idx = 0;

    // call-stack state to avoid recursive calls
    LinearList<V> path = new LinearList<>();
    LinearList<Iterator<V>> branches = new LinearList<>();

    ISet<ISet<V>> result = new Set<ISet<V>>().linear();

    for (V seed : graph.vertices()) {

      if (state.contains(seed)) {
        continue;
      }

      branches.addLast(LinearList.of(seed).iterator());

      do {

        // traverse deeper
        if (branches.last().hasNext()) {
          V w = branches.last().next();
          TarjanState ws = state.get(w, null);
          if (ws == null) {
            ws = new TarjanState(idx++);
            state.put(w, ws);
            ISet<V> out = graph.out(w);
            if (out.size() > 0) {
              stack.addLast(w);
              path.addLast(w);
              branches.addLast(graph.out(w).iterator());
            }
          } else if (ws.onStack) {
            TarjanState vs = state.get(path.last()).get();
            vs.lowlink = min(vs.lowlink, ws.index);
          }

          // return
        } else {
          branches.popLast();
          V w = path.popLast();
          TarjanState ws = state.get(w).get();

          // update predecessor's lowlink, if they exist
          if (path.size() > 0) {
            V v = path.last();
            TarjanState vs = state.get(v).get();
            vs.lowlink = min(vs.lowlink, ws.lowlink);
          }

          // create a new group, if it's larger than one vertex
          if (ws.lowlink == ws.index) {
            if (stack.last() == w) {
              stack.popLast();
              state.get(w).get().onStack = false;
            } else {
              ISet<V> group = new Set<V>().linear();
              for (; ; ) {
                V x = stack.popLast();
                group.add(x);
                state.get(x).get().onStack = false;
                if (x == w) {
                  break;
                }
              }
              result.add(group.forked());
            }
          }
        }
      } while (path.size() > 0);
    }

    return result.forked();
  }

  public static <V, E> IGraph<V, E> merge(IGraph<V, E> a, IGraph<V, E> b, BinaryOperator<E> merge) {
    if (a.vertices().size() < b.vertices().size()) {
      return merge(b, a, (x, y) -> merge.apply(y, x));
    }

    IGraph<V, E> result = a.forked().linear();
    for (V src : b.vertices()) {
      for (V dst : b.out(src)) {
        a = a.link(src, dst, b.edge(src, dst), merge);
      }
    }
    return result.forked();
  }

  /// traversal

  public static <V> Iterator<V> bfsVertices(Iterable<V> start, Function<V, Iterable<V>> adjacent) {
    LinearList<V> queue = new LinearList<>();
    ISet<V> traversed = new LinearSet<>();

    start.forEach(queue::addLast);

    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return queue.size() > 0;
      }

      @Override
      public V next() {
        V v = queue.popFirst();
        traversed.add(v);

        adjacent.apply(v).forEach(w -> {
          if (!traversed.contains(w)) {
            queue.addLast(w);
          }
        });

        return v;
      }
    };
  }

  public static <V, E> Iterator<IEdge<V, E>> bfsEdges(Iterable<V> start, IGraph<V, E> graph, Function<V, Iterable<V>> adjacent) {
    LinearList<Edge<V, E>> queue = new LinearList<>();
    ISet<V> traversed = new LinearSet<V>();

    for (V v : start) {
      traversed.add(v);
      for (V w : adjacent.apply(v)) {
        queue.addLast(Edge.create(graph, v, w));
      }
    }

    return new Iterator<IEdge<V, E>>() {
      @Override
      public boolean hasNext() {
        return queue.size() > 0;
      }

      @Override
      public Edge<V, E> next() {
        Edge<V, E> e = queue.popFirst();
        traversed.add(e.to);

        adjacent.apply(e.to).forEach(w -> {
          if (!traversed.contains(w)) {
            queue.addLast(Edge.create(graph, e.to, w));
          }
        });

        return e;
      }
    };
  }


}
