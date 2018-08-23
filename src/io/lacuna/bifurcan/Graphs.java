package io.lacuna.bifurcan;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

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

  /// utilities

  public static <V, E> boolean equals(IGraph<V, E> a, IGraph<V, E> b) {
    if (a.isDirected() != b.isDirected() || !a.vertices().equals(b.vertices())) {
      return false;
    }

    for (V v : a.vertices()) {
      ISet<V> aOut = a.out(v);
      ISet<V> bOut = b.out(v);

      if (!aOut.equals(bOut)) {
        return false;
      }

      for (V w : aOut) {
        if (!Objects.equals(a.edge(v, w), b.edge(v, w))) {
          return false;
        }
      }
    }

    return true;
  }

  public static <V, E> int hash(IGraph<V, E> g) {
    int hash = g.vertices().stream().mapToInt(Objects::hashCode).reduce(0, (a, b) -> a ^ b);
    if (g.isDirected()) {
      for (V v : g.vertices()) {
        for (V w : g.out(v)) {
          hash = hash ^ (Objects.hashCode(w) * 31) ^ Objects.hashCode(g.edge(v, w));
        }
      }
    } else {
      for (V v : g.vertices()) {
        for (V w : g.out(v)) {
          hash = hash ^ Objects.hashCode(v) ^ Objects.hashCode(w) ^ Objects.hashCode(g.edge(v, w));
        }
      }
    }
    return hash;
  }

  public static <V, E> IGraph<V, E> merge(IGraph<V, E> a, IGraph<V, E> b, BinaryOperator<E> merge) {

    if (a.isDirected() != b.isDirected()) {
      throw new IllegalArgumentException("cannot merge directed and undirected graphs");
    }

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

  /// undirected graphs

  public static <V> Set<Set<V>> connectedComponents(IGraph<V, ?> graph) {
    if (graph.isDirected()) {
      throw new IllegalArgumentException("graph must be undirected, try Graphs.stronglyConnectedComponents instead");
    }

    LinearSet<V> traversed = new LinearSet<>((int) graph.vertices().size(), graph.vertexHash(), graph.vertexEquality());
    Set<Set<V>> result = new Set<Set<V>>().linear();

    for (V seed : graph.vertices()) {
      if (!traversed.contains(seed)) {
        traversed.add(seed);
        Set<V> group = new Set<>(graph.vertexHash(), graph.vertexEquality()).linear();
        bfsVertices(LinearList.of(seed), graph::out).forEachRemaining(group::add);
        result.add(group.forked());
      }
    }

    return result.forked();
  }

  public static <V> Set<Set<V>> biconnectedComponents(IGraph<V, ?> graph) {
    Set<V> cuts = articulationPoints(graph);

    Set<Set<V>> result = new Set<Set<V>>().linear();

    for (Set<V> component : connectedComponents(graph.select(graph.vertices().difference(cuts)))) {
      result.add(
        component.union(
          cuts.stream()
            .filter(v -> graph.out(v).containsAny(component))
            .collect(Sets.collector())));
    }

    for (int i = 0; i < cuts.size() - 1; i++) {
      for (int j = i + 1; j < cuts.size(); j++) {
        V a = cuts.nth(i);
        V b = cuts.nth(i + 1);
        if (graph.out(a).contains(b)) {
          result.add(Set.of(a, b));
        }
      }
    }

    return result.forked();
  }

  private static class ArticulationPointState<V> {
    final V node;
    final int depth;
    int lowlink;
    int childCount = 0;

    public ArticulationPointState(V node, int depth) {
      this.node = node;
      this.depth = depth;
      this.lowlink = depth;
    }
  }

  public static <V> Set<V> articulationPoints(IGraph<V, ?> graph) {
    if (graph.isDirected()) {
      throw new IllegalArgumentException("graph must be undirected");
    }

    // algorithmic state
    IMap<V, ArticulationPointState<V>> state = new LinearMap<>(
      (int) graph.vertices().size(),
      graph.vertexHash(),
      graph.vertexEquality());

    // call-stack state
    LinearList<ArticulationPointState<V>> path = new LinearList<>();
    LinearList<Iterator<V>> branches = new LinearList<>();

    Set<V> result = new Set<V>().linear();

    for (V seed : graph.vertices()) {

      if (state.contains(seed)) {
        continue;
      }

      ArticulationPointState<V> s = new ArticulationPointState<>(seed, 0);
      path.addLast(s);
      branches.addLast(graph.out(seed).iterator());
      state.put(seed, s);

      while (path.size() > 0) {

        // traverse deeper
        if (branches.last().hasNext()) {

          V w = branches.last().next();
          ArticulationPointState<V> vs = path.last();
          ArticulationPointState<V> ws = state.get(w, null);
          if (ws == null) {
            ws = new ArticulationPointState<>(w, (int) path.size());
            vs.childCount++;
            state.put(w, ws);
            path.addLast(ws);
            branches.addLast(graph.out(w).iterator());
          } else {
            vs.lowlink = min(vs.lowlink, ws.depth);
          }

          // return
        } else {
          branches.popLast();
          ArticulationPointState<V> ws = path.popLast();

          if (path.size() > 0) {
            ArticulationPointState<V> vs = path.last();
            vs.lowlink = min(ws.lowlink, vs.lowlink);

            if ((path.size() > 1 && ws.lowlink >= vs.depth)
              || (path.size() == 1 && vs.childCount > 1)) {
              result.add(vs.node);
            }
          }
        }
      }
    }

    return result.forked();
  }

  private static class ShortestPathState<V> {
    public final V origin, node;
    public final ShortestPathState<V> prev;
    public final double distance;

    private ShortestPathState(V origin) {
      this.origin = origin;
      this.prev = null;
      this.node = origin;
      this.distance = 0;
    }

    public ShortestPathState(V node, ShortestPathState<V> prev, double edge) {
      this.origin = prev.origin;
      this.node = node;
      this.prev = prev;
      this.distance = prev.distance + edge;
    }

    public IList<V> path() {
      IList<V> result = new LinearList<>();

      ShortestPathState<V> curr = this;
      for (; ; ) {
        result.addFirst(curr.node);
        if (curr.node.equals(curr.origin)) {
          break;
        }
        curr = curr.prev;
      }

      return result;
    }
  }

  public static <V, E> Optional<IList<V>> shortestPath(IGraph<V, E> graph, V from, Predicate<V> accept, ToDoubleFunction<E> cost) {
    return shortestPath(graph, LinearList.of(from), accept, cost);
  }

  /**
   * @return the shortest path, if one exists, between a starting vertex and an accepted vertex, excluding trivial
   * solutions where a starting vertex is accepted.
   */
  public static <V, E> Optional<IList<V>> shortestPath(IGraph<V, E> graph, Iterable<V> start, Predicate<V> accept, ToDoubleFunction<E> cost) {
    IMap<V, IMap<V, ShortestPathState<V>>> originStates = new LinearMap<>();
    PriorityQueue<ShortestPathState<V>> queue = new PriorityQueue<>(Comparator.comparingDouble(x -> x.distance));

    for (V v : start) {
      if (graph.vertices().contains(v)) {
        ShortestPathState<V> init = new ShortestPathState<>(v);
        originStates.getOrCreate(v, LinearMap::new).put(v, init);
        queue.add(init);
      }
    }

    ShortestPathState<V> curr;
    for (; ; ) {
      curr = queue.poll();
      if (curr == null) {
        return Optional.empty();
      }

      IMap<V, ShortestPathState<V>> states = originStates.get(curr.origin).get();
      if (states.get(curr.node).get() != curr) {
        continue;
      } else if (curr.prev != null && accept.test(curr.node)) {
        return Optional.of(List.from(curr.path()));
      }

      for (V v : graph.out(curr.node)) {
        double edge = cost.applyAsDouble(graph.edge(curr.node, v));
        if (edge < 0) {
          throw new IllegalArgumentException("negative edge weights are unsupported");
        }

        ShortestPathState<V> next = states.get(v, null);
        if (next == null) {
          next = new ShortestPathState<V>(v, curr, edge);
        } else if (curr.distance + edge < next.distance) {
          next = new ShortestPathState<V>(v, curr, edge);
        } else {
          continue;
        }

        states.put(v, next);
        queue.add(next);
      }
    }
  }

  /// directed graphs

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
   * @param graph             a directed graph
   * @param includeSingletons if false, omits any singleton vertex sets
   * @return a set of all strongly connected vertices in the graph
   */
  public static <V> Set<Set<V>> stronglyConnectedComponents(IGraph<V, ?> graph, boolean includeSingletons) {

    if (!graph.isDirected()) {
      throw new IllegalArgumentException("graph must be directed, try Graphs.connectedComponents instead");
    }

    // algorithmic state
    IMap<V, TarjanState> state = new LinearMap<>(
      (int) graph.vertices().size(),
      graph.vertexHash(),
      graph.vertexEquality());
    LinearList<V> stack = new LinearList<>();

    // call-stack state
    LinearList<V> path = new LinearList<>();
    LinearList<Iterator<V>> branches = new LinearList<>();

    Set<Set<V>> result = new Set<Set<V>>().linear();

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
            ws = new TarjanState((int) state.size());
            state.put(w, ws);
            stack.addLast(w);
            path.addLast(w);
            branches.addLast(graph.out(w).iterator());
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

          // create a new group
          if (ws.lowlink == ws.index) {
            if (!includeSingletons && stack.last() == w) {
              stack.popLast();
              state.get(w).get().onStack = false;
            } else {
              Set<V> group = new Set<V>(graph.vertexHash(), graph.vertexEquality()).linear();
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

  public static <V, E> List<IGraph<V, E>> stronglyConnectedSubgraphs(IGraph<V, E> graph, boolean includeSingletons) {
    List<IGraph<V, E>> result = new List<IGraph<V, E>>().linear();
    stronglyConnectedComponents(graph, includeSingletons).forEach(s -> result.addLast(graph.select(s)));
    return result.forked();
  }

  public static <V, E> List<List<V>> cycles(IGraph<V, E> graph) {

    if (!graph.isDirected()) {
      throw new IllegalArgumentException("graph must be directed");
    }

    // traversal
    LinearList<V> path = new LinearList<>();
    IList<Iterator<V>> branches = new LinearList<>();

    //state
    LinearSet<V> blocked = new LinearSet<>(graph.vertexHash(), graph.vertexEquality());

    // TODO: implement the rest of the Johnson algorithm, which has better asymptotic behavior
    List<List<V>> result = new List<List<V>>().linear();
    for (IGraph<V, E> subgraph : stronglyConnectedSubgraphs(graph, true)) {

      blocked.clear();

      for (V seed : subgraph.vertices()) {

        long threshold = subgraph.indexOf(seed);

        path.addLast(seed);
        branches.addLast(subgraph.out(seed).iterator());

        do {
          // traverse deeper
          if (branches.last().hasNext()) {
            V x = branches.last().next();
            if (subgraph.indexOf(x) < threshold) {
              continue;
            }

            if (subgraph.vertexEquality().test(seed, x)) {
              result.addLast(List.from(path).addLast(seed));
            } else if (!blocked.contains(x)) {
              path.addLast(x);
              branches.addLast(subgraph.out(x).iterator());
            }

            blocked.add(x);

            // return
          } else {
            branches.removeLast();
            blocked.remove(path.popLast());
          }
        } while (path.size() > 0);
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


}
