(ns bifurcan.graph-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer [defspec]]
   [bifurcan.test-utils :as u :refer [iterations]]
   [clojure.set :as set])
  (:import
   [java.util.function
    BiPredicate
    ToDoubleFunction
    ToLongFunction]
   [io.lacuna.bifurcan
    List
    Graph
    DirectedGraph
    DirectedAcyclicGraph
    IGraph
    Maps
    Graphs
    Set
    ISet]))

;;;

(def fixed-cost
  (reify ToDoubleFunction
    (applyAsDouble [_ x]
      1.0)))

(defn ->set [^Iterable x]
  (->> x .iterator iterator-seq set))



(defn construct [graph vertex->out]
  (reduce
    (fn [g [v out]]
      (reduce #(.link ^IGraph %1 v %2) g out))
    graph
    vertex->out))

(defn vertices [^IGraph g]
  (.toSet (.vertices g)))

(defn in [^IGraph g v]
  (.toSet (.in g v)))

(defn out [^IGraph g v]
  (.toSet (.out g v)))

(defn reachable [^IGraph g f v]
  (loop [result   #{}
         frontier #{v}]
    (if (empty? frontier)
      result
      (let [result' (set/union result frontier)]
        (recur
          result'
          (set/difference
            (->> frontier
              (mapcat #(f g %))
              set)
            result'))))))

(defn naive-strongly-connected-components [^IGraph g]
  (let [v->downstream (zipmap
                        (vertices g)
                        (->> g vertices (map #(reachable g out %))))
        v->connected  (zipmap
                        (keys v->downstream)
                        (->> v->downstream
                          (map (fn [[k s]]
                                 (->> s
                                   (filter #(contains? (v->downstream %) k))
                                   set)))))]
    (->> v->connected
      vals
      (remove #(= 1 (count %)))
      set)))

(defn naive-articulation-points [^IGraph g]
  (let [n (-> g Graphs/connectedComponents .size)]
    (->> g
      vertices
      (remove #(-> (.out g %) (.remove %) .size zero?))
      ->set
      (filter #(not= n (-> g (.remove %) Graphs/connectedComponents .size)))
      set)))

(defn extend-path [^IGraph g path pred]
  (->> (.out g (last path))
    (filter pred)
    (remove (set (rest path)))
    (map #(conj path %))))

(defn naive-cycles [^IGraph g]
  (let [acc (atom #{})]
    (dotimes [i (-> g .vertices .size)]
      (let [seed (-> g .vertices (.nth i))
            pred (complement (disj (->> g .vertices (take i) set) seed))]
        (loop [paths [[seed]]]
          (when-not (empty? paths)
            (let [paths' (mapcat #(extend-path g % pred) paths)]

              (doseq [p (filter #(= seed (last %)) paths')]
                (swap! acc conj p))

              (recur (remove #(= seed (last %)) paths')))))))
    @acc))

(defn naive-merge
  "Merges two graphs together using a function (merge-fn edge-value1
  edge-value2)."
  [^IGraph a, ^IGraph b, merge-fn]
  (.forked ^IGraph
           (reduce (fn [g v]
                     (reduce (fn [^IGraph g v']
                               (.link g v v' (.edge b v v') merge-fn))
                             g
                             (.out b v)))
                   (.linear a)
                   (.vertices b))))

(deftest edge-test
  (doseq [g [(Graph.)
             (DirectedGraph.)
             (DirectedAcyclicGraph.)]]
    (let [g (.link g 1 2 :meow)]
      (is (= :meow (.edge g 1 2)))
      (is (= :meow (.edge g 1 2 :default)))
      (is (= :default (.edge g 2 3 :default))))))

(deftest select-equality-test
  ; Select should preserve vertex hash and equality semantics
  (let [eq (reify BiPredicate
             (test [_ a b] (= a b)))
        hash (reify ToLongFunction
               (applyAsLong [_ x]
                 (hash x)))]
    (doseq [g [(Graph. hash eq)
               (DirectedGraph. hash eq)
               (DirectedAcyclicGraph. hash eq)]]
      (let [g (.. g (link 1 2) (link 2 3) (link 3 4))]
        (is (identical? eq (.vertexEquality g)))
        (is (identical? hash (.vertexHash g)))
        (let [g' (.select g (Set/from [2 3]))]
          (is (identical? eq (.vertexEquality g')))
          (is (identical? hash (.vertexHash g'))))))))

;;;

(defn gen-sized-graph [init size]
  (let [size (min 10 size)]
    (gen/fmap
      #(construct init (zipmap (range size) %))
      (gen/vector
        (gen/fmap set (gen/vector (gen/choose 0 (dec size)) 0 size))
        size
        size))))

(def gen-graph
  (gen/sized #(gen-sized-graph (Graph.) %)))

(def gen-digraph
  (gen/sized #(gen-sized-graph (DirectedGraph.) %)))

(def gen-dag
  (gen/fmap
    #(DirectedAcyclicGraph/from %)
    (gen/such-that
      (fn [g] (-> g (Graphs/stronglyConnectedComponents false) .size zero?))
      gen-digraph)))

;;;

;; the `naive-cycles` is especially naive, so don't run too many iterations
(defspec test-cycles 1e3
  (prop/for-all [digraph gen-digraph]
    (= (naive-cycles digraph)
      (->> (Graphs/cycles digraph)
        (map seq)
        set))))

(defspec test-strongly-connected-components iterations
  (prop/for-all [digraph gen-digraph]
    (= (naive-strongly-connected-components digraph)
      (->> (Graphs/stronglyConnectedComponents digraph false)
        ->set
        (map ->set)
        set))))

(defspec test-articulation-points iterations
  (prop/for-all [graph gen-graph]
    (= (naive-articulation-points graph)
      (->> graph
        Graphs/articulationPoints
        ->set))))

(defspec ^:focus merge-digraph iterations
  (prop/for-all [a gen-digraph
                 b gen-digraph]
                (= (naive-merge a b Maps/MERGE_LAST_WRITE_WINS)
                   (.merge a b Maps/MERGE_LAST_WRITE_WINS)
                   ; Graphs/merge uses a diff, more general implementation
                   (Graphs/merge a b Maps/MERGE_LAST_WRITE_WINS))))
