(ns bifurcan.graph-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)]
   [bifurcan.test-utils :as u]
   [clojure.set :as set])
  (:import
   [io.lacuna.bifurcan
    Graph
    DirectedGraph
    DirectedAcyclicGraph
    IGraph
    Graphs
    Set
    ISet]))

;;;

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
      #(-> % Graphs/stronglyConnectedComponents .size zero?)
      gen-digraph)))

;;;

(defspec test-strongly-connected-components 1e5
  (prop/for-all [digraph gen-digraph]
    (= (naive-strongly-connected-components digraph)
      (->> digraph
        Graphs/stronglyConnectedComponents
        ->set
        (map ->set)
        set))))

(defspec test-articulation-points 1e5
  (prop/for-all [graph gen-graph]
    (= (naive-articulation-points graph)
      (->> graph
        Graphs/articulationPoints
        ->set))))
