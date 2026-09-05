(ns bifurcan.test-utils
  (:require [clojure [datafy :refer [datafy]]]
            [clojure.core [protocols :as protocols]
                          [reducers :as reducers]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :as ct :refer (defspec)])
  (:import (io.lacuna.bifurcan IntMap
                               FloatMap
                               SortedMap
                               Map
                               Maps
                               List
                               Lists
                               Set
                               Sets
                               IEdge
                               IEntry
                               IList
                               IGraph
                               IMap
                               ISet
                               LinearList
                               LinearMap
                               LinearSet
                               SortedSet)))

(def iterations
  "Number of iterations for generative tests"
  (long 1e5))

;; Coercion back to Clojure structures.

(extend-protocol protocols/Datafiable
  IList
  (datafy [xs]
    (mapv datafy xs))

  IEntry
  (datafy [entry]
    (clojure.lang.MapEntry. (datafy (.key entry))
                            (datafy (.value entry))))

  IEdge
  (datafy [edge]
    {:from  (datafy (.from edge))
     :to    (datafy (.to edge))
     :value (datafy (.value edge))})

  IMap
  (datafy [s]
    (let [iter (.iterator s)]
      (loop [m (transient {})]
        (if (.hasNext iter)
          (let [kv ^IEntry (.next iter)]
            (recur (assoc! m
                           (datafy (.key kv))
                           (datafy (.value kv)))))
          (persistent! m)))))

  ISet
  (datafy [s]
    (let [iter (.iterator s)]
    (loop [s (transient #{})]
      (if (.hasNext iter)
        (recur (conj! s (datafy (.next iter))))
        (persistent! s)))))

  IGraph
  (datafy [g]
    (->> (.vertices g)
         (reducers/map (fn [vertex] [(datafy vertex) (datafy (.out g vertex))]))
         (into {}))))

;; Functional wrappers

(defn ->to-int-fn [f]
  (reify java.util.function.ToIntFunction
    (applyAsInt [_ x]
      (f x))))

(defn ->to-long-fn [f]
  (reify java.util.function.ToLongFunction
    (applyAsLong [_ x]
      (f x))))

(defn ->fn [f]
  (reify java.util.function.Function
    (apply [_ x]
      (f x))))

(defn ->unary-operator [f]
  (reify java.util.function.UnaryOperator
    (apply [_ x]
      (f x))))

(defn ->binary-operator [f]
  (reify java.util.function.BinaryOperator
    (apply [_ a b]
      (f a b))))

(defn ->predicate [f]
  (reify java.util.function.Predicate
    (test [_ x]
      (f x))))

(defn ->consumer [f]
  (reify java.util.function.Consumer
    (accept [_ a]
      (f a))))

(defn ->bi-consumer [f]
  (reify java.util.function.BiConsumer
    (accept [_ a b]
      (f a b))))

(defn ->bi-predicate [f]
  (reify java.util.function.BiPredicate
    (test [_ a b]
      (f a b))))

(defn ->bi-fn [f]
  (reify java.util.function.BiFunction
    (apply [_ a b]
      (f a b))))

(defn log-steps [n exponent steps]
  (let [log (int (/ (Math/log n) (Math/log exponent)))]
    (->> log
      (* steps)
      inc
      range
      (map #(Math/pow exponent (/ % steps))))))

(defn actions->generator [actions]
  (->> actions
    (map
      (fn [[name generators]]
        (apply gen/tuple
          (gen/return name)
          generators)))
    gen/one-of
    gen/list))

(defn apply-actions [actions coll action->fn]
  (reduce
    (fn [c [action & args]]
      (if (contains? action->fn action)
        (apply (action->fn action) c args)
        c))
    coll
    actions))

(defmacro def-collection-check
  [name iterations action-spec generators colls & predicate]
  (let [actions (gensym "actions")]
    `(defspec ~name ~iterations
       (prop/for-all [~@generators
                      ~actions (actions->generator ~action-spec)]
         (let [~@(->> (zipmap
                        (->> colls (partition 3) (map first))
                        (->> colls
                          (partition 3)
                          (map
                            (fn [[_ coll action->fn]]
                              `(apply-actions ~actions ~coll ~action->fn)))))
                   (apply concat))]
           (if-not (do ~@predicate)
             (do #_(prn ~actions) false)
             true))))))
