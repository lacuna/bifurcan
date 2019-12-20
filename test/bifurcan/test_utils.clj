(ns bifurcan.test-utils
  (:require
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)]))

(def iterations 1e4)

(defn ->to-int-fn [f]
  (reify java.util.function.ToIntFunction
    (applyAsInt [_ x]
      (f x))))

(defn ->fn [f]
  (reify java.util.function.Function
    (apply [_ x]
      (f x))))

(defn ->predicate [f]
  (reify java.util.function.Predicate
    (test [_ x]
      (f x))))

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
  (let [log (/ (Math/log n) (Math/log exponent))]
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
    (fn [c [action & args]] (apply (action->fn action) c args))
    coll
    actions))

(defmacro def-collection-check
  [name iterations action-spec colls & predicate]
  (let [actions (gensym "actions")]
    `(defspec ~name ~iterations
       (prop/for-all [~actions (actions->generator ~action-spec)]
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
