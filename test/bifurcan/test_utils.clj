(ns bifurcan.test-utils
  (:require
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)])
  (:import
   [org.apache.commons.lang3.builder
    ReflectionToStringBuilder
    RecursiveToStringStyle]))

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

(defn reflect-to-str
  "Recursively reflect over the object to create a string.
  Use to snapshot object internals to assert immutability."
  [obj]
  (ReflectionToStringBuilder/toString obj (RecursiveToStringStyle.)))
