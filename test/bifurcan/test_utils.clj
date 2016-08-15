(ns bifurcan.test-utils
  (:require
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)]))

(defn action [generators fa fb]
  {:generators generators
   :fa fa
   :fb fb})

(defn actions->generator [actions]
  (->> actions
    (map (fn [[name {:keys [generators]}]]
           (apply gen/tuple
             (gen/return name)
             generators)))
    gen/one-of
    gen/list))

(defn apply-actions [actions->generator actions a b]
  (loop [a a, b b, s actions]
    (if (empty? s)
      [a b]
      (let [[action & args] (first s)
            {:keys [fa fb]} (get actions->generator action)]
        (recur (apply fa a args) (apply fb b args) (rest s))))))

(defmacro def-collection-check
  [name num-checks action-spec [a a-gen, b b-gen] & predicate]
  `(defspec ~name ~num-checks
     (let [a# ~action-spec]
       (prop/for-all [actions# (actions->generator a#)]
         (let [[~a ~b] (apply-actions a# actions# ~a-gen ~b-gen)]
           ~@predicate)))))
