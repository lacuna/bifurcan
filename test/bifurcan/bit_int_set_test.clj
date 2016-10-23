(ns bifurcan.bit-int-set-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u])
  (:import
   [java.util
    HashSet]
   [io.lacuna.bifurcan.utils
    BitIntSet]))

;;;

(defn hash-add [^HashSet s v]
  (doto s (.add v)))

(defn hash-remove [^HashSet s v]
  (doto s (.remove v)))

;;;

(defn bit-set-add [[len v] bits val]
  (let [v' (BitIntSet/add v bits len val)]
    (if (identical? v v')
      [len v]
      [(inc len) v'])))

(defn bit-set-remove [[len v] bits val]
  (let [v' (BitIntSet/remove v bits len val)]
    (if (identical? v v')
      [len v]
      [(dec len) v'])))

(defn bit-int-set [[len v] bits]
  (->> (range len)
    (map #(BitIntSet/get v bits %))
    set))

;;;

(defn set-actions [bits max-val]
  (let [gen-element (gen/large-integer* {:min 0, :max max-val})]
    {:add    (u/action [gen-element]
               hash-add
               #(bit-set-add %1 bits %2))
     :remove (u/action [gen-element]
               hash-remove
               #(bit-set-remove %1 bits %2))}))

(defn construct-colls [bits actions]
  (let [[l [len v]] (u/apply-actions
                      (set-actions bits 0)
                      actions
                      (HashSet.)
                      [0 (BitIntSet/create)])]
    [l (bit-int-set [len v] bits)]))

(u/def-collection-check test-bit-int-set-16 1e4 (set-actions 16 16384)
  [s (HashSet.)
   [len v] [0 (BitIntSet/create)]]
  (= s (bit-int-set [len v] 16)))

(u/def-collection-check test-bit-int-set-48 1e4 (set-actions 48 Integer/MAX_VALUE)
  [s (HashSet.)
   [len v] [0 (BitIntSet/create)]]
  (= s (bit-int-set [len v] 48)))
