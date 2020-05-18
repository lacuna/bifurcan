(ns bifurcan.bit-int-set-test
  (:refer-clojure :exclude [bit-set])
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

(defn set-actions [max-val]
  (let [gen-element (gen/large-integer* {:min 0, :max max-val})]
    {:add    [gen-element]
     :remove [gen-element]}))

(def java-set
  {:add    hash-add
   :remove hash-remove})

(defn bit-set [bits]
  {:add    #(bit-set-add %1 bits %2)
   :remove #(bit-set-remove %1 bits %2)})

(u/def-collection-check test-bit-int-set-16 1e4 (set-actions 16384)
  []
  [s (HashSet.) java-set
   [len v] [0 (BitIntSet/create)] (bit-set 16)]
  (= s (bit-int-set [len v] 16)))

(u/def-collection-check test-bit-int-set-48 1e4 (set-actions Integer/MAX_VALUE)
  []
  [s (HashSet.) java-set
   [len v] [0 (BitIntSet/create)] (bit-set 48)]
  (= s (bit-int-set [len v] 48)))
