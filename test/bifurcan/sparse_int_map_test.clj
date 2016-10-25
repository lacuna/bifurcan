(ns bifurcan.sparse-int-map-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u]
   [criterium.core :as c])
  (:import
   [java.util
    HashMap]
   [io.lacuna.bifurcan.utils
    SparseIntMap]))

;;;

(defn hash-put [^HashMap s k v]
  (doto s (.put k v)))

(defn hash-remove [^HashMap s k]
  (doto s (.remove k)))

(defn sparse-put [^SparseIntMap m k v]
  (.put m k v))

(defn sparse-remove [^SparseIntMap m k]
  (.remove m k))

(defn sparse->map [^SparseIntMap m]
  (->> (range (.size m))
    (map #(vector (.key m %) (.val m %)))
    (into {})))

;;;

(defn map-actions [max-val]
  (let [gen-key (gen/large-integer* {:min 0, :max max-val})
        gen-val gen/pos-int]
    {:put    (u/action [gen-key gen-val] hash-put sparse-put)
     :remove (u/action [gen-key] hash-remove sparse-remove)}))

(defn construct-colls [bits actions]
  (let [[a b] (u/apply-actions
                (map-actions bits 0)
                actions
                (HashMap.)
                SparseIntMap/EMPTY)]
    [a (sparse->map b)]))

(u/def-collection-check test-sparse-int-map 1e4 (map-actions Long/MAX_VALUE)
  [m (HashMap.)
   s SparseIntMap/EMPTY]
  (= m (sparse->map s)))

;;;
