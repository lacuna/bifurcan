(ns bifurcan.linear-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u])
  (:import
   [io.lacuna.bifurcan.utils
    BitVector]
   [io.lacuna.bifurcan
    LinearList
    LinearMap]))

;;;

(defn list-append [^LinearList l x]
  (.append l x))

(defn map-put [^LinearMap m k v]
  (.put m k v))

(defn map-remove [^LinearMap m k]
  (.remove m k))

(defn ->map [^LinearMap m]
  (->> m .entries .iterator iterator-seq (map #(vector (.key %) (.value %))) (into {})))

;;;

(defn list-actions []
  {:add (u/action [gen/pos-int] conj list-append)})

(defn map-actions []
  {:put    (u/action [gen/pos-int gen/pos-int] assoc map-put)
   :remove (u/action [gen/pos-int] dissoc map-remove)})

(defn construct-maps [actions]
  (let [[a b] (u/apply-actions
                (map-actions)
                actions
                {}
                (LinearMap.))]
    [a b]))

(u/def-collection-check test-linear-list 1e4 (list-actions)
  [v []
   l (LinearList.)]
  (= (seq v) (-> l .iterator iterator-seq)))

(u/def-collection-check test-linear-map 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m (->map m')))
