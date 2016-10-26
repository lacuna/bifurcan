(ns bifurcan.linear-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u]
   [criterium.core :as c])
  (:import
   [java.util
    HashMap
    ArrayList]
   [io.lacuna.bifurcan.utils
    BitVector
    Bits]
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

(u/def-collection-check test-linear-map-merge 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m' (->> (.partition m' 8) (reduce #(.merge ^LinearMap %1 %2)))))


;;;

(defn uuid []
  (str (java.util.UUID/randomUUID)))

(deftest ^:benchmark benchmark-linear-list
  (println "\n*** append 1e6 elements to LinearList")
  (c/quick-bench
    (loop [l (LinearList.), idx 0]
      (when (< idx 1e6)
        (recur (.append l idx) (unchecked-inc idx)))))

  (println "\n*** append 1e6 elements to ArrayList")
  (c/quick-bench
    (loop [l (ArrayList.), idx 0]
      (when (< idx 1e6)
        (recur (doto l (.add idx)) (unchecked-inc idx))))))

(defn run-benchmark-linear-map [n load-factor]
  (println "\n*** put entries to LinearMap")
  (let [s (vec (repeatedly n uuid))]
    (c/quick-bench
      (loop [m (LinearMap. 16 load-factor), s s]
        (when-not (empty? s)
          (recur (.put m (first s) (first s)) (rest s))))))

  (println "\n*** put entries to HashMap")
  (let [s (vec (repeatedly n uuid))]
    (c/quick-bench
      (loop [m (HashMap. 16), s s]
        (when-not (empty? s)
          (recur (doto m (.put (first s) (first s))) (rest s))))))

  (println "\n*** get entries from LinearMap")
  (let [ks (vec (repeatedly n uuid))
        m  (reduce #(.put ^LinearMap %1 %2 %2) (LinearMap. 16 load-factor) ks)]
    (c/quick-bench
      (doseq [k ks]
        (.get ^LinearMap m k))))

  (println "\n*** get entries from HashMap")
  (let [ks (vec (repeatedly n uuid))
        m  (reduce #(doto ^HashMap %1 (.put %2 %2)) (HashMap. 16) ks)]
    (c/quick-bench
      (doseq [k ks]
        (.get ^HashMap m k)))))

(deftest ^:benchmark benchmark-linear-map
  (doall
    (for [n [10 1e2 1e3 1e4 1e5 1e6 1e7]
          load [0.5 0.75 0.85 0.9 0.95 0.99]]
      (do
        (println "\n\n=== benchmarking map: n =" n "load =" load)
        (run-benchmark-linear-map n load)))))

(deftest ^:benchmark benchmark-clojure-map
  (println "\n*** put 1e6 entries to {}")
  (let [s (vec (repeatedly 1e6 uuid))]
    (c/quick-bench
      (loop [m (transient {}), s s]
        (when-not (empty? s)
          (recur (assoc! m (first s) (first s)) (rest s))))))

  (println "\n*** get 1e6 entries from {}")
  (let [ks (vec (repeatedly 1e6 uuid))
        m (->> ks (map #(vector % %)) (into {}))]
    (c/quick-bench
      (doseq [k ks]
        (get m k)))))
