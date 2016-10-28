(ns bifurcan.linear-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u]
   [criterium.core :as c])
  (:import
   [java.util
    HashMap
    HashSet
    ArrayList]
   [io.lacuna.bifurcan.utils
    BitVector
    Bits]
   [io.lacuna.bifurcan
    LinearList
    LinearMap
    LinearSet]))

;;;

(defn list-append [^LinearList l x]
  (.append l x))

(defn map-put [^LinearMap m k v]
  (.put m k v))

(defn map-remove [^LinearMap m k]
  (.remove m k))

(defn set-add [^LinearSet m e]
  (.add m e))

(defn set-remove [^LinearSet m e]
  (.remove m e))

(defn ->map [^LinearMap m]
  (->> m .entries .iterator iterator-seq (map #(vector (.key %) (.value %))) (into {})))

(defn ->set [^LinearSet s]
  (->> s .elements .iterator iterator-seq (into #{})))

;;;

(defn list-actions []
  {:add (u/action [gen/pos-int] conj list-append)})

(defn map-actions []
  {:put    (u/action [gen/pos-int gen/pos-int] assoc map-put)
   :remove (u/action [gen/pos-int] dissoc map-remove)})

(defn set-actions []
  {:add    (u/action [gen/pos-int] conj set-add)
   :remove (u/action [gen/pos-int] disj set-remove)})

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

(u/def-collection-check test-linear-map-equality 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m (->map m')))

(u/def-collection-check test-linear-map-lookup 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m (zipmap (keys m) (->> m keys (map #(-> m' (.get %) (.orElse nil)))))))

(u/def-collection-check test-linear-map-merge 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m' (->> (.partition m' 8) (reduce #(.merge ^LinearMap %1 %2)))))

(u/def-collection-check test-linear-set 1e4 (set-actions)
  [s #{}
   s' (LinearSet.)]
  (= s (->set s')))


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

(defn run-benchmark-linear-map [n]
  (println "\n*** put entries to LinearMap")
  (let [s (vec (repeatedly n uuid))]
    (c/quick-bench
      (loop [m (LinearMap. 16), s s]
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
        m  (reduce #(.put ^LinearMap %1 %2 %2) (LinearMap. 16) ks)]
    (c/quick-bench
      (doseq [k ks]
        (.get ^LinearMap m k))))

  (println "\n*** get entries from HashMap")
  (let [ks (vec (repeatedly n uuid))
        m  (reduce #(doto ^HashMap %1 (.put %2 %2)) (HashMap. 16) ks)]
    (c/quick-bench
      (doseq [k ks]
        (.get ^HashMap m k)))))

(defn run-benchmark-linear-set [n]
  (println "\n*** add entries to LinearSet")
  (let [s (vec (repeatedly n uuid))]
    (c/quick-bench
      (loop [m (LinearSet. 16), s s]
        (when-not (empty? s)
          (recur (.add m (first s)) (rest s))))))

  (println "\n*** add entries to HashSet")
  (let [s (vec (repeatedly n uuid))]
    (c/quick-bench
      (loop [m (HashSet. 16), s s]
        (when-not (empty? s)
          (recur (doto m (.add (first s))) (rest s))))))

  (println "\n*** check entries in LinearSet")
  (let [ks (vec (repeatedly n uuid))
        m  (reduce #(.add ^LinearSet %1 %2) (LinearSet. 16) ks)]
    (c/quick-bench
      (doseq [k ks]
        (.contains ^LinearSet m k))))

  (println "\n*** check entries in HashSet")
  (let [ks (vec (repeatedly n uuid))
        m  (reduce #(doto ^HashSet %1 (.add %2)) (HashSet. 16) ks)]
    (c/quick-bench
      (doseq [k ks]
        (.contains ^HashSet m k)))))

(defn run-benchmark-clojure-map [n]
  (println "\n*** put entries to {}")
  (let [s (vec (repeatedly n uuid))]
    (c/quick-bench
      (loop [m (transient {}), s s]
        (when-not (empty? s)
          (recur (assoc! m (first s) (first s)) (rest s))))))

  (println "\n*** get entries from {}")
  (let [ks (vec (repeatedly n uuid))
        m (->> ks (map #(vector % %)) (into {}))]
    (c/quick-bench
      (doseq [k ks]
        (get m k)))))

(defn run-benchmark-clojure-set [n]
  (println "\n*** put entries to #{}")
  (let [s (vec (repeatedly n uuid))]
    (c/quick-bench
      (loop [m (transient #{}), s s]
        (when-not (empty? s)
          (recur (conj! m (first s)) (rest s))))))

  (println "\n*** get entries from #{}")
  (let [ks (vec (repeatedly n uuid))
        m (->> ks (into #{}))]
    (c/quick-bench
      (doseq [k ks]
        (contains? m k)))))

(deftest ^:benchmark benchmark-maps
  (doseq [n [10 1e2 1e3 1e4 1e5 1e6 1e7]]
    (println "\n\n=== benchmarking map: n =" n)
    (run-benchmark-linear-map n))

  (doseq [n [10 1e2 1e3 1e4 1e5 1e6 1e7]]
    (println "\n\n=== benchmarking {} n =" n)
    (run-benchmark-clojure-map n)))

(deftest ^:benchmark benchmark-sets
  (doseq [n [10 1e2 1e3 1e4 1e5 1e6 1e7]]
    (println "\n\n=== benchmarking set: n =" n)
    (run-benchmark-linear-set n))

  (doseq [n [10 1e2 1e3 1e4 1e5 1e6 1e7]]
    (println "\n\n=== benchmarking #{} n =" n)
    (run-benchmark-clojure-set n)))
