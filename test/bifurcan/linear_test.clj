(ns bifurcan.linear-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u]
   [criterium.core :as c]
   [clojure.pprint :refer (pprint)])
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
  (= m' (->> (.split m' 8) (reduce #(.merge ^LinearMap %1 %2)))))

(u/def-collection-check test-linear-set 1e4 (set-actions)
  [s #{}
   s' (LinearSet.)]
  (= s (->set s')))


;;;

(defn uuid []
  (str (java.util.UUID/randomUUID)))

(defn append-to-linear-list [^long n]
  (loop [l (LinearList.), idx 0]
    (when (< idx n)
      (recur (.append l idx) (unchecked-inc idx)))))

(defn append-to-array-list [^long n]
  (loop [l (ArrayList.), idx 0]
    (when (< idx n)
      (recur (doto l (.add idx)) (unchecked-inc idx)))))

(defn construct-linear-map [^LinearMap m vs]
  (loop [m m, vs vs]
    (if (empty? vs)
      m
      (recur (.put m (first vs) nil) (rest vs)))))

(defn construct-hash-map [m vs]
  (loop [m (HashMap. 16), vs vs]
    (if (empty? vs)
      m
      (recur (doto m (.put (first vs) nil)) (rest vs)))))

(defn construct-clojure-map [m vs]
  (loop [m (transient m), vs vs]
    (if (empty? vs)
      (persistent! m)
      (recur (assoc! m (first vs) nil) (rest vs)))))

(defn lookup-linear-map [^LinearMap m ks]
  (doseq [k ks]
    (.get m k)))

(defn lookup-hash-map [^HashMap m ks]
  (doseq [k ks]
    (.get m k)))

(defn lookup-clojure-map [m ks]
  (doseq [k ks]
    (get m k)))

(defn construct-linear-set [^LinearSet s vs]
  (loop [s s, vs vs]
    (if (empty? vs)
      s
      (recur (.add s (first vs)) (rest vs)))))

(defn construct-hash-set [^HashSet s vs]
  (loop [s s, vs vs]
    (if (empty? vs)
      s
      (recur (doto s (.add (first vs))) (rest vs)))))

(defn construct-clojure-set [s vs]
  (loop [s (transient s), vs vs]
    (if (empty? vs)
      (persistent! s)
      (recur (conj! s (first vs)) (rest vs)))))

(defn lookup-linear-set [^LinearSet s vs]
  (doseq [v vs]
    (.contains ^LinearSet s v)))

(defn lookup-hash-set [^HashSet s vs]
  (doseq [v vs]
    (.contains ^HashSet s v)))

(defn lookup-clojure-set [s vs]
  (doseq [v vs]
    (contains? s v)))

;;;

(defn benchmark [f]
  (-> (c/quick-benchmark* f nil)
    :mean
    first
    (* 1e9)))

(defn generate-entries [n]
  (->> uuid (repeatedly n) vec))

(defn benchmark-collection [base-collection construct lookup]
  (prn (class (base-collection 0)))
  (->> (range 1 7)
    (map #(Math/pow 10 %))
    (map (fn [n]
           (prn n)
           (let [s  (generate-entries n)
                 s' (generate-entries n)
                 c  (base-collection n)
                 c' (construct c s)]
             [n (->>
                  {:construct           (benchmark #(construct (base-collection n) s))
                   :construct-duplicate (benchmark #(-> (base-collection n) (construct s) (construct s)))
                   :lookup              (benchmark #(lookup c' s))
                   :lookup-misses       (benchmark #(lookup c' s'))}
                  (map (fn [[k v]]
                         [k (int (/ v n))]))
                  (into {}))])))
    (into {})))

(deftest ^:benchmark benchmark-collections
  (pprint
    {:linear-map  (benchmark-collection (fn [_] (LinearMap.)) construct-linear-map lookup-linear-map)
     :linear-set  (benchmark-collection (fn [_] (LinearSet.)) construct-linear-set lookup-linear-set)
     :hash-map    (benchmark-collection (fn [_] (HashMap.)) construct-hash-map lookup-hash-map)
     :hash-set    (benchmark-collection (fn [_] (HashSet.)) construct-hash-set lookup-hash-set)
     :clojure-map (benchmark-collection (fn [_] {}) construct-clojure-map lookup-clojure-map)
     :clojure-set (benchmark-collection (fn [_] #{}) construct-clojure-set lookup-clojure-set)
     }))
