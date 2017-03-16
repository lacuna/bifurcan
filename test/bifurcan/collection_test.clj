(ns bifurcan.collection-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u])
  (:import
   [java.util
    HashMap
    HashSet
    ArrayList
    ArrayDeque
    Collection]
   [io.lacuna.bifurcan.utils
    BitVector
    Bits]
   [io.lacuna.bifurcan
    IntMap
    Map
    Maps
    List
    Lists
    Set
    Sets
    IMap
    IMap$IEntry
    IList
    ISet
    LinearList
    LinearMap
    LinearSet
    IMap$IEntry]))

(set! *warn-on-reflection* true)

;;;

(defn ->map [^IMap m]
  (->> m
    .entries
    .iterator
    iterator-seq
    (map (fn [^IMap$IEntry e] [(.key e) (.value e)]))
    (into {})))

(defn ->set [^ISet s]
  (->> s .elements .iterator iterator-seq (into #{})))

;;;

(defn map= [a ^IMap b]
  #_(when-not (= a
    (zipmap (.keys b) (->> b .keys (map #(.get b % nil))))
    (->map b))
    (prn a b))
  (= a
    (zipmap (.keys b) (->> b .keys (map #(.get b % nil))))
    (->map b)))

(defn set= [a ^ISet b]
  (= a
    (->set b)
    (->> b ->set (filter #(.contains b %)) (into #{}))))

(defn list= [a ^IList b]
  (= (seq a)
    (-> b .iterator iterator-seq)
    (->> (.size b) range (map #(.nth b %)) seq)))

;;;

(def list-actions
  {:add-first    [gen/pos-int]
   :add-last     [gen/pos-int]
   :remove-first []
   :remove-last  []})

(def map-actions
  {:put    [gen/large-integer gen/pos-int]
   :remove [gen/large-integer]})

(def set-actions
  {:add    [gen/large-integer]
   :remove [gen/large-integer]})

(def clj-list
  {:add-first    #(cons %2 %1)
   :add-last     #(conj (vec %1) %2)
   :remove-first #(or (rest %) [])
   :remove-last  #(or (butlast %) [])})

(def bifurcan-list
  {:add-first    #(.addFirst ^IList %1 %2)
   :add-last     #(.addLast ^IList %1 %2)
   :remove-first #(.removeFirst ^IList %)
   :remove-last  #(.removeLast ^IList %)})

(def clj-map
  {:put    assoc!
   :remove dissoc!})

(def bifurcan-map
  {:put    #(.put ^IMap %1 %2 %3)
   :remove #(.remove ^IMap %1 %2)})

(def clj-set
  {:add    conj!
   :remove disj!})

(def bifurcan-set
  {:add    #(.add ^ISet %1 %2)
   :remove #(.remove ^ISet %1 %2)})

;;;

(def iterations 1e4)

(u/def-collection-check test-linear-map iterations map-actions
  [m' (LinearMap.) bifurcan-map
   m (transient {}) clj-map]
  (map= (persistent! m) m'))

(u/def-collection-check test-map iterations map-actions
  [m' (.linear (Map.)) bifurcan-map
   m (transient {}) clj-map]
  (map= (persistent! m) m'))

(u/def-collection-check test-int-map iterations map-actions
  [m' (.linear (IntMap.)) bifurcan-map
   m (transient {}) clj-map]
  (map= (persistent! m) m'))

(u/def-collection-check test-linear-set iterations set-actions
  [s' (LinearSet.) bifurcan-set
   s (transient #{}) clj-set]
  (set= (persistent! s) s'))

(u/def-collection-check test-set iterations set-actions
  [s' (Set.) bifurcan-set
   s (transient #{}) clj-set]
  (set= (persistent! s) s'))

(u/def-collection-check test-linear-list iterations list-actions
  [l (LinearList.) bifurcan-list
   v [] clj-list]
  (list= v l))

(u/def-collection-check test-list iterations list-actions
  [l (List.) bifurcan-list
   v [] clj-list]
  (list= v l))

(u/def-collection-check test-linear-map-merge iterations map-actions
  [m (LinearMap.) bifurcan-map]
  (let [m' (->> (.split ^IMap m 8) (reduce #(.union ^IMap %1 %2) (LinearMap.)))]
    (= m m')))

(u/def-collection-check test-map-merge iterations map-actions
  [m (Map.) bifurcan-map]
  (let [m' (->> (.split ^IMap m 8) (reduce #(.union ^IMap %1 %2) (Map.)))]
    (when-not (= m m') (prn m m'))
    (= m m')))

(u/def-collection-check test-int-map-merge iterations map-actions
  [m (IntMap.) bifurcan-map]
  (let [m' (->> (.split ^IMap m 8) (reduce #(.union ^IMap %1 %2) (IntMap.)))]
    (= m m')))
