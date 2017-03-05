(ns bifurcan.linear-test
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
    Bits
    SparseIntMap]
   [io.lacuna.bifurcan
    IntMap
    Map
    List
    Set
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

(defn list-add-first [^IList l v]
  (.addFirst l v))

(defn list-add-last [^IList l v]
  (.addLast l v))

(defn list-remove-first [^IList l]
  (.removeFirst l))

(defn list-remove-last [^IList l]
  (.removeLast l))

(defn map-put [^IMap m k v]
  (.put m k v))

(defn map-remove [^IMap m k]
  (.remove m k))

(defn set-add [^ISet m e]
  (.add m e))

(defn set-remove [^ISet m e]
  (.remove m e))

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
  (and
    (= a
      (zipmap (.keys b) (->> b .keys (map #(.get b % nil))))
      (->map b))))

(defn set= [a ^ISet b]
  (= a
    (->set b)
    (->> b ->set (filter #(.contains b %)) (into #{}))))

(defn list= [a ^IList b]
  (= (seq a)
    (-> b .iterator iterator-seq)
    (->> (.size b) range (map #(.nth b %)) seq)))

;;;

(defn list-actions []
  {:add-first (u/action [gen/pos-int] #(cons %2 %1) list-add-first)
   :add-last (u/action [gen/pos-int] #(conj (vec %1) %2) list-add-last)
   :remove-first (u/action [] rest list-remove-first)
   :remove-last (u/action [] butlast list-remove-last)})

(defn map-actions []
  {:put    (u/action [gen/large-integer gen/pos-int] assoc map-put)
   :remove (u/action [gen/large-integer] dissoc map-remove)})

(defn set-actions []
  {:add    (u/action [gen/large-integer] conj set-add)
   :remove (u/action [gen/large-integer] disj set-remove)})

(defn construct-lists [actions]
  (let [[a b] (u/apply-actions
                (list-actions)
                actions
                []
                (LinearList.))]
    [a b]))

(defn construct-maps [actions]
  (let [[a b] (u/apply-actions
                (map-actions)
                actions
                {}
                (LinearMap.))]
    [a b]))

(u/def-collection-check test-linear-map 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (map= m m'))

(u/def-collection-check test-map 1e4 (map-actions)
  [m {}
   m' (.linear (Map.))]
  (map= m m'))

(u/def-collection-check test-linear-set 1e4 (set-actions)
  [s #{}
   s' (LinearSet.)]
  (set= s s'))

(u/def-collection-check test-set 1e4 (set-actions)
  [s #{}
   s' (Set.)]
  (set= s s'))

(u/def-collection-check test-linear-list 1e4 (list-actions)
  [v []
   l (LinearList.)]
  (list= v l))

(u/def-collection-check test-list 1e4 (list-actions)
  [v []
   l (List.)]
  (list= v l))

(u/def-collection-check test-linear-map-merge 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m' (->> (.split ^IMap m' 8) (reduce #(.union ^IMap %1 %2)))))

(u/def-collection-check test-map-merge 1e4 (map-actions)
  [m {}
   m' (Map.)]
  (= m' (->> (.split ^IMap m' 8) (reduce #(.union ^IMap %1 %2)))))
