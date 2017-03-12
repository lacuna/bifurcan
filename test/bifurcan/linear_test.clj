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
  {:add-first (u/action [gen/pos-int] list-add-first #(cons %2 %1))
   :add-last (u/action [gen/pos-int] list-add-last #(conj (vec %1) %2))
   :remove-first (u/action [] list-remove-first #(or (rest %) []))
   :remove-last (u/action [] list-remove-last #(or (butlast %) []))})

(defn map-actions []
  {:put    (u/action [gen/large-integer gen/pos-int] map-put assoc!)
   :remove (u/action [gen/large-integer] map-remove dissoc!)})

(defn set-actions []
  {:add    (u/action [gen/large-integer] set-add conj)
   :remove (u/action [gen/large-integer] set-remove disj)})

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
  [m' (LinearMap.)
   m (transient {})]
  (map= (persistent! m) m'))

(u/def-collection-check test-map 1e4 (map-actions)
  [m' (.linear (Map.))
   m (transient {})]
  (map= (persistent! m) m'))

(u/def-collection-check test-int-map 1e4 (map-actions)
  [m' (.linear (IntMap.))
   m (transient {})]
  (map= (persistent! m) m'))

(u/def-collection-check test-linear-set 1e4 (set-actions)
  [s' (LinearSet.)
   s #{}]
  (set= s s'))

(u/def-collection-check test-set 1e4 (set-actions)
  [s' (Set.)
   s #{}]
  (set= s s'))

(u/def-collection-check test-linear-list 1e4 (list-actions)
  [l (LinearList.)
   v []]
  (list= v l))

(u/def-collection-check test-list 1e4 (list-actions)
  [l (List.)
   v []]
  (list= v l))

(u/def-collection-check test-linear-map-merge 1e4 (map-actions)
  [m' (LinearMap.)]
  (= m' (->> (.split ^IMap m' 8) (reduce #(.union ^IMap %1 %2)))))

(u/def-collection-check test-map-merge 1e4 (map-actions)
  [m' (Map.)]
  (= m' (->> (.split ^IMap m' 8) (reduce #(.union ^IMap %1 %2)))))
