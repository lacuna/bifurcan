(ns bifurcan.collection-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)]
   [bifurcan.test-utils :as u]
   [clojure.set :as set])
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
    Iterators]
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

(set! *warn-on-reflection* false)

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

(defn ->vec [^IList l]
  (->> l .iterator iterator-seq (into [])))

;;;

(defn map= [a ^IMap b]
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
   :set          [(gen/choose 0 999) gen/pos-int]
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
   :set          #(assoc (vec %1) (min (count %1) %2) %3)
   :remove-first #(or (rest %) [])
   :remove-last  #(or (butlast %) [])})

(def bifurcan-list
  {:add-first    #(.addFirst ^IList %1 %2)
   :add-last     #(.addLast ^IList %1 %2)
   :set          #(.set ^IList %1 (min (.size ^IList %1) %2) %3)
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

;; Generators

(defn map-gen [init]
  (->> map-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-map))))

(defn set-gen [init]
  (->> set-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-set))))

(defn list-gen [init]
  (->> list-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-list))))

;; Maps

(u/def-collection-check test-linear-map iterations map-actions
  [a (transient {}) clj-map
   b (LinearMap.) bifurcan-map]
  (map= (persistent! a) b))

(u/def-collection-check test-map iterations map-actions
  [a (transient {}) clj-map
   b (Map.) bifurcan-map
   c (.linear (Map.)) bifurcan-map]
  (let [a (persistent! a)]
    (and
      (= b c)
      (map= a b)
      (map= a c))))

(u/def-collection-check test-int-map iterations map-actions
  [a (transient {}) clj-map
   b (IntMap.) bifurcan-map
   c (.linear (IntMap.)) bifurcan-map]
  (let [a (persistent! a)]
    (and
      (= b c)
      (map= a b)
      (map= a c))))

(u/def-collection-check test-unreified-map iterations map-actions
  [a (transient {}) clj-map
   b Maps/EMPTY bifurcan-map]
  (let [a (persistent! a)]
    (map= a b)))

;; Sets

(u/def-collection-check test-linear-set iterations set-actions
  [a (transient #{}) clj-set
   b (LinearSet.) bifurcan-set]
  (set= (persistent! a) b))

(u/def-collection-check test-set iterations set-actions
  [a (transient #{}) clj-set
   b (Set.) bifurcan-set
   c (.linear (Set.)) bifurcan-set]
  (let [a (persistent! a)]
    (and
      (= b c)
      (set= a b)
      (set= a c))))

(u/def-collection-check test-unreified-set iterations set-actions
  [a (transient #{}) clj-set
   b Sets/EMPTY bifurcan-set]
  (let [a (persistent! a)]
    (set= a b)))

;; Lists

(u/def-collection-check test-linear-list iterations list-actions
  [a [] clj-list
   b (LinearList.) bifurcan-list]
  (list= a b))

(u/def-collection-check test-list iterations list-actions
  [a [] clj-list
   b (List.) bifurcan-list
   c (.linear (List.)) bifurcan-list]
  (and
    (= b c)
    (list= a b)
    (list= a c)))

(u/def-collection-check test-unreified-list iterations list-actions
  [a [] clj-list
   b Lists/EMPTY bifurcan-list
   c (.linear (Lists/from [])) bifurcan-list]
  (and
    (= b c)
    (list= a b)
    (list= a c)))

;; Collection split/merge

(defn map-union [maps init]
  (reduce #(.union ^IMap %1 %2) init maps))

(defn set-union [sets init]
  (reduce #(.union ^ISet %1 %2) init sets))

(defspec test-linear-map-split iterations
  (prop/for-all [m (map-gen #(LinearMap.))]
    (-> m (.split 2) (map-union (LinearMap.)))))

(defspec test-map-split iterations
  (prop/for-all [m (map-gen #(Map.))]
    (-> m (.split 2) (map-union (Map.)))))

(defspec test-int-map-split iterations
  (prop/for-all [m (map-gen #(IntMap.))]
    (-> m (.split 2) (map-union (IntMap.)))))

(defspec test-linear-list-split iterations
  (prop/for-all [l (list-gen #(LinearList.))]
    (-> l (.split 2) into-array Lists/concat)))

(defspec test-list-split iterations
  (prop/for-all [l (list-gen #(List.))]
    (-> l (.split 2) into-array Lists/concat)))

(defspec test-unreified-list-split iterations
  (prop/for-all [l (list-gen #(Lists/from []))]
    (-> l (.split 2) into-array Lists/concat)))

(defspec test-linear-set-split iterations
  (prop/for-all [s (set-gen #(LinearSet.))]
    (-> s (.split 2) (set-union (LinearSet.)))))

(defspec test-set-split iterations
  (prop/for-all [s (set-gen #(Set.))]
    (-> s (.split 2) (set-union (Set.)))))

;; IList concat

(defspec test-list-concat iterations
  (prop/for-all [a (list-gen #(List.))
                 b (list-gen #(List.))]
    (= (concat (->vec a) (->vec b))
      (->vec (.concat ^IList a b)))))

(defspec test-linear-list-concat iterations
  (prop/for-all [a (list-gen #(LinearList.))
                 b (list-gen #(LinearList.))]
    (= (concat (->vec a) (->vec b))
      (->vec (.concat ^IList a b)))))

(defspec test-unreified-list-concat iterations
  (prop/for-all [a (list-gen #(Lists/from [1 2 3]))
                 b (list-gen #(Lists/from [1 2 3]))]
    (= (concat (->vec a) (->vec b))
      (->vec (.concat ^IList a b)))))

;; LinearSet set operations

(defspec test-linear-set-union iterations
  (prop/for-all [a (set-gen #(LinearSet.))
                 b (set-gen #(LinearSet.))]
    (= (set/union (->set a) (->set b))
      (->set (.union ^ISet (.clone a) b)))))

(defspec test-linear-set-intersection iterations
  (prop/for-all [a (set-gen #(LinearSet.))
                 b (set-gen #(LinearSet.))]
    (= (set/intersection (->set a) (->set b))
      (->set (.intersection ^ISet (.clone a) ^ISet b)))))

(defspec test-linear-set-difference iterations
  (prop/for-all [a (set-gen #(LinearSet.))
                 b (set-gen #(LinearSet.))]
    (= (set/difference (->set a) (->set b))
      (->set (.difference ^ISet (.clone a) ^ISet b)))))

;; Map set operations

(defspec test-map-merge iterations
  (prop/for-all [a (map-gen #(Map.))
                 b (map-gen #(Map.))]
    (= (merge (->map a) (->map b))
      (->map (.union ^IMap a b)))))

(defspec test-map-intersection iterations
  (prop/for-all [a (map-gen #(Map.))
                 b (map-gen #(Map.))]
    (= (select-keys (->map a) (keys (->map b)))
      (->map (.intersection ^IMap a ^IMap b)))))

(defspec test-map-difference iterations
  (prop/for-all [a (map-gen #(Map.))
                 b (map-gen #(Map.))]
    (= (apply dissoc (->map a) (keys (->map b)))
      (->map (.difference ^IMap a ^IMap b)))))

;; LinearMap set operations

(defspec test-linear-map-merge iterations
  (prop/for-all [a (map-gen #(LinearMap.))
                 b (map-gen #(LinearMap.))]
    (= (merge (->map a) (->map b))
      (->map (.union ^IMap a b)))))

(defspec test-linear-map-intersection iterations
  (prop/for-all [a (map-gen #(LinearMap.))
                 b (map-gen #(LinearMap.))]
    (= (select-keys (->map a) (keys (->map b)))
      (->map (.intersection ^IMap a ^IMap b)))))

(defspec test-linear-map-difference iterations
  (prop/for-all [a (map-gen #(LinearMap.))
                 b (map-gen #(LinearMap.))]
    (= (apply dissoc (->map a) (keys (->map b)))
      (->map (.difference ^IMap a ^IMap b)))))

;; IntMap set operations

(defspec test-int-map-merge iterations
  (prop/for-all [a (map-gen #(IntMap.))
                 b (map-gen #(IntMap.))]
    (= (merge (->map a) (->map b))
      (->map (.union ^IMap a b)))))

(defspec test-int-map-intersection iterations
  (prop/for-all [a (map-gen #(IntMap.))
                 b (map-gen #(IntMap.))]
    (= (select-keys (->map a) (keys (->map b)))
      (->map (.intersection ^IMap a ^IMap b)))))

(defspec test-int-map-difference iterations
  (prop/for-all [a (map-gen #(IntMap.))
                 b (map-gen #(IntMap.))]
    (= (apply dissoc (->map a) (keys (->map b)))
      (->map (.difference ^IMap a ^IMap b)))))
