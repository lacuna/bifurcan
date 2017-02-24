(ns bifurcan.benchmark-test
  (:require
   [proteus :refer [let-mutable]]
   [potemkin :as p :refer (doary)]
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u]
   [criterium.core :as c]
   [clojure.pprint :refer (pprint)])
  (:import
   [java.util.function
    ToIntFunction
    BiPredicate]
   [java.util.concurrent
    ThreadLocalRandom]
   [java.util
    HashMap
    HashSet
    ArrayList
    ArrayDeque
    Collection]
   [io.lacuna.bifurcan
    Map
    IMap
    IList
    ISet
    LinearList
    LinearMap
    LinearSet
    IMap$IEntry]))

(def clojure-hash
  (reify ToIntFunction
    (applyAsInt [_ k]
      (clojure.lang.Util/hasheq k))))

(def clojure-eq
  (reify BiPredicate
    (test [_ a b]
      (clojure.lang.Util/equiv a b))))

(set! *warn-on-reflection* true)
(defn construct-linear-list [^LinearList l vs]
  (doary [v vs]
    (.addLast l v))
  l)

(defn construct-java-list [^java.util.List l vs]
  (doary [v vs]
    (.add l v))
  l)

(defn construct-java-deque [^java.util.Deque l vs]
  (doary [v vs]
    (.addLast l v))
  l)

(defn construct-vector [v vs]
  (let-mutable [l (transient v)]
    (doary [v vs]
      (set! l (conj! l v)))
    l))

(defn construct-map [^IMap m vs]
  (let-mutable [m (.linear m)]
    (doary [v vs]
      (set! m (.put ^IMap m v nil)))
    m))

(defn construct-hash-map [^HashMap m vs]
  (doary [v vs]
    (.put m v nil))
  m)

(defn construct-clojure-map [m vs]
  (let-mutable [m (transient m)]
    (doary [v vs]
      (set! m (assoc! m v nil)))
    (persistent! m)))

(defn lookup-linear-list [^LinearList l ks]
  (doary [k ks]
    (.nth l k)))

(defn lookup-java-list [^java.util.List l ks]
  (doary [k ks]
    (.get l k)))

(defn lookup-vector [v ks]
  (doary[k ks]
    (nth v k)))

(defn lookup-map [^IMap m ks]
  (doary [k ks]
    (.get m k nil)))

(defn lookup-hash-map [^HashMap m ks]
  (doary [k ks]
    (.get m k)))

(defn lookup-clojure-map [m ks]
  (doary [k ks]
    (get m k)))

(defn construct-linear-set [^LinearSet s vs]
  (doary [v vs]
    (.add s v))
  s)

(defn construct-hash-set [^HashSet s vs]
  (doary [v vs]
    (.add s v))
  s)

(defn construct-clojure-set [s ^objects vs]
  (let [len (alength vs)]
    (loop [s (transient s), i 0]
      (if (<= len i)
        (persistent! s)
        (recur (conj! s (get vs i)) (unchecked-inc i))))))

(defn lookup-linear-set [^LinearSet s vs]
  (doary [v vs]
    (.contains ^LinearSet s v)))

(defn lookup-hash-set [^HashSet s vs]
  (doary [v vs]
    (.contains ^HashSet s v)))

(defn lookup-clojure-set [s vs]
  (doary [v vs]
    (contains? s v)))

;;;

;; a simple object that exists to provide minimal overhead within a hashmap
(deftype Obj [^int hash]
  Object
  (hashCode [_] (int hash))
  (equals [this o] (identical? this o)))

(defn benchmark [f]
  (-> (c/quick-benchmark* f {:samples 18})
    :mean
    first
    (* 1e9)))

(defn generate-entries [n]
  (->> #(Obj. (rand-int Integer/MAX_VALUE)) (repeatedly n) into-array))

(defn generate-numbers [n]
  (->> n range shuffle into-array))

(defn benchmark-collection [base-collection generate-entries construct lookup test?]
  (prn (class (base-collection 0)))
  (->> (range 1 6)
    (map #(Math/pow 10 %))
    (map (fn [n]
           (println (str "10^" (int (Math/log10 n))))
           (let [n  (long n)
                 s  (generate-entries n)
                 s' (generate-entries n)
                 c  (base-collection n)
                 c' (construct c s)
                 s  (-> s seq shuffle into-array)]
             [n (->>
                  (merge
                    (when (test? :construct)
                      {:construct (benchmark #(construct (base-collection n) s))})
                    (when (test? :construct-duplicate)
                      {:construct-duplicate (benchmark #(-> (base-collection n) (construct s) (construct s)))})
                    (when (test? :lookup)
                      {:lookup (benchmark #(lookup c' s))})
                    (when (test? :lookup-misses)
                      {:lookup-misses (benchmark #(lookup c' s'))}))
                  (map (fn [[k v]]
                         [k (int (/ v n))]))
                  (into {}))])))
    (into {})))

(deftest ^:benchmark test-construction
  (pprint
    [:map-clj-semantics (benchmark-collection (fn [_] (Map. clojure-hash clojure-eq)) generate-entries construct-map lookup-map #{:construct :lookup})
     :map (benchmark-collection (fn [_] (Map.)) generate-entries construct-map lookup-map #{:construct :lookup})
     :clojure-map (benchmark-collection (fn [_] {}) generate-entries construct-clojure-map lookup-clojure-map #{:construct :lookup})]))

(deftest ^:benchmark benchmark-collections
  (pprint
    [(comment
       :linear-list (benchmark-collection (fn [_] (LinearList.)) generate-numbers construct-linear-list lookup-linear-list  #{:construct :lookup})
       :array-list  (benchmark-collection (fn [_] (ArrayList.)) generate-numbers construct-java-list lookup-java-list #{:construct :lookup})
       :array-deque (benchmark-collection (fn [_] (ArrayDeque.)) generate-numbers construct-java-deque nil #{:construct})
       :vector      (benchmark-collection (fn [_] []) generate-numbers construct-vector lookup-vector #{:construct :lookup}))

     :linear-map  (benchmark-collection (fn [_] (LinearMap.)) generate-entries construct-map lookup-map (constantly true))
     :linear-set  (benchmark-collection (fn [_] (LinearSet.)) generate-entries construct-linear-set lookup-linear-set (constantly true))
     :map         (benchmark-collection (fn [_] (Map.)) generate-entries construct-map lookup-map (constantly true))

     :hash-set    (benchmark-collection (fn [_] (HashSet.)) generate-entries construct-hash-set lookup-hash-set (constantly true))
     :clojure-set (benchmark-collection (fn [_] #{}) generate-entries construct-clojure-set lookup-clojure-set (constantly true))
     :hash-map    (benchmark-collection (fn [_] (HashMap.)) generate-entries construct-hash-map lookup-hash-map (constantly true))
     :clojure-map (benchmark-collection (fn [_] {}) generate-entries construct-clojure-map lookup-clojure-map (constantly true))]
    ))
