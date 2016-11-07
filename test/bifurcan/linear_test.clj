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
    ArrayList
    ArrayDeque
    Collection]
   [io.lacuna.bifurcan.utils
    BitVector
    Bits
    SparseIntMap]
   [io.lacuna.bifurcan
    IMap
    IList
    ISet
    IEditableList
    IEditableSet
    IEditableMap
    LinearList
    LinearMap
    LinearSet
    IMap$IEntry]))

(set! *warn-on-reflection* true)

;;;

(defn list-add-first [^IEditableList l v]
  (.addFirst l v))

(defn list-add-last [^IEditableList l v]
  (.addLast l v))

(defn list-remove-first [^IEditableList l]
  (.removeFirst l))

(defn list-remove-last [^IEditableList l]
  (.removeLast l))

(defn map-put [^IEditableMap m k v]
  (.put m k v))

(defn map-remove [^IEditableMap m k]
  (.remove m k))

(defn set-add [^IEditableSet m e]
  (.add m e))

(defn set-remove [^IEditableSet m e]
  (.remove m e))

(defn ->map [^IMap m]
  (->> m .entries .iterator iterator-seq (map (fn [^IMap$IEntry e] [(.key e) (.value e)])) (into {})))

(defn ->set [^ISet s]
  (->> s .elements .iterator iterator-seq (into #{})))

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

(u/def-collection-check test-linear-list 1e4 (list-actions)
  [v []
   l (LinearList.)]
  (= (seq v) (-> ^LinearList l .iterator iterator-seq)))

(u/def-collection-check test-linear-map-equality 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m (->map m')))

(u/def-collection-check test-linear-map-lookup 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m (zipmap (keys m) (->> m keys (map #(-> ^IMap m' (.get % nil)))))))

(u/def-collection-check test-linear-map-merge 1e4 (map-actions)
  [m {}
   m' (LinearMap.)]
  (= m' (->> (.split ^IMap m' 8) (reduce #(.merge ^LinearMap %1 %2)))))

(u/def-collection-check test-linear-set 1e4 (set-actions)
  [s #{}
   s' (LinearSet.)]
  (= s (->set s')))


;;;

(defn construct-linear-list [^LinearList l vs]
  (loop [l l, vs vs]
    (if (empty? vs)
      l
      (recur (.addLast l (first vs)) (rest vs)))))

(defn construct-java-list [^java.util.List l vs]
  (loop [l l, vs vs]
    (if (empty? vs)
      l
      (recur (doto l (.add (first vs))) (rest vs)))))

(defn construct-java-deque [^java.util.Deque l vs]
  (loop [l l, vs vs]
    (if (empty? vs)
      l
      (recur (doto l (.addLast (first vs))) (rest vs)))))

(defn construct-vector [v vs]
  (loop [l (transient v), vs vs]
    (if (empty? vs)
      (persistent! l)
      (recur (conj! l (first vs)) (rest vs)))))

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

(defn lookup-linear-list [^LinearList l ks]
  (doseq [k ks]
    (.nth l k)))

(defn lookup-java-list [^java.util.List l ks]
  (doseq [k ks]
    (.get l k)))

(defn lookup-vector [v ks]
  (doseq [k ks]
    (nth v k)))

(defn lookup-linear-map [^LinearMap m ks]
  (doseq [k ks]
    (.get m k nil)))

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
  (->> #(Obj. (rand-int Integer/MAX_VALUE)) (repeatedly n) vec))

(defn generate-numbers [n]
  (->> n range shuffle vec))

(defn benchmark-collection [base-collection generate-entries construct lookup test?]
  (prn (class (base-collection 0)))
  (->> (range 1 8)
    (map #(Math/pow 10 %))
    (map (fn [n]
           (println (str "10^" (int (Math/log10 n))))
           (let [s  (generate-entries n)
                 s' (generate-entries n)
                 c  (base-collection n)
                 c' (construct c s)]
             [n (->>
                  (merge
                    (when (test? :construct)
                      {:construct (benchmark #(construct (base-collection n) s))})
                    #_(when (test? :construct-duplicate)
                      {:construct-duplicate (benchmark #(-> (base-collection n) (construct s) (construct s)))})
                    (when (test? :lookup)
                      {:lookup (benchmark #(lookup c' s))})
                    (when (test? :lookup-misses)
                      {:lookup-misses (benchmark #(lookup c' s'))}))
                  (map (fn [[k v]]
                         [k (int (/ v n))]))
                  (into {}))])))
    (into {})))

(deftest ^:benchmark benchmark-collections
  (pprint
    [:linear-list #_(benchmark-collection (fn [_] (LinearList.)) generate-numbers construct-linear-list lookup-linear-list  #{:construct :lookup})
     :array-list #_(benchmark-collection (fn [_] (ArrayList.)) generate-numbers construct-java-list lookup-java-list #{:construct :lookup})
     :array-deque #_(benchmark-collection (fn [_] (ArrayDeque.)) generate-numbers construct-java-deque nil #{:construct})
     :vector #_(benchmark-collection (fn [_] []) generate-numbers construct-vector lookup-vector #{:construct :lookup})

     :linear-map  (benchmark-collection (fn [_] (LinearMap.)) generate-entries construct-linear-map lookup-linear-map (constantly true))
     :linear-set  #_(benchmark-collection (fn [_] (LinearSet.)) generate-entries construct-linear-set lookup-linear-set (constantly true))

     :hash-set    #_(benchmark-collection (fn [_] (HashSet.)) generate-entries construct-hash-set lookup-hash-set (constantly true))
     :clojure-set #_(benchmark-collection (fn [_] #{}) generate-entries construct-clojure-set lookup-clojure-set (constantly true))
     :hash-map    (benchmark-collection (fn [_] (HashMap.)) generate-entries construct-hash-map lookup-hash-map (constantly true))
     :clojure-map (benchmark-collection (fn [_] {}) generate-entries construct-clojure-map lookup-clojure-map (constantly true))]
    ))
