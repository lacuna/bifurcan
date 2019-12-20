(ns bifurcan.benchmark-test
  (:require
   [proteus :refer [let-mutable]]
   [potemkin :as p :refer (doary doit)]
   [byte-streams :as bs]
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u]
   [bifurcan.rope-tests]
   [criterium.core :as c]
   [clojure.set :as set]
   [clojure.pprint :refer (pprint)]
   [clojure.java.shell :as sh]
   [clojure.java.io :as io])
  (:import
   [java.util.function
    ToIntFunction
    Predicate
    BiPredicate]
   [java.util.concurrent
    ThreadLocalRandom]
   [java.util
    Map$Entry
    HashMap
    HashSet
    TreeMap
    ArrayList
    ArrayDeque
    Collection
    Iterator
    PrimitiveIterator$OfInt]
   [org.organicdesign.fp.collections
    PersistentTreeMap
    PersistentHashMap
    PersistentHashMap$MutableHashMap
    PersistentHashSet
    PersistentHashSet$MutableHashSet
    PersistentVector
    PersistentVector$MutableVector
    RrbTree
    RrbTree$MutableRrbt]
   [io.usethesource.capsule.core
    PersistentTrieMap
    PersistentTrieSet]
   [org.pcollections
    PMap
    PSet
    PVector
    HashTreePMap
    TreePVector
    HashTreePSet]
   [io.usethesource.capsule
    Map$Transient
    Map$Immutable
    Set$Transient
    Set$Immutable]
   [io.lacuna.bifurcan
    IntMap
    Map
    SortedMap
    List
    IMap
    IList
    ISet
    Set
    ICollection
    LinearList
    LinearMap
    LinearSet
    IEntry
    Rope]))

(set! *warn-on-reflection* true)

(def clojure-hash
  (reify ToIntFunction
    (applyAsInt [_ k]
      (clojure.lang.Util/hasheq k))))

(def clojure-eq
  (reify BiPredicate
    (test [_ a b]
      (clojure.lang.Util/equiv a b))))

(defn scala-fn1 [f]
  (reify scala.Function1
    (apply [_ x]
      (f x))))

(defn predicate [f]
  (reify Predicate
    (test [_ x]
      (f x))))

(let [method (doto (.getDeclaredMethod Object "clone" (into-array Class []))
               (.setAccessible true))]
  (defn clone [x]
    (.invoke method x (object-array 0))))

(defmacro doary-int
  "An array-specific version of doseq."
  [[x ary] & body]
  (let [ary-sym (gensym "ary")]
    `(let [~(with-meta ary-sym {:tag "ints"}) ~ary]
       (dotimes [idx# (alength ~ary-sym)]
         (let [~x (aget ~ary-sym idx#)]
           ~@body)))))

;;; Bifurcan

(defn construct-set [^ISet s vs]
  (let [s (.linear s)]
    (doary [v vs]
      (.add s v))
    s))

(defn construct-list [^IList l vs]
  (let [l (.linear l)]
    (doary [v vs]
      (.addLast l v))
    l))

(defn construct-map [^IMap m vs]
  (let [m (.linear m)]
    (doary [v vs]
      (.put m v nil))
    m))

(defn construct-int-map [^IntMap m vs]
  (let [m (.linear m)]
    (doary [v vs]
      (.put m (long v) nil))
    m))

(defn lookup-set [^ISet s vs]
  (doary [v vs]
    (.contains s v)))

(defn lookup-list [^IList l ks]
  (doary [k ks]
    (.nth l k)))

(defn lookup-map [^IMap m ks]
  (doary [k ks]
    (.get m k nil)))

(defn lookup-int-map [^IntMap m ks]
  (doary [k ks]
    (.get m (long k) nil)))

(defn map-union [^IMap a ^IMap b]
  (.union a b))

(defn set-union [^ISet a ^ISet b]
  (.union a b))

(defn iterator [^Iterable c]
  (.iterator c))

(defn map-intersection [^IMap a ^IMap b]
  (.intersection a b))

(defn set-intersection [^ISet a ^ISet b]
  (.intersection a b))

(defn map-difference [^IMap a ^IMap b]
  (.difference a b))

(defn set-difference [^ISet a ^ISet b]
  (.difference a b))

(defn split [^ICollection c ^long parts]
  (.split c parts))

(defn consume-iterator [^Iterator it]
  (loop [x nil]
    (if (.hasNext it)
      (recur (or (.next it) x))
      x)))

(defn consume-int-iterator [^PrimitiveIterator$OfInt it]
  (loop [x nil]
    (if (.hasNext it)
      (recur (or (.nextInt it) x))
      x)))

(defn consume-entry-iterator [^Iterator it]
  (loop [x nil]
    (if (.hasNext it)
      (recur (or (.key ^IEntry (.next it)) x))
      x)))

;;; Java

(defn construct-java-list [^java.util.List l vs]
  (doary [v vs]
    (.add l v))
  l)

(defn construct-java-map [^java.util.Map m vs]
  (doary [v vs]
    (.put m v nil))
  m)

(defn union-java-maps [^java.util.Map a ^java.util.Map b]
  (let [^java.util.Map a (clone a)]
    (.putAll a b)
    a))

(defn diff-java-maps [^java.util.Map a ^java.util.Map b]
  (let [^java.util.Map a (clone a)]
    (.removeAll (.keySet a) (.keySet b))
    a))

(defn intersect-java-maps [^java.util.Map a ^java.util.Map b]
  (let [^java.util.Map m (clone a)]
    (doit [k (.keySet a)]
      (when-not (.containsKey b k)
        (.remove m k)))
    m))

(defn lookup-java-set [^java.util.Set s vs]
  (doary [v vs]
    (.contains s v)))

(defn lookup-java-list [^java.util.List l ks]
  (doary [k ks]
    (.get l k)))

(defn lookup-java-map [^java.util.Map m ks]
  (doary [k ks]
    (.get m k)))

(defn consume-java-entry-iterator [^Iterator it]
  (loop [x nil]
    (if (.hasNext it)
      (recur (or (.getKey ^Map$Entry (.next it)) x))
      x)))

(defn construct-hash-set [^HashSet s vs]
  (doary [v vs]
    (.add s v))
  s)

(defn intersect-hash-sets [^HashSet a ^HashSet b]
  (let [^HashSet m (.clone a)]
    (doit [x b]
      (when-not (.contains a x)
        (.remove m x)))
    m))

;;; Clojure

(defn construct-clojure-map [m vs]
  (let-mutable [m (transient m)]
    (doary [v vs]
      (set! m (assoc! m v nil)))
    (persistent! m)))

(defn construct-clojure-sorted-map [m vs]
  (let-mutable [m m]
    (doary [v vs]
      (set! m (assoc m v nil)))
    m))

(defn construct-clojure-set [s vs]
  (let-mutable [s (transient s)]
    (doary [v vs]
      (set! s (conj! s v)))
    (persistent! s)))

(defn construct-clojure-vector [v vs]
  (let-mutable [l (transient v)]
    (doary [v vs]
      (set! l (conj! l v)))
    (persistent! l)))

;;; Capsule

(defn union-capsule-maps [^Map$Immutable a ^Map$Immutable b]
  (let [m (.asTransient a)]
    (.__putAll m b)
    (.freeze m)))

(defn diff-capsule-maps [^Map$Immutable a ^Map$Immutable b]
  (let [m (.asTransient a)]
    (doit [k (.keySet b)]
      (.__remove m k))
    (.freeze m)))

(defn intersect-capsule-maps [^Map$Immutable a ^Map$Immutable b]
  (let [m (.asTransient a)]
    (doit [k (.keySet a)]
      (when-not (.containsKey b k)
        (.__remove m k)))
    (.freeze m)))

(defn construct-capsule-map [^Map$Immutable m ks]
  (let [val ""
        m   (.asTransient m)]
    (doary [k ks]
      (.__put ^Map$Transient m k val))
    (.freeze ^Map$Transient m)))

(defn construct-capsule-set [^Set$Immutable s vs]
  (let [s (.asTransient s)]
    (doary [v vs]
      (.__insert ^Set$Transient s v))
    (.freeze ^Set$Transient s)))

;;; PCollections

(defn construct-pcollections-map [^PMap m ks]
  (let [val ""]
    (let-mutable [m m]
      (doary [k ks]
        (set! m (.plus ^PMap m k val)))
      m)))

(defn construct-pcollections-set [^PSet s vs]
  (let-mutable [s s]
    (doary [v vs]
      (set! s (.plus ^PSet s v)))
    s))

(defn construct-pcollections-vector [^PVector v vs]
  (let-mutable [v v]
    (doary [x vs]
      (set! v (.plus ^PVector v x)))
    v))

(defn intersect-pcollections-maps [^PMap a ^PMap b]
  (let-mutable [a a]
    (doit [k (.keySet b)]
      (when-not (.containsKey ^PMap a k)
        (set! a (.minus ^PMap a k))))
    a))

(defn intersect-pcollections-sets [^PSet a ^PSet b]
  (let-mutable [a a]
    (doit [k b]
      (when-not (.contains ^PSet a k)
        (set! a (.minus ^PSet a k))))
    a))

;;; Paguro

(defn construct-paguro-map [^PersistentHashMap m ks]
  (let [val ""]
    (let-mutable [m (.mutable m)]
      (doary [k ks]
        (set! m (.assoc ^PersistentHashMap$MutableHashMap m k val)))
      (.immutable ^PersistentHashMap$MutableHashMap m))))

(defn construct-paguro-sorted-map [^PersistentTreeMap m ks]
  (let [val ""]
    (let-mutable [m m]
      (doary [k ks]
        (set! m (.assoc ^PersistentTreeMap m k val)))
      m)))

(defn construct-paguro-set [^PersistentHashSet s vs]
  (let-mutable [s (.mutable s)]
    (doary [v vs]
      (set! s (.put ^PersistentHashSet$MutableHashSet s v)))
    (.immutable ^PersistentHashSet$MutableHashSet s)))

(defn construct-paguro-vector [^RrbTree$MutableRrbt v vs]
  (let-mutable [v v]
    (doary [x vs]
      (set! v (.append ^RrbTree$MutableRrbt v x)))
    (.immutable ^RrbTree$MutableRrbt v)))

(defn intersect-paguro-map [^PersistentHashMap a ^PersistentHashMap b]
  (let-mutable [a (.mutable a)]
    (doit [k (.keySet b)]
      (when-not (.containsKey ^PersistentHashMap$MutableHashMap a k)
        (set! a (.without ^PersistentHashMap$MutableHashMap a k))))
    (.immutable ^PersistentHashMap$MutableHashMap a)))

(defn intersect-paguro-sorted-map [^PersistentTreeMap a ^PersistentTreeMap b]
  (let-mutable [a a]
    (doit [k (.keySet ^PersistentTreeMap a)]
      (when-not (.containsKey ^PersistentTreeMap b k)
        (set! a (.without ^PersistentTreeMap a k))))
    a))

(defn difference-paguro-map [^PersistentHashMap a ^PersistentHashMap b]
  (let-mutable [a (.mutable a)]
    (doit [k (.keySet b)]
      (set! a (.without ^PersistentHashMap$MutableHashMap a k)))
    (.immutable ^PersistentHashMap$MutableHashMap a)))

(defn difference-paguro-sorted-map [^PersistentTreeMap a ^PersistentTreeMap b]
  (let-mutable [a a]
    (doit [k (.keySet b)]
      (set! a (.without ^PersistentTreeMap a k)))
    a))

(defn union-paguro-map [^PersistentHashMap a ^PersistentHashMap b]
  (let-mutable [a (.mutable a)]
    (doit [e (.entrySet b)]
      (set! a
        (.assoc ^PersistentHashMap$MutableHashMap a
          (.getKey ^java.util.Map$Entry e)
          (.getValue ^java.util.Map$Entry e))))
    (.immutable ^PersistentHashMap$MutableHashMap a)))

(defn union-paguro-sorted-map [^PersistentTreeMap a ^PersistentTreeMap b]
  (let-mutable [a a]
    (doit [^java.util.Map$Entry e (.entrySet b)]
      (set! a (.assoc ^PersistentTreeMap a (.getKey e) (.getValue e))))
    a))

(defn intersect-paguro-set [^PersistentHashSet a ^PersistentHashSet b]
  (let-mutable [a (.mutable a)]
    (doit [k a]
      (when-not (.contains b k)
        (set! a (.without ^PersistentHashSet$MutableHashSet a k))))
    a))

(defn difference-paguro-set [^PersistentHashSet a ^PersistentHashSet b]
  (let-mutable [a (.mutable a)]
    (doit [k b]
      (set! a (.without ^PersistentHashSet$MutableHashSet a k)))
    a))

(defn union-paguro-set [^PersistentHashSet a ^PersistentHashSet b]
  (let-mutable [a (.mutable a)]
    (doit [k b]
      (set! a (.put ^PersistentHashSet$MutableHashSet a k)))
    a))

;;; Scala

(defn construct-scala-map [^scala.collection.mutable.Builder m ks]
  (let [val ""]
    (let-mutable [m m]
      (doary [k ks]
        (set! m (.$plus$eq ^scala.collection.mutable.Builder m (scala.Tuple2. k val))))
      (.result ^scala.collection.mutable.Builder m))))

(defn construct-scala-collection [^scala.collection.mutable.Builder m ks]
  (let-mutable [m m]
    (doary [k ks]
      (set! m (.$plus$eq ^scala.collection.mutable.Builder m k)))
    (.result ^scala.collection.mutable.Builder m)))

(defn consume-scala-iterator [^scala.collection.Iterator it]
  (loop [x nil]
    (if (.hasNext it)
      (recur (or (.next it) x))
      x)))

;;; Vavr

(defn construct-vavr-map [^io.vavr.collection.Map m ks]
  (let [val ""]
    (let-mutable [m m]
      (doary [k ks]
        (set! m (.put ^io.vavr.collection.Map m ^Object k val)))
      m)))

(defn construct-vavr-set [^io.vavr.collection.Set s vs]
  (let-mutable [s s]
    (doary [v vs]
      (set! s (.add ^io.vavr.collection.Set s v)))
    s))

(defn construct-vavr-vector [^io.vavr.collection.Vector v vs]
  (let-mutable [v v]
    (doary [x vs]
      (set! v (.append ^io.vavr.collection.Vector v x)))
    v))

;;; Strings / Ropes

(defn insert-string [^String a ^String b ^long idx]
  (let [idx (.offsetByCodePoints a 0 idx)]
    (.concat
      (.concat
        (.substring a 0 idx)
        b)
      (.substring a idx (.length a)))))

(defn remove-string [^String a ^long start ^long end]
  (let [start' (.offsetByCodePoints a 0 start)
        end'   (.offsetByCodePoints a start' (- end start))]
    (.concat
      (.substring a 0 start')
      (.substring a end' (.length a)))))

(defn concat-string [^String a ^String b]
  (.concat a b))

(defn insert-rope [^Rope a ^Rope b ^long idx]
  (.insert a idx b))

(defn remove-rope [^Rope a ^long start ^long end]
  (.remove a start end))

(defn concat-rope [^Rope a ^Rope b]
  (.concat a b))

;;;

;; a simple object that exists to provide minimal overhead within a hashmap
(deftype Obj [^int hash]
  Object
  (hashCode [_] (int hash))
  (equals [this o] (identical? this o)))

(defn generate-entries [n]
  (->> #(Obj. (rand-int Integer/MAX_VALUE)) (repeatedly n) into-array))

(defn generate-numbers [n]
  (->> n range shuffle into-array))

(defn generate-string [n]
  (let [is (->> [0 0x80 0x800 0x10000]
             cycle
             (take n)
             shuffle
             int-array)]
    (String. ^ints is 0 (int n))))

;;;

(defn base-collection [label class]
  {:label label
   :base (eval `(fn [] (new ~class)))
   :clone clone})

(defn base-map [label class]
  (merge
    (base-collection label class)
    {:construct construct-map
     :entries generate-entries
     :lookup lookup-map
     :consume consume-entry-iterator
     :iterator iterator
     :union map-union
     :difference map-difference
     :intersection map-intersection
     :split split
     :add #(.put ^IMap %1 %2 nil)
     :remove #(.remove ^IMap %1 %2)}))

(defn base-set [label class]
  (merge
    (base-collection label class)
    {:construct construct-set
     :entries generate-entries
     :lookup lookup-set
     :consume consume-iterator
     :iterator iterator
     :union set-union
     :difference set-difference
     :intersection set-intersection
     :split split
     :add #(.add ^ISet %1 %2)
     :remove #(.remove ^ISet %1 %2)}))

(defn base-list [label class]
  (merge
    (base-collection label class)
    {:construct construct-list
     :entries generate-numbers
     :lookup lookup-list
     :consume consume-iterator
     :iterator iterator
     :concat #(.concat ^IList %1 %2)
     :split split}))

;;; maps

(def linear-map (base-map "bifurcan.LinearMap" LinearMap))

(def bifurcan-map (base-map "bifurcan.Map" Map))

(def scala-map
  {:label        "scala.HashMap"
   :construct    construct-scala-map
   :base         #(.newBuilder (scala.collection.immutable.HashMap$/MODULE$))
   :entries      generate-entries
   :lookup       #(doary [k %2] (.get ^scala.collection.immutable.Map %1 k))
   :iterator     #(.iterator ^scala.collection.immutable.Map %)
   :consume      consume-scala-iterator
   :union        #(.$plus$plus ^scala.collection.immutable.Map %1 ^scala.collection.immutable.Map %2)
   :difference   #(.$minus$minus ^scala.collection.immutable.Map %1 (.keySet ^scala.collection.immutable.Map %2))
   :intersection (fn [a b] (.filter ^scala.collection.immutable.Map a ^scala.Function1 (scala-fn1 #(.contains ^scala.collection.immutable.Map b (._1 ^scala.Tuple2 %)))))
   :add          #(.$plus ^scala.collection.immutable.Map %1 (scala.Tuple2. %2 ""))
   :remove       #(.$minus ^scala.collection.immutable.Map %1 %2)})

(def pcollections-map
  {:label        "pcollections.HashTreePMap"
   :construct    construct-pcollections-map
   :base         #(HashTreePMap/empty)
   :entries      generate-entries
   :lookup       lookup-java-map
   :iterator     #(-> ^java.util.Map % .entrySet .iterator)
   :consume      consume-java-entry-iterator
   :union        #(.plusAll ^PMap %1 %2)
   :difference   #(.minusAll ^PMap %1 (.keySet ^PMap %2))
   :intersection intersect-pcollections-maps
   :add          #(.plus ^PMap %1 %2 "")
   :remove       #(.minus ^PMap %1 %2)})

(def paguro-map
  {:label        "paguro.PersistentHashMap"
   :construct    construct-paguro-map
   :base         #(PersistentHashMap/empty)
   :entries      generate-entries
   :lookup       #(doary [k %2] (.entry ^PersistentHashMap %1 k))
   :iterator     iterator
   :consume      consume-iterator
   :union        union-paguro-map
   :difference   difference-paguro-map
   :intersection intersect-paguro-map
   :add          #(.assoc ^PersistentHashMap %1 %2 "")
   :remove       #(.without ^PersistentHashMap %1 %2)})

(def capsule-map
  {:label        "capsule.PersistentTrieMap"
   :construct    construct-capsule-map
   :base         #(PersistentTrieMap/of)
   :entries      generate-entries
   :lookup       lookup-java-map
   :iterator     #(-> ^java.util.Map % .entrySet .iterator)
   :consume      consume-java-entry-iterator
   :union        union-capsule-maps
   :difference   diff-capsule-maps
   :intersection intersect-capsule-maps
   :add          #(.__put ^Map$Immutable %1 %2 "")
   :remove       #(.__remove ^Map$Immutable %1 %2)})

(def vavr-map
  {:label        "vavr.HashMap"
   :construct    construct-vavr-map
   :base         #(io.vavr.collection.HashMap/empty)
   :entries      generate-entries
   :lookup       #(doary [k %2] (.get ^io.vavr.collection.Map %1 k))
   :iterator     iterator
   :consume      consume-iterator
   :union        #(.merge ^io.vavr.collection.Map %1 %2)
   :difference   (fn [a b] (.removeKeys ^io.vavr.collection.Map a (predicate #(.containsKey ^io.vavr.collection.Map b %))))
   :intersection (fn [a b] (.removeKeys ^io.vavr.collection.Map a (predicate #(not (.containsKey ^io.vavr.collection.Map b %)))))
   :add          #(.put ^io.vavr.collection.Map %1 %2 "")
   :remove       #(.remove ^io.vavr.collection.Map %1 %2)})

(def java-hash-map
  (merge (base-collection "java.HashMap" HashMap)
    {:entries      generate-entries
     :construct    construct-java-map
     :lookup       lookup-java-map
     :iterator     #(-> ^java.util.Map % .entrySet .iterator)
     :consume      consume-java-entry-iterator
     :union        union-java-maps
     :difference   diff-java-maps
     :intersection intersect-java-maps
     :add          #(doto ^java.util.Map %1 (.put %2 nil))
     :remove       #(doto ^java.util.Map %1 (.remove %2))}))

(def clojure-map
  {:label        "clojure.PersistentHashMap"
   :base         (constantly {})
   :entries      generate-entries
   :construct    construct-clojure-map
   :lookup       #(doary [k %2] (get %1 k))
   :iterator     iterator
   :consume      consume-java-entry-iterator
   :union        merge
   :difference   #(persistent! (apply dissoc! (transient %1) (keys %2)))
   :intersection #(select-keys %1 (keys %2))
   :add          #(assoc %1 %2 nil)
   :remove       dissoc})

;;; sorted maps

(def bifurcan-sorted-map
  (merge
    (base-map "bifurcan.SortedMap" SortedMap)
    {:entries generate-numbers}))

(def int-map
  (merge
    (base-map "bifurcan.IntMap" IntMap)
    {:construct construct-int-map
     :lookup    lookup-int-map
     :entries   generate-numbers}))

(def scala-int-map
  (merge scala-map
    {:label   "scala.LongMap"
     :base    #(.newBuilder (scala.collection.immutable.LongMap$/MODULE$))
     :entries generate-numbers}))

(def scala-sorted-map
  (merge scala-int-map
    {:label "scala.TreeMap"
     :base  #(.newBuilder
               (scala.collection.immutable.TreeMap$/MODULE$)
               (reify scala.math.Ordering
                 (compare [_ a b]
                   (compare a b))))}))

(def vavr-sorted-map
  (merge vavr-map
    {:label   "vavr.TreeMap"
     :base    #(io.vavr.collection.TreeMap/empty)
     :entries generate-numbers}))

(def paguro-sorted-map
  (merge paguro-map
    {:label        "paguro.PersistentTreeMap"
     :construct    construct-paguro-sorted-map
     :base         #(PersistentTreeMap/empty)
     :lookup       #(doary [k %2] (.entry ^PersistentTreeMap %1 k))
     :entries      generate-numbers
     :union        union-paguro-sorted-map
     :difference   difference-paguro-sorted-map
     :intersection intersect-paguro-sorted-map
     :add          #(.assoc ^PersistentTreeMap %1 %2 "")
     :remove       #(.without ^PersistentTreeMap %1 %2)}))

(def java-sorted-map
  (merge java-hash-map
    {:label   "java.TreeMap"
     :entries generate-numbers
     :base    #(TreeMap.)}))

(def clojure-sorted-map
  (merge clojure-map
    {:label      "clojure.PersistentTreeMap"
     :base       sorted-map
     :construct  construct-clojure-sorted-map
     :entries    generate-numbers
     :difference #(apply dissoc %1 (keys %2))}))

;; set

(def linear-set (base-set "bifurcan.LinearSet" LinearSet))

(def bifurcan-set (base-set "bifurcan.Set" Set))

(def java-hash-set
  (merge (base-collection "java.HashSet" HashSet)
    {:construct    construct-hash-set
     :lookup       lookup-java-set
     :entries      generate-entries
     :iterator     iterator
     :consume      consume-iterator
     :union        #(doto ^HashSet (.clone ^HashSet %1) (.addAll %2))
     :difference   #(doto ^HashSet (.clone ^HashSet %1) (.removeAll %2))
     :intersection intersect-hash-sets
     :add          #(doto ^HashSet %1 (.add %2))
     :remove       #(doto ^HashSet %1 (.remove %2))}))

(def pcollections-set
  {:label        "pcollections.HashTreePSet"
   :construct    construct-pcollections-set
   :base         #(HashTreePSet/empty)
   :entries      generate-entries
   :lookup       lookup-java-set
   :iterator     #(.iterator ^Iterable %)
   :consume      consume-iterator
   :union        #(.plusAll ^PSet %1 ^java.util.Collection %2)
   :difference   #(.minusAll ^PSet %1 ^java.util.Collection %2)
   :intersection intersect-pcollections-sets
   :add          #(.plus ^PSet %1 %2)
   :remove       #(.minus ^PSet %1 %2)})

(def vavr-set
  {:label        "vavr.HashSet"
   :construct    construct-vavr-set
   :base         #(io.vavr.collection.HashSet/empty)
   :entries      generate-entries
   :lookup       #(doary [k %2] (.contains ^io.vavr.collection.Set %1 k))
   :iterator     #(.iterator ^Iterable %)
   :consume      consume-iterator
   :union        #(.union ^io.vavr.collection.Set %1 %2)
   :difference   #(.diff ^io.vavr.collection.Set %1 %2)
   :intersection #(.intersect ^io.vavr.collection.Set %1 %2)
   :add          #(.add ^io.vavr.collection.Set %1 %2)
   :remove       #(.remove ^io.vavr.collection.Set %1 %2)})

(def scala-set
  {:label        "scala.HashSet"
   :construct    construct-scala-collection
   :base         #(.newBuilder (scala.collection.immutable.HashSet$/MODULE$))
   :entries      generate-entries
   :lookup       #(doary [k %2] (.contains ^scala.collection.immutable.Set %1 k))
   :iterator     #(.iterator ^scala.collection.immutable.Set %)
   :consume      consume-scala-iterator
   :union        #(.union ^scala.collection.immutable.Set %1 ^scala.collection.immutable.Set %2)
   :difference   #(.diff ^scala.collection.immutable.Set %1 ^scala.collection.immutable.Set %2)
   :intersection #(.$amp ^scala.collection.immutable.Set %1 %2)
   :add          #(.$plus ^scala.collection.immutable.Set %1 %2)
   :remove       #(.$minus ^scala.collection.immutable.Set %1 %2)})

(def paguro-set
  {:label        "paguro.PersistentHashSet"
   :construct    construct-paguro-set
   :base         #(PersistentHashSet/empty)
   :entries      generate-entries
   :lookup       #(doary [k %2] (.contains ^PersistentHashSet %1 k))
   :iterator     iterator
   :consume      consume-iterator
   :union        union-paguro-set
   :difference   difference-paguro-set
   :intersection intersect-paguro-set
   :add          #(.put ^PersistentHashSet %1 %2)
   :remove       #(.without ^PersistentHashSet %1 %2)})

(def capsule-set
  {:label        "capsule.PersistentTrieSet"
   :construct    construct-capsule-set
   :base         #(PersistentTrieSet/of)
   :entries      generate-entries
   :lookup       lookup-java-set
   :iterator     iterator
   :consume      consume-iterator
   :union        #(.__insertAll ^Set$Immutable %1 %2)
   :difference   #(.__removeAll ^Set$Immutable %1 %2)
   :intersection #(.__retainAll ^Set$Immutable %1 %2)
   :add          #(.__insert ^Set$Immutable %1 %2)
   :remove       #(.__remove ^Set$Immutable %1 %2)})

(def clojure-set
  {:label        "clojure.PersistentHashSet"
   :base         (constantly #{})
   :construct    construct-clojure-set
   :entries      generate-entries
   :iterator     iterator
   :consume      consume-iterator
   :lookup       #(doary [x %2] (contains? %1 x))
   :union        #(into %1 %2)
   :difference   #(persistent! (apply disj! (transient %1) %2))
   :intersection #(persistent!
                    (reduce
                      (fn [result item]
                        (if (contains? %2 item)
                          result
                          (disj! result item)))
                      (transient %1) %1))
   :add          conj
   :remove       disj})

;; lists

(def linear-list (base-list "bifurcan.LinearList" LinearList))

(def bifurcan-list (base-list "bifurcan.List" List))

(def java-array-list
  {:label     "java.ArrayList"
   :base      #(ArrayList.)
   :entries   generate-numbers
   :construct construct-java-list
   :lookup    lookup-java-list
   :iterator  iterator
   :consume   consume-iterator
   :concat    #(doto ^ArrayList (.clone ^ArrayList %) (.addAll %2))})

(def pcollections-vector
  {:label     "pcollections.TreePVector"
   :base      #(TreePVector/empty)
   :entries   generate-numbers
   :construct construct-pcollections-vector
   :iterator  iterator
   :consume   consume-iterator
   :lookup    lookup-java-list
   :concat    #(.plusAll ^PVector %1 ^java.util.Collection %2)})

(def paguro-vector
  {:label     "paguro.RrbTree"
   :base      #(RrbTree/emptyMutable)
   :entries   generate-numbers
   :construct construct-paguro-vector
   :iterator  iterator
   :consume   consume-iterator
   :lookup    #(doary [k %2] (.get ^RrbTree %1 (int k)))
   :concat    #(.join ^RrbTree %1 ^RrbTree %2)})

(def scala-vector
  {:label     "scala.Vector"
   :base      #(.newBuilder (scala.collection.immutable.Vector$/MODULE$))
   :entries   generate-numbers
   :construct construct-scala-collection
   :iterator  #(.iterator ^scala.collection.immutable.Vector %)
   :consume   consume-scala-iterator
   :lookup    #(doary [k %2] (.apply ^scala.collection.immutable.Vector %1 k))
   :concat    #(.$plus$plus ^scala.collection.immutable.Vector %1 %2)})

(def vavr-vector
  {:label     "vavr.Vector"
   :base      #(io.vavr.collection.Vector/empty)
   :entries   generate-numbers
   :construct construct-vavr-vector
   :iterator  iterator
   :consume   consume-iterator
   :lookup    #(doary [k %2] (.get ^io.vavr.collection.Vector %1 k))
   :concat    #(.appendAll ^io.vavr.collection.Vector %1 ^Iterable %2)})

(def clojure-vector
  {:label     "clojure.PersistentVector"
   :base      (constantly [])
   :entries   generate-numbers
   :construct construct-clojure-vector
   :iterator  iterator
   :consume   consume-iterator
   :lookup    #(doary [i %2] (nth %1 i))
   :concat    #(into %1 %2)})

;; strings

(def java-string
  {:label     "java.String"
   :base      (constantly "")
   :lookup    #(doary-int [i %2]
                 (.codePointAt ^String %1 (.offsetByCodePoints ^String %1 0 i)))
   :indices   range
   :entries   generate-string
   :construct concat-string
   :consume   consume-int-iterator
   :iterator  #(.iterator (.codePoints ^String %))
   :concat    concat-string
   :remove    remove-string
   :insert    insert-string})

(def rope
  {:label     "bifurcan.Rope"
   :base      (constantly (Rope/from ""))
   :lookup    #(doary-int [i %2]
                 (.nth ^Rope %1 i))
   :indices   range
   :entries   generate-string
   :construct #(.concat ^Rope %1 (Rope/from %2))
   :consume   consume-int-iterator
   :iterator  #(.codePoints ^Rope %)
   :concat    concat-rope
   :remove    remove-rope
   :insert    insert-rope})

;;;

(def ^:dynamic *warmup* false)

(defn benchmark [n f]
  (binding [c/*final-gc-problem-threshold* 0.1]
    (-> (c/quick-benchmark* f
          (merge
            {:samples               10
             :target-execution-time 1e8}
            (if *warmup*
              {:samples               6
               :warmup-jit-period     1e10
               :target-execution-time 1e9}
              {:warmup-jit-period 1e8})))
      :mean
      first
      (* 1e9)
      long)))

(defn benchmark-construct [n {:keys [base entries construct]}]
  (let [s (entries n)]
    (benchmark n #(do (construct (base) s) nil))))

(defn benchmark-lookup [n {:keys [base indices entries construct lookup]}]
  (let [s (entries n)
        c (construct (base) s)
        s (if indices
            (-> n indices shuffle int-array)
            s)]
    (benchmark n #(lookup c s))))

(defn benchmark-clone [n {:keys [base entries construct clone]}]
  (let [c (construct (base) (entries n))]
    (benchmark n #(do (clone c) nil))))

(defn benchmark-iteration [n {:keys [base entries construct iterator consume] :as m}]
  (let [c (construct (base) (entries n))]
    (benchmark n (consume (iterator c)))))

(defn benchmark-iteration [n {:keys [base entries construct iterator consume] :as m}]
  (let [c (construct (base) (entries n))]
    (benchmark n #(consume (iterator c)))))

(defn benchmark-concat [n {:keys [base entries construct concat]}]
  (let [init (construct (base) (entries 0))
        a (construct (base) (entries (/ n 2)))
        b (construct (base) (entries (/ n 2)))]
    (benchmark n #(do (-> init (concat a) (concat b)) nil))))

(defn benchmark-insert [n {:keys [base entries construct insert]}]
  (let [s       (construct (base) (entries n))
        s'      (construct (base) (entries 1))
        indices (->> n range shuffle int-array)]
    (benchmark n
      #(let-mutable [s s]
         (doary-int [i indices]
           (set! s (insert s s' (long i))))))))

(defn benchmark-remove [^long n {:keys [base entries construct remove]}]
  (let [s       (construct (base) (entries n))
        indices (->> n range shuffle int-array)]
    (benchmark n
      #(doary-int [i indices]
         (remove s (long i) (unchecked-inc (long i)))))))

(defn benchmark-equals [n {:keys [label base entries construct add remove iterator]}]
  (let [n        (long n)
        s        (entries n)
        a        (construct (base) s)
        b        (construct (base) s)
        int-map? (= entries generate-numbers)]
    (benchmark n
      #(let [e  (aget ^objects s (rand-int n))
             e' (if int-map?
                  (long (rand-int Integer/MAX_VALUE))
                  (Obj. (rand-int Integer/MAX_VALUE)))
             b  (-> b (remove e) (add e'))]
         (.equals ^Object a b)
         (-> b (remove e') (add e))
         nil))))

(defn benchmark-union [n {:keys [base entries construct union clone]}]
  (let [e (entries (Math/ceil (* n 1.5)))
        s-a (->> e (take n) into-array)
        s-b (->> e (drop (/ n 2)) (take n) into-array)
        a   (construct (base) s-a)
        b   (construct (base) s-b)]
    (benchmark n #(do (union a b) nil))))

(defn benchmark-difference [n {:keys [base entries construct difference clone]}]
  (let [e (entries (Math/ceil (* n 1.5)))
        s-a (->> e (take n) into-array)
        s-b (->> e (drop (/ n 2)) (take n) into-array)
        a (construct (base) s-a)
        b (construct (base) s-b)]
    (benchmark n #(do (difference a b) nil))))

(defn benchmark-intersection [n {:keys [base entries construct intersection clone]}]
  (let [e (entries (Math/ceil (* n 1.5)))
        s-a (->> e (take n) into-array)
        s-b (->> e (drop (/ n 2)) (take n) into-array)
        a (construct (base) s-a)
        b (construct (base) s-b)]
    (benchmark n #(do (intersection a b) nil))))

;;;

(def maps [bifurcan-map java-hash-map clojure-map vavr-map scala-map paguro-map linear-map capsule-map #_pcollections-map])

(def sets [bifurcan-set java-hash-set clojure-set vavr-set scala-set paguro-set linear-set capsule-set #_pcollections-set])

(def sorted-maps [bifurcan-sorted-map java-sorted-map clojure-sorted-map vavr-sorted-map scala-sorted-map paguro-sorted-map int-map scala-int-map])

(def lists [bifurcan-list java-array-list clojure-vector vavr-vector scala-vector paguro-vector linear-list #_pcollections-vector])

(def strings [java-string rope])

(def all-colls (concat maps sorted-maps sets lists strings))

(def bench->types
  {:construct    [benchmark-construct
                  (-> all-colls set (disj java-string))]
   :lookup       [benchmark-lookup
                  all-colls]
   :clone        [benchmark-clone
                  [linear-map java-hash-map linear-set java-hash-set java-sorted-map]]
   :iteration    [benchmark-iteration
                  all-colls]
   :concat       [benchmark-concat
                  (concat lists strings)]
   :union        [benchmark-union
                  (concat maps sets sorted-maps)]
   :difference   [benchmark-difference
                  (concat maps sets sorted-maps)]
   :intersection [benchmark-intersection
                  (concat maps sets sorted-maps)]
   :equals       [benchmark-equals
                  (concat maps sets sorted-maps)]
   :insert       [benchmark-insert
                  strings]
   :remove       [benchmark-remove
                  strings]
   })

(defn run-benchmarks [n coll]
  (let [bench->types bench->types #_(select-keys bench->types [:iteration])]
    (println "benchmarking:" n)
    (->> bench->types
      (map (fn [[k [f colls]]] [k (when (-> colls set (contains? coll)) (f n coll))]))
      (into {}))))

(defn run-benchmark-suite [n log-step coll]
  (let [n     (if (= coll java-string)
                (min 1e4 n)
                n)
        sizes (->> (u/log-steps n 10 log-step)
                (drop log-step)
                (map long))]
    (prn (:label coll) sizes)
    (println "warming up...")
    (binding [*warmup* true]
      (run-benchmarks 10 coll))
    (println "warmed up")
    (zipmap sizes (map #(run-benchmarks % coll) sizes))))

(defn validate []
  (with-redefs [benchmark (fn [_ f] (f))]
    (doseq [c all-colls]
      (run-benchmarks 10 c))))

;;;

(defn extract-csv [coll->n->benchmark->nanos benchmark colls scale]
  (let [sizes          (->> coll->n->benchmark->nanos vals (map keys) (sort-by count) last sort)
        coll->n->nanos (->> colls
                         (map :label)
                         (select-keys coll->n->benchmark->nanos)
                         (map (fn [[coll n->benchmark->nanos]]
                                [coll
                                 (zipmap
                                   sizes
                                   (->> sizes
                                     (map #(get n->benchmark->nanos %))
                                     (map #(get % benchmark))))]))
                         (into {}))]
    (apply str
      "size," (->> colls (map :label) (interpose ",") (apply str)) "\n"
      (->> sizes
        (map
          (fn [size]
            (->> colls
              (map :label)
              (map #(get-in coll->n->nanos [% size]))
              (map #(scale % size))
              (interpose ",")
              (apply str size ","))))
        (#(interleave % (repeat "\n")))
        (apply str)))))

(def benchmark-csv
  {"clone" [:clone [linear-map linear-set java-hash-map java-hash-set]]

   "sorted_map_construct" [:construct sorted-maps]
   "map_construct"        [:construct maps]
   "list_construct"       [:construct lists]

   "sorted_map_lookup" [:lookup sorted-maps]
   "map_lookup"        [:lookup maps]
   "list_lookup"       [:lookup lists]

   "sorted_map_iterate" [:iteration sorted-maps]
   "map_iterate"        [:iteration maps]
   "list_iterate"       [:iteration lists]

   "string_construct" [:construct strings]
   "string_lookup"    [:lookup strings]
   "string_insert"    [:insert strings]
   "string_remove"    [:remove strings]
   "string_concat"    [:concat strings]
   "string_iterate"   [:iteration strings]

   "concat" [:concat lists]

   "set_union"        [:union sets]
   "set_difference"   [:difference sets]
   "set_intersection" [:intersection sets]

   "map_union"        [:union maps]
   "map_difference"   [:difference maps]
   "map_intersection" [:intersection maps]

   "sorted_map_union"        [:union sorted-maps]
   "sorted_map_difference"   [:difference sorted-maps]
   "sorted_map_intersection" [:intersection sorted-maps]

   "sorted_map_equals" [:equals sorted-maps]
   "map_equals"        [:equals maps]
   "set_equals"        [:equals sets]

   })

(defn write-out-csvs [descriptor]
  (doseq [[file [benchmark colls]] benchmark-csv]
    (spit (str "benchmarks/data/" file ".csv")
      (extract-csv descriptor benchmark colls
        (fn [x n] (when x (float (/ x n))))))))

;;;

(defn benchmark-collection [n step idx]
  (let [result (-> (sh/sh "sh" "-c"
                     (str "lein with-profile dev,bench run -m bifurcan.benchmark-test benchmark-collection " n " " step " " idx))
                 :out
                 bs/to-string)]
    (try
      (let [x (-> result
                bs/to-line-seq
                last
                read-string)]
        (if (map? x)
          x
          (do
            (println "invalid benchmark for" (-> all-colls (nth idx) :label))
            (println result))))
      (catch Throwable e
        (throw e)))))

(defn -main [task & args]
  (case task
    "benchmark-collection"
    (let [[n step idx] args]
      (try
        (prn
          (run-benchmark-suite
            (read-string n)
            (read-string step)
            (nth all-colls (read-string idx))))
        (catch Throwable e
          (.printStackTrace e System/out))))

    "benchmark"
    (let [[n step]   args
          descriptor (->> (range (count all-colls))
                       (map (fn [idx]
                              (when ((constantly true)
                                      (nth all-colls idx))
                                (let [coll (-> all-colls (nth idx) :label)]
                                  (println "benchmarking" coll)
                                  [coll (benchmark-collection (or n "1e6") (or step "1") idx)]))))
                       (into {}))

          ;; merge with the existing benchmarks, if we didn't regenerate all of them
          descriptor (merge-with #(merge-with merge %1 %2)
                       (try
                         (read-string
                           (slurp "benchmarks/data/benchmarks.edn"))
                         (catch Throwable e
                           nil))
                       descriptor)]

      (spit "benchmarks/data/benchmarks.edn" (pr-str descriptor))

      (write-out-csvs descriptor)))

  (flush)
  (Thread/sleep 100)
  (System/exit 0))
