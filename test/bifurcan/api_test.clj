(ns bifurcan.api-test
  "Covers public methods which no other test exercises, and the parts of the
  custom hash/equality contract which aren't checked anywhere else. Most of
  these are small enough not to need generative testing; where the method has
  interesting structure we compare against a Clojure equivalent over generated
  input."
  (:require [bifurcan.invariant :as inv]
            [bifurcan.test-utils :as u]
            [clojure [datafy :refer [datafy]]
                     [test :refer [deftest is testing]]]
            [clojure.test.check [generators :as gen]
                                [properties :as prop]
                                [clojure-test :refer [defspec]]])
  (:import (java.util.function BiPredicate
                               ToLongFunction)
           (io.lacuna.bifurcan DirectedAcyclicGraph
                               DirectedGraph
                               FloatMap
                               Graph
                               IGraph
                               IList
                               IMap
                               ISet
                               IntMap
                               IntSet
                               LinearList
                               LinearMap
                               LinearSet
                               List
                               Lists
                               Map
                               Rope
                               Set
                               Sets
                               SortedMap
                               SortedSet)))

(def iterations (/ u/iterations 100))

;; # Custom hash and equality
;;
;; A collection built with custom semantics must *honor* them, not merely
;; carry them around. Here we use a hash and equality pair which treats a
;; keyword and a string with the same name as the same key, so a collection
;; which ignores them will report duplicates.

(def by-name-hash
  (u/->to-long-fn #(hash (name %))))

(def by-name-equality
  (u/->bi-predicate #(= (name %1) (name %2))))

(defn by-name-set []
  (Set. by-name-hash by-name-equality))

(defn by-name-map []
  (Map. by-name-hash by-name-equality))

(deftest custom-equality-map-test
  (testing "keys which are equal under the custom predicate collapse"
    (let [m (-> (by-name-map) (.put :x 1) (.put "x" 2))]
      (is (= 1 (.size m)))
      (is (= 2 (.get m :x nil)))
      (is (= 2 (.get m "x" nil)))
      (is (inv/valid? m))))

  (testing "the semantics survive set operations"
    (let [a (-> (by-name-map) (.put :x 1) (.put :y 2))
          b (-> (by-name-map) (.put "x" 9))]
      (is (= 2 (.size (.union a b))))
      (is (= 1 (.size (.intersection a ^IMap b))))
      (is (= 1 (.size (.difference a ^IMap b))))))

  (testing "the functions are preserved across operations"
    (let [m (-> (by-name-map) (.put :x 1) .forked)]
      (is (identical? by-name-hash (.keyHash m)))
      (is (identical? by-name-equality (.keyEquality m)))
      (is (identical? by-name-hash (.keyHash (.put m :y 2))))
      (is (identical? by-name-hash (.keyHash (.remove m :x)))))))

(deftest custom-equality-set-test
  (testing "elements which are equal under the custom predicate collapse"
    (let [s (-> (by-name-set) (.add :x) (.add "x"))]
      (is (= 1 (.size s)))
      (is (.contains s :x))
      (is (.contains s "x"))
      (is (inv/valid? s))))

  (testing "the functions are preserved"
    (let [s (-> (by-name-set) (.add :x) .forked)]
      (is (identical? by-name-hash (.valueHash s)))
      (is (identical? by-name-equality (.valueEquality s))))))

(deftest custom-equality-graph-test
  ;; This is issue #48: vertices which are equal under the custom predicate
  ;; should be the same vertex. Note that we can't use :x -> :y plus "y" -> "x"
  ;; on a DAG, because under these semantics that really is a cycle.
  (doseq [[name g] [["Graph" (Graph. by-name-hash by-name-equality)]
                    ["DirectedGraph" (DirectedGraph. by-name-hash by-name-equality)]]]
    (testing name
      (let [g (-> g (.link :x :y) (.link "y" "x"))]
        (is (= 2 (.size (.vertices g)))))))

  (testing "DirectedAcyclicGraph"
    (let [g (-> (DirectedAcyclicGraph. by-name-hash by-name-equality)
                (.link :x :y)
                (.link "x" :z))]
      (is (= 3 (.size (.vertices g))))))

  (testing "a cycle under the custom equality is still a cycle"
    (is (thrown? Exception
          (-> (DirectedAcyclicGraph. by-name-hash by-name-equality)
              (.link :x :y)
              (.link "y" "x"))))))

;; # mapValues and update

(def gen-int-map
  (gen/fmap #(zipmap % %) (gen/vector gen/large-integer 0 32)))

(defn map-impls
  "Builds one of each map type from a Clojure map of integers to integers."
  [m]
  {"Map"       (Map/from ^java.util.Map m)
   "IntMap"    (IntMap/from ^java.util.Map m)
   "SortedMap" (SortedMap/from ^java.util.Map m)
   "LinearMap" (LinearMap/from ^java.util.Map m)
   "FloatMap"  (FloatMap/from ^java.util.Map (zipmap (map double (keys m)) (vals m)))})

(defn- attempt
  "Runs a thunk, returning its value, or the exception it threw. Some of the
  methods below are broken in ways that throw rather than return the wrong
  answer, and a thrown exception aborts the whole generative run."
  [f]
  (try (f) (catch Throwable t t)))

(defn- describe
  "Renders a value, or a thrown exception, in one line."
  [x]
  (if (instance? Throwable x)
    (str "threw " (.getName (class x)) ": " (.getMessage ^Throwable x))
    (pr-str x)))

(defn- expect
  "Compares a value against what a method produced, printing a note when they
  differ so that a generative failure is legible."
  [label expected actual]
  (or (= expected actual)
      (do (println label "expected" (describe expected) "but got" (describe actual))
          false)))

(defspec mapValues-test iterations
  (prop/for-all [m gen-int-map]
    (every?
      (fn [[name ^IMap b]]
        (let [expected (if (= "FloatMap" name)
                         (zipmap (map double (keys m)) (map inc (vals m)))
                         (zipmap (keys m) (map inc (vals m))))
              actual   (attempt #(datafy (.mapValues b (u/->bi-fn (fn [_ v] (inc v))))))]
          (expect (str name " mapValues") expected actual)))
      (map-impls m))))

(defspec mapValues-sees-every-key-test iterations
  ;; The function must be called once per entry, with the real key and value.
  (prop/for-all [m gen-int-map]
    (every?
      (fn [[name ^IMap b]]
        (let [seen     (atom {})
              _        (attempt #(.mapValues b (u/->bi-fn (fn [k v] (swap! seen assoc k v) v))))
              expected (if (= "FloatMap" name)
                         (zipmap (map double (keys m)) (vals m))
                         m)]
          (expect (str name " mapValues visited") expected @seen)))
      (map-impls m))))

(deftest update-test
  (doseq [[name ^IMap m] (map-impls {1 10, 3 30})]
    (testing name
      (let [k      (if (= "FloatMap" name) 1.0 1)
            absent (if (= "FloatMap" name) 7.0 7)]
        (testing "an existing key sees its value"
          (is (= 11 (.get (.update m k (u/->unary-operator inc)) k nil))))
        (testing "a missing key sees nil"
          (is (= [nil] (let [seen (atom [])]
                         (.update m absent (u/->unary-operator
                                             (fn [v] (swap! seen conj v) 1)))
                         @seen))))
        (testing "a missing key is added"
          (is (= 1 (.get (.update m absent (u/->unary-operator (fn [_] 1))) absent nil))))))))

;; # zip

(deftest zip-test
  (doseq [[name ^ISet s] [["Set" (Set/from ^Iterable [1 2 3])]
                          ["SortedSet" (reduce #(.add ^ISet %1 %2) (SortedSet.) [1 2 3])]
                          ["LinearSet" (LinearSet/from ^java.util.Collection [1 2 3])]]]
    (testing name
      (is (= {1 2, 2 3, 3 4} (datafy (.zip s (u/->fn inc))))))))

;; # Graphs

(defn ->digraph [edges]
  (reduce (fn [^IGraph g [from to]] (.link g from to)) (DirectedGraph.) edges))

(defn ->dag [edges]
  (reduce (fn [^IGraph g [from to]] (.link g from to)) (DirectedAcyclicGraph.) edges))

(deftest dag-top-bottom-test
  ;; Issue #49: `bottom` should be the vertices with no outgoing edges, and
  ;; `top` the vertices with no incoming edges.
  (let [g (->dag [[:a1 :b] [:a2 :b] [:b :c1] [:b :c2] [:a3 :c3]])]
    (is (= #{:a1 :a2 :a3} (datafy (.top g))))
    (is (= #{:c1 :c2 :c3} (datafy (.bottom g))))))

(deftest dag-remove-top-bottom-test
  ;; Issue #46: removing a vertex should remove it from top and bottom.
  (let [g (-> (->dag [[:a :b] [:b :c]]) (.remove :a))]
    (is (= #{:b} (datafy (.top g))))
    (is (= #{:c} (datafy (.bottom g))))
    (is (not (contains? (datafy (.top g)) :a)))
    (is (not (contains? (datafy (.bottom g)) :a)))))

(defspec dag-top-bottom-agree-with-edges-test iterations
  ;; Whatever the implementation, top and bottom must agree with the edges.
  (prop/for-all [edges (gen/vector (gen/tuple (gen/choose 0 8) (gen/choose 0 8)) 0 16)]
    (let [acyclic (->> edges (filter (fn [[a b]] (< a b))) vec)
          g       (->dag acyclic)
          vs      (datafy (.vertices g))
          out     (fn [v] (datafy (.out g v)))
          in      (fn [v] (datafy (.in g v)))]
      (and (= (set (filter #(empty? (in %)) vs)) (datafy (.top g)))
           (= (set (filter #(empty? (out %)) vs)) (datafy (.bottom g)))))))

(deftest transpose-test
  (testing "a directed graph reverses"
    (let [g (.transpose (->digraph [[:a :b] [:b :c]]))]
      (is (= #{:a} (datafy (.out g :b))))
      (is (= #{:b} (datafy (.out g :c))))
      (is (= #{} (datafy (.out g :a))))))

  (testing "an undirected graph is its own transpose"
    (let [g (-> (Graph.) (.link :a :b))]
      (is (= (datafy g) (datafy (.transpose g)))))))

(defspec transpose-twice-test iterations
  (prop/for-all [edges (gen/vector (gen/tuple (gen/choose 0 8) (gen/choose 0 8)) 0 16)]
    (let [g (->digraph edges)]
      (= (datafy g) (datafy (.transpose (.transpose g)))))))

(deftest map-edges-test
  (doseq [[name ^IGraph g] [["Graph" (-> (Graph.) (.link :a :b 2))]
                            ["DirectedGraph" (-> (DirectedGraph.) (.link :a :b 2))]
                            ["DirectedAcyclicGraph" (-> (DirectedAcyclicGraph.) (.link :a :b 2))]]]
    (testing name
      (let [g' (.mapEdges g (u/->fn (fn [e] (* 10 (.value e)))))]
        (is (= 20 (.edge g' :a :b)))
        (is (= (datafy (.vertices g)) (datafy (.vertices g'))))))))

(deftest unlink-test
  (doseq [[name ^IGraph g] [["Graph" (-> (Graph.) (.link :a :b) (.link :b :c))]
                            ["DirectedGraph" (-> (DirectedGraph.) (.link :a :b) (.link :b :c))]
                            ["DirectedAcyclicGraph" (-> (DirectedAcyclicGraph.) (.link :a :b) (.link :b :c))]]]
    (testing name
      (let [g' (.unlink g :a :b)]
        (is (not (contains? (datafy (.out g' :a)) :b)))
        (is (contains? (datafy (.out g' :b)) :c))
        (testing "the vertices remain"
          (is (= #{:a :b :c} (datafy (.vertices g')))))))))

(deftest is-directed-test
  (is (not (.isDirected (Graph.))))
  (is (.isDirected (DirectedGraph.)))
  (is (.isDirected (DirectedAcyclicGraph.))))

;; # containsAll and containsAny

(deftest contains-all-any-test
  (testing "LinearMap against a set of keys"
    (let [m (LinearMap/from ^java.util.Map {1 :a, 2 :b})]
      (is (.containsAll m (Set/from ^Iterable [1 2])))
      (is (not (.containsAll m (Set/from ^Iterable [1 3]))))
      (is (.containsAny m (Set/from ^Iterable [1 3])))
      (is (not (.containsAny m (Set/from ^Iterable [3 4]))))))

  (testing "LinearMap against another map"
    (let [m (LinearMap/from ^java.util.Map {1 :a, 2 :b})]
      (is (.containsAll m (LinearMap/from ^java.util.Map {1 :x})))
      (is (not (.containsAll m (LinearMap/from ^java.util.Map {3 :x}))))
      (is (.containsAny m (LinearMap/from ^java.util.Map {1 :x, 3 :y})))
      (is (not (.containsAny m (LinearMap/from ^java.util.Map {3 :x}))))))

  (testing "the java.util views"
    (is (.containsAll (.toSet (Set/from ^Iterable [1 2 3])) [1 2]))
    (is (not (.containsAll (.toSet (Set/from ^Iterable [1 2])) [1 3])))
    (is (.containsAll (.toList (List/from ^Iterable [1 2 3])) [1 2]))
    (is (not (.containsAll (.toList (List/from ^Iterable [1 2])) [1 3])))))

;; # LinearList popFirst and popLast

(deftest pop-test
  (let [l (LinearList/from ^java.util.Collection [1 2 3])]
    (is (= 1 (.popFirst l)))
    (is (= 3 (.popLast l)))
    (is (= [2] (datafy l))))

  (testing "an empty list throws"
    (is (thrown? Exception (.popFirst (LinearList.))))
    (is (thrown? Exception (.popLast (LinearList.))))))

;; # Sorted index lookups

(defspec sorted-index-test iterations
  (prop/for-all [ks (gen/vector (gen/choose 0 1000) 1 32)]
    (let [ks     (vec (distinct ks))
          sorted (vec (sort ks))
          m      (reduce (fn [^IMap m k] (.put m k k)) (SortedMap.) ks)
          s      (reduce (fn [^ISet s k] (.add s k)) (SortedSet.) ks)]
      (every?
        (fn [k]
          ;; for a key which is present, all three indices are its own
          (let [rank (count (take-while #(< % k) sorted))]
            (and
              (= (.indexOf m k) (.inclusiveFloorIndex m k))
              (= (.indexOf s k) (.inclusiveFloorIndex s k))
              (= (.indexOf m k) (.ceilIndex m k))
              (= (.indexOf s k) (.ceilIndex s k))
              (= rank (.. m (indexOf k) getAsLong))
              (= rank (.. s (indexOf k) getAsLong)))))
        ks))))

(defspec sorted-index-absent-test iterations
  ;; For a key which isn't present, inclusiveFloorIndex is the rank of the
  ;; largest smaller key, and ceilIndex the rank of the smallest larger one.
  (prop/for-all [ks (gen/vector (gen/choose 0 100) 1 32)
                 k  (gen/choose 0 100)]
    (let [ks     (vec (distinct (remove #{k} ks)))
          sorted (vec (sort ks))
          m      (reduce (fn [^IMap m x] (.put m x x)) (SortedMap.) ks)
          below  (count (take-while #(< % k) sorted))
          floor  (if (zero? below) nil (dec below))
          ceil   (if (= below (count sorted)) nil below)]
      (and
        (= floor (let [o (.inclusiveFloorIndex m k)]
                   (when (.isPresent o) (.getAsLong o))))
        (= ceil (let [o (.ceilIndex m k)]
                  (when (.isPresent o) (.getAsLong o))))))))

(deftest comparator-test
  (is (some? (.comparator (SortedMap.))))
  (is (some? (.comparator (SortedSet.))))
  (testing "the comparator orders the collection"
    (let [m (reduce (fn [^IMap m k] (.put m k k)) (SortedMap.) [3 1 2])]
      (is (= [1 2 3] (mapv #(.key %) (iterator-seq (.iterator m))))))))

;; # clear

(deftest clear-test
  (is (= 0 (.size (.clear (LinearList/from ^java.util.Collection [1 2 3])))))
  (is (= 0 (.size (.clear (LinearSet/from ^java.util.Collection [1 2 3])))))
  (is (= 0 (.size (.clear (LinearMap/from ^java.util.Map {1 2}))))))

;; # Rope

(def gen-string
  (gen/fmap #(apply str %)
            (gen/vector (gen/elements ["a" "é" "😀" "中" "z"]) 0 256)))

(defn- code-points [^String s]
  (vec (.toArray (.codePoints s))))

(defspec rope-char-test iterations
  (prop/for-all [s gen-string]
    (let [r  ^Rope (Rope/from s)
          cs (.toCharSequence r)]
      (and
        (inv/valid? r)
        ;; size is in code points, the CharSequence is in code units
        (= (count (code-points s)) (.size r))
        (= (count s) (.length cs))
        (= s (str cs))
        (= s (.toString r))
        (every? #(= (.charAt s %) (.charAt cs %)) (range (count s)))
        ;; chars() walks code units, codePoints() walks code points
        (= (vec (map int s)) (vec (iterator-seq (.chars r))))
        (= (code-points s) (vec (iterator-seq (.codePoints r))))
        (= (reverse (map int s)) (vec (iterator-seq (.reverseChars r))))
        (= (reverse (code-points s)) (vec (iterator-seq (.reverseCodePoints r))))))))

(defspec rope-nth-test iterations
  (prop/for-all [s gen-string]
    (let [r   ^Rope (Rope/from s)
          cps (code-points s)]
      (every? #(= (nth cps %) (.nth r %)) (range (count cps))))))

(defspec rope-slice-test iterations
  ;; slice takes code point indices
  (prop/for-all [s gen-string
                 a gen/nat
                 b gen/nat]
    (let [cps   (code-points s)
          n     (count cps)
          [i j] (sort [(mod a (inc n)) (mod b (inc n))])
          r     ^Rope (Rope/from s)
          slice (.slice r i j)]
      (and
        (inv/valid? slice)
        (= (subvec cps i j) (code-points (.toString slice)))))))

(defspec rope-bytes-round-trip-test iterations
  (prop/for-all [s gen-string]
    ;; bytes() yields the UTF-8 encoding, in chunks
    (let [buffers (iterator-seq (.bytes ^Rope (Rope/from s)))
          total   (reduce + (map #(.remaining ^java.nio.ByteBuffer %) buffers))
          out     (byte-array total)]
      (loop [bs buffers, offset 0]
        (when-let [^java.nio.ByteBuffer b (first bs)]
          (let [n (.remaining b)]
            (.get b out offset n)
            (recur (next bs) (+ offset n)))))
      (= s (String. out "UTF-8")))))

(defspec rope-concat-test iterations
  (prop/for-all [a gen-string
                 b gen-string]
    (let [r (.concat ^Rope (Rope/from a) (Rope/from b))]
      (and (inv/valid? r)
           (= (str a b) (.toString r))))))

(defspec rope-compare-test iterations
  (prop/for-all [a gen-string
                 b gen-string]
    (let [expected (compare (vec (code-points a)) (vec (code-points b)))
          actual   (.compareTo ^Rope (Rope/from a) (Rope/from b))]
      ;; only the sign is specified
      (= (Integer/signum expected) (Integer/signum actual)))))

;; # Linear and forked isolation
;;
;; `forked()` hands out a collection which shares nodes with the linear one it
;; came from. Continuing to write to the linear collection must not be visible
;; through the forked snapshot. Issues #36 and #37.

(defn- linear-types
  "Empty collections paired with a function that adds an integer-ish element."
  []
  [["List"       (List.)       (fn [c x] (.addLast ^IList c x))]
   ["Map"        (Map.)        (fn [c x] (.put ^IMap c x x))]
   ["Set"        (Set.)        (fn [c x] (.add ^ISet c x))]
   ["IntMap"     (IntMap.)     (fn [c x] (.put ^IMap c (long x) x))]
   ["IntSet"     (IntSet.)     (fn [c x] (.add ^ISet c (long x)))]
   ["SortedMap"  (SortedMap.)  (fn [c x] (.put ^IMap c x x))]
   ["SortedSet"  (SortedSet.)  (fn [c x] (.add ^ISet c x))]
   ["FloatMap"   (FloatMap.)   (fn [c x] (.put ^IMap c (double x) x))]
   ["LinearList" (LinearList.) (fn [c x] (.addLast ^IList c x))]
   ["LinearMap"  (LinearMap.)  (fn [c x] (.put ^IMap c x x))]
   ["LinearSet"  (LinearSet.)  (fn [c x] (.add ^ISet c x))]])

(defn- contents [c]
  (mapv str (iterator-seq (.iterator ^Iterable c))))

(defspec forked-isolation-test iterations
  (prop/for-all [seed (gen/vector (gen/choose 0 4096) 0 200)
                 more (gen/vector (gen/choose 0 4096) 1 100)]
    (every?
      (fn [[name empty add]]
        (let [l (.linear ^io.lacuna.bifurcan.ICollection empty)]
          (doseq [x seed] (add l x))
          (let [f      (.forked ^io.lacuna.bifurcan.ICollection l)
                before (contents f)]
            (doseq [x more] (add l x))
            (and (expect (str name " forked snapshot") before (contents f))
                 (inv/valid? f)
                 (inv/valid? l)))))
      (linear-types))))

(defspec linear-after-fork-still-works-test iterations
  ;; giving up ownership on fork must not break the linear collection
  (prop/for-all [seed (gen/vector (gen/choose 0 1024) 0 100)
                 more (gen/vector (gen/choose 0 1024) 0 100)]
    (every?
      (fn [[name empty add]]
        (let [l (.linear ^io.lacuna.bifurcan.ICollection empty)]
          (doseq [x seed] (add l x))
          (.forked ^io.lacuna.bifurcan.ICollection l)
          (doseq [x more] (add l x))
          (and (inv/valid? l)
               ;; every element we added is still reachable
               (= (count (contents l))
                  (.size ^io.lacuna.bifurcan.ICollection l)))))
      (linear-types))))

(defspec list-set-isolation-test iterations
  ;; Issue #36: repeated `set` on forked lists mutated earlier versions
  (prop/for-all [n    (gen/choose 1 200)
                 idx  gen/nat
                 vals (gen/vector (gen/choose 0 1000) 1 8)]
    (let [i        (mod idx n)
          base     (reduce (fn [^IList l e] (.addLast l e)) (List.) (range n))
          ;; force every version into existence before reading any of them
          versions (vec (reductions (fn [^IList l v] (.set l i v)) base vals))
          expected (into [(vec (range n))]
                         (map #(assoc (vec (range n)) i %) vals))]
      (and (every? inv/valid? versions)
           (expect "List.set versions"
                   (mapv #(mapv str %) expected)
                   (mapv contents versions))))))

(defspec list-minimal-depth-test iterations
  ;; Issue #17: appending built trees one level deeper than necessary
  (prop/for-all [n (gen/choose 1 5000)]
    (let [l    (reduce (fn [^IList v e] (.addLast v e)) (List.) (range n))
          root (inv/list-root l)
          ;; the tree holds everything except the prefix and suffix buffers
          tree-size (.size root)]
      (and (inv/valid? l)
           (or (zero? tree-size)
               ;; a root at this shift must be the smallest that fits the tree
               (let [shift (.shift root)]
                 (and (<= tree-size (bit-shift-left 1 (+ shift 5)))
                      (or (= shift 5)
                          (> tree-size (bit-shift-left 1 shift))))))))))

(deftest list-depth-at-growth-boundary-test
  ;; Issue #17: the root grew a level as soon as it had 32 children, even
  ;; though the last of them was nearly empty. Reproducing it needs a list
  ;; large enough for the root to be at shift 15.
  (let [capac (fn [shift] (bit-shift-left 1 (+ shift 5)))
        n     (+ (capac 15) (- (capac 10)) (capac 5) (capac 0))
        l     (reduce (fn [^IList v e] (.addLast v e)) (List.) (range n))
        root  (inv/list-root l)]
    (is (< n (capac 15)))
    (is (= 15 (.shift root)) "a shift-20 root is one level deeper than needed")
    (is (inv/valid? l))
    (is (= (range n) (iterator-seq (.iterator l))))))
