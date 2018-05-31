(ns bifurcan.collection-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)]
   [bifurcan.test-utils :as u]
   [clojure.set :as set]
   [proteus :refer [let-mutable]])
  (:import
   [java.util
    HashMap
    HashSet
    ArrayList
    ArrayDeque
    Collection]
   [io.lacuna.bifurcan.utils
    Encodings
    BitVector
    Bits
    Iterators]
   [io.lacuna.bifurcan.nodes
    ListNodes$Node]
   [io.lacuna.bifurcan
    IntMap
    FloatMap
    Map
    Maps
    List
    Lists
    Set
    Sets
    IMap
    IEntry
    IList
    ISet
    LinearList
    LinearMap
    LinearSet]))

(set! *warn-on-reflection* false)

;;;

(defn ->map [^IMap m]
  (->> m
    .entries
    .iterator
    iterator-seq
    (map (fn [^IEntry e] [(.key e) (.value e)]))
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

(defn valid-map-indices? [^IMap m]
  (->> m
    .keys
    .iterator
    iterator-seq
    (every? #(= % (.key (.nth m (.indexOf m %)))))))

(defn valid-set-indices? [^ISet m]
  (->> m
    .elements
    .iterator
    iterator-seq
    (every? #(= % (.nth m (.indexOf m %))))))

;;;

(def gen-element (gen/elements [0 1]))

(def gen-double
  (->> gen/double
    (gen/such-that #(not (Double/isNaN %)))
    (gen/fmap #(if (== -0.0 %) 0.0 %))))

(def list-actions
  {:add-first    [gen-element]
   :add-last     [gen-element]
   :set          [gen/pos-int gen-element]
   :slice        [gen/pos-int gen/pos-int]
   :concat       [(gen/vector gen-element 0 1e3)]
   :remove-first []
   :remove-last  []})

(def map-actions
  {:put          [gen/large-integer gen-element]
   :remove       [gen/large-integer]
   :union        [(gen/vector gen/large-integer 0 32)]
   :intersection [(gen/vector gen/large-integer 0 32)]
   :difference   [(gen/vector gen/large-integer 0 32)]})

(def float-map-actions
  {:put          [gen-double gen-element]
   :remove       [gen-double]
   :union        [(gen/vector gen-double 0 32)]
   :intersection [(gen/vector gen-double 0 32)]
   :difference   [(gen/vector gen-double 0 32)]})

(def set-actions
  {:add          [gen/large-integer]
   :remove       [gen/large-integer]
   :union        [(gen/vector gen/large-integer 0 32)]
   :intersection [(gen/vector gen/large-integer 0 32)]
   :difference   [(gen/vector gen/large-integer 0 32)]})

(def clj-list
  {:add-first    #(cons %2 %1)
   :add-last     #(conj (vec %1) %2)
   :set          #(assoc (vec %1) (min (count %1) %2) %3)
   :slice        #(let [[s e] (sort [%2 %3])]
                    (->> %1 (drop s) (take (- e s)) vec))
   :concat       #(vec (concat %1 %2))
   :remove-first #(or (rest %) [])
   :remove-last  #(or (butlast %) [])})

(def bifurcan-list
  {:add-first    #(.addFirst ^IList %1 %2)
   :add-last     #(.addLast ^IList %1 %2)
   :set          #(.set ^IList %1 (min (.size ^IList %1) %2) %3)
   :slice        #(let [^IList l %1
                        [s e] (sort [%2 %3])]
                    (.slice l
                      (max 0 (min (.size l) s))
                      (min (.size l) e)))
   :concat       #(.concat ^IList %1 (List/from %2))
   :remove-first #(.removeFirst ^IList %)
   :remove-last  #(.removeLast ^IList %)})

(def clj-map
  {:put          assoc
   :remove       dissoc
   :union        #(merge %1 (zipmap %2 %2))
   :intersection #(select-keys %1 %2)
   :difference   #(apply dissoc %1 %2)})

(def bifurcan-map
  {:put          #(.put ^IMap %1 %2 %3)
   :remove       #(.remove ^IMap %1 %2)
   :union        #(.union ^IMap %1 (Map/from ^java.util.Map (zipmap %2 %2)))
   :intersection #(.intersection ^IMap %1 (Map/from ^java.util.Map (zipmap %2 %2)))
   :difference   #(.difference ^IMap %1 (Map/from ^java.util.Map (zipmap %2 %2)))})

(def int-map
  {:put          #(.put ^IMap %1 %2 %3)
   :remove       #(.remove ^IMap %1 %2)
   :union        #(.union ^IMap %1 (IntMap/from (zipmap %2 %2)))
   :intersection #(.intersection ^IMap %1 (IntMap/from (zipmap %2 %2)))
   :difference   #(.difference ^IMap %1 (IntMap/from (zipmap %2 %2)))})

(def float-map
  {:put          #(.put ^IMap %1 %2 %3)
   :remove       #(.remove ^IMap %1 %2)
   :union        #(.union ^IMap %1 (FloatMap/from (zipmap %2 %2)))
   :intersection #(.intersection ^IMap %1 (FloatMap/from (zipmap %2 %2)))
   :difference   #(.difference ^IMap %1 (FloatMap/from (zipmap %2 %2)))})

(def clj-set
  {:add          conj
   :remove       disj
   :union        #(reduce conj %1 %2)
   :difference   #(reduce disj %1 %2)
   :intersection #(set/intersection %1 (set %2))})

(defn construct-set [template elements]
  (if (instance? Set template)
    (Set/from elements)
    (LinearSet/from elements)))

(def bifurcan-set
  {:add          #(.add ^ISet %1 %2)
   :remove       #(.remove ^ISet %1 %2)
   :union        #(.union ^ISet %1 (construct-set %1 %2))
   :intersection #(.intersection ^ISet %1 (construct-set %1 %2))
   :difference   #(.difference ^ISet %1 (construct-set %1 %2))})

;;;

(def iterations 1e4)

;; Generators

(defn map-gen [init]
  (->> map-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-map))))

(defn int-map-gen [init]
  (->> map-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) int-map))))

(defn float-map-gen [init]
  (->> float-map-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) float-map))))

(defn set-gen [init]
  (->> set-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-set))))

(defn list-gen [init]
  (->> list-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-list))))

(defn ->tree [^ListNodes$Node n]
  (let [nodes (.numNodes n)]
    {:shift (.shift n)
     :offsets (take nodes (.offsets n))
     :nodes (->> n
              .nodes
              (take nodes)
              (map #(if (instance? ListNodes$Node %)
                      (->tree %)
                      (seq %))))}))

;; Maps

(u/def-collection-check test-linear-map iterations map-actions
  [a {} clj-map
   b (LinearMap.) bifurcan-map]
  (map= a b))

(u/def-collection-check test-map iterations map-actions
  [a {} clj-map
   b (Map.) bifurcan-map
   c (.linear (Map.)) bifurcan-map]
  (and
    (= b c)
    (map= a b)
    (map= a c)))

(defspec test-linear-forked-map iterations
  (prop/for-all [actions (u/actions->generator map-actions)]
    (let [n  (count actions)
          a  (u/apply-actions (take n actions) (.linear (Map.)) bifurcan-map)
          b  (u/apply-actions (drop n actions) (.forked a) bifurcan-map)
          a' (u/apply-actions (take n actions) {} clj-map)
          b' (u/apply-actions actions {} clj-map)]
      (and
        (map= a' a)
        (map= b' b)))))

(u/def-collection-check test-int-map iterations map-actions
  [a {} clj-map
   b (IntMap.) int-map
   c (.linear (IntMap.)) int-map]
  (and
    (= b c)
    (map= a b)
    (map= a c)))

(u/def-collection-check test-float-map iterations float-map-actions
  [a {} clj-map
   b (FloatMap.) float-map
   c (.linear (FloatMap.)) float-map]
  (and
    (= b c)
    (map= a b)
    (map= a c)))

(u/def-collection-check test-virtual-map iterations map-actions
  [a {} clj-map
   b Maps/EMPTY bifurcan-map]
  (map= a b))

(u/def-collection-check test-map-indices iterations map-actions
  [a (Map.) bifurcan-map]
  (valid-map-indices? a))

(u/def-collection-check test-int-map-indices iterations map-actions
  [a (IntMap.) int-map]
  (valid-map-indices? a))

(u/def-collection-check test-int-map-indices iterations float-map-actions
  [a (FloatMap.) float-map]
  (valid-map-indices? a))

;; Sets

(u/def-collection-check test-linear-set iterations set-actions
  [a #{} clj-set
   b (LinearSet.) bifurcan-set]
  (set= a b))

(u/def-collection-check test-set iterations set-actions
  [a #{} clj-set
   b (Set.) bifurcan-set
   c (.linear (Set.)) bifurcan-set]
  (and
    (= b c)
    (set= a b)
    (set= a c)))

(u/def-collection-check test-virtual-set iterations set-actions
  [a #{} clj-set
   b Sets/EMPTY bifurcan-set]
  (set= a b))

(u/def-collection-check test-set-indices iterations set-actions
  [a (Set.) bifurcan-set]
  (valid-set-indices? a))

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

(u/def-collection-check test-virtual-list iterations list-actions
  [a [] clj-list
   b Lists/EMPTY bifurcan-list
   c (.linear (Lists/from [])) bifurcan-list]
  (and
    (= b c)
    (list= a b)
    (list= a c)))

(defspec test-list-range iterations
  (prop/for-all [n (gen/choose 1 1e4)]
    (list= (range n)
      (List/from (range n)))))

(defspec test-list-range-concat iterations
  (prop/for-all [a (gen/choose 1 2e3)
                 b (gen/choose 1 2e3)]
    (list= (concat (range a) (range b))
      (List/from (concat (range a) (range b))))))

(defspec test-list-slice iterations
  (prop/for-all [start (gen/choose 1 1e4)
                 end (gen/choose 1 1e4)]
    (let [start (min start end)
          end   (max start end)
          s     (range (* 2 end))]
      (and
        (list=
          (->> s (drop start) (take (- end start)))
          (.slice (List/from s) start end))
        (list=
          (->> s (drop start) (take (- end start)))
          (Lists/slice (List/from s) start end))))))

;;; IntMap

(defspec test-int-map-slice iterations
  (prop/for-all [start (gen/choose 1 1e4)
                 end (gen/choose 1 1e4)]
    (let [start (min start end)
          end   (max start end)
          s     (range (* 2 end))]
      (map=
        (->> s (drop start) (take (inc (- end start))) (map #(vector % %)) (into {}))
        (-> (->> s (map #(vector % %)) (into {})) IntMap/from (.slice start end))))))

(defspec test-int-map-floor iterations
  (prop/for-all [m (int-map-gen #(IntMap.))
                 k gen/pos-int]
    (= (->> m .keys .toSet (take-while #(<= % k)) last)
      (some-> m (.floor k) .key))))

(defspec test-int-map-ceil iterations
  (prop/for-all [m (int-map-gen #(IntMap.))
                 k gen/pos-int]
    (= (->> m .keys .toSet (drop-while #(< % k)) first)
      (some-> m (.ceil k) .key))))

;;; FloatMap

(defspec test-float-map-slice iterations
  (prop/for-all [start (gen/choose 1 1e4)
                 end (gen/choose 1 1e4)]
    (let [start (min start end)
          end   (max start end)
          s     (map double (range (* 2 end)))]
      (map=
        (->> s
          (drop start)
          (take (inc (- end start)))
          (map #(vector % %))
          (into {}))
        (-> (->> s (map #(vector % %)) (into {}))
          FloatMap/from
          (.slice (double start) (double end)))))))

(defspec test-float-map-floor iterations
  (prop/for-all [m (float-map-gen #(FloatMap.))
                 k gen-double]
    (= (->> m .keys .toSet (take-while #(<= % k)) last)
      (some-> m (.floor k) .key))))

(defspec test-float-map-ceil iterations
  (prop/for-all [m (float-map-gen #(FloatMap.))
                 k gen-double]
    (= (->> m .keys .toSet (drop-while #(< % k)) first)
      (some-> m (.ceil k) .key))))

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
  (prop/for-all [m (int-map-gen #(IntMap.))]
    (-> m (.split 2) (map-union (IntMap.)))))

(defspec test-int-map-split iterations
  (prop/for-all [m (float-map-gen #(FloatMap.))]
    (-> m (.split 2) (map-union (FloatMap.)))))

(defspec test-linear-list-split iterations
  (prop/for-all [l (list-gen #(LinearList.))]
    (-> l (.split 2) into-array Lists/concat)))

(defspec test-list-split iterations
  (prop/for-all [l (list-gen #(List.))]
    (-> l (.split 2) into-array Lists/concat)))

(defspec test-virtual-list-split iterations
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
  (prop/for-all [a (-> list-actions u/actions->generator)
                 b (-> list-actions u/actions->generator)]
    (let [a (u/apply-actions a (List.) bifurcan-list)
          b (u/apply-actions b (List.) bifurcan-list)]
      (if (list= (concat (->vec a) (->vec b))
            (.concat ^IList a b))
        true
        (do
          (prn (.size a))
          false)))))

(defspec test-linear-list-concat iterations
  (prop/for-all [a (list-gen #(LinearList.))
                 b (list-gen #(LinearList.))]
    (= (concat (->vec a) (->vec b))
      (->vec (.concat ^IList a b)))))

(defspec test-virtual-list-concat iterations
  (prop/for-all [a (list-gen #(Lists/from []))
                 b (list-gen #(Lists/from []))]
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

;; Set operations

(defspec test-set-union iterations
  (prop/for-all [a (set-gen #(Set.))
                 b (set-gen #(Set.))]
    (= (set/union (->set a) (->set b))
      (->set (.union ^ISet a ^ISet b)))))

(defspec test-set-difference iterations
  (prop/for-all [a (set-gen #(Set.))
                 b (set-gen #(Set.))]
    (= (set/difference (->set a) (->set b))
      (->set (.difference ^ISet a ^ISet b)))))

(defspec test-set-intersection iterations
  (prop/for-all [a (set-gen #(Set.))
                 b (set-gen #(Set.))]
    (= (set/intersection (->set a) (->set b))
      (->set (.intersection ^ISet a ^ISet b)))))

;; LinearSet operations

(defspec test-linear-set-union iterations
  (prop/for-all [a (set-gen #(LinearSet.))
                 b (set-gen #(LinearSet.))]
    (= (set/union (->set a) (->set b))
      (->set (.union ^ISet a ^ISet b)))))

(defspec test-linear-set-difference iterations
  (prop/for-all [a (set-gen #(LinearSet.))
                 b (set-gen #(LinearSet.))]
    (= (set/difference (->set a) (->set b))
      (->set (.difference ^ISet a ^ISet b)))))

(defspec test-linear-set-intersection iterations
  (prop/for-all [a (set-gen #(LinearSet.))
                 b (set-gen #(LinearSet.))]
    (= (set/intersection (->set a) (->set b))
      (->set (.intersection ^ISet a ^ISet b)))))

;; FloatMap operations

(defspec test-long-double-roundtrip iterations
  (prop/for-all [n gen-double]
    (== n (-> n Encodings/doubleToLong Encodings/longToDouble))))

(defspec test-long-double-compare iterations
  (prop/for-all [a gen-double
                 b gen-double]
    (= (Double/compare a b)
      (Long/compare
        (Encodings/doubleToLong a)
        (Encodings/doubleToLong b)))))
