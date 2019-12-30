(ns bifurcan.collection-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer [defspec]]
   [clojure.pprint :refer [pprint]]
   [bifurcan.test-utils :as u :refer [iterations]]
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
   [io.lacuna.bifurcan.diffs
    DiffMap
    DiffList
    DiffSet]
   [io.lacuna.bifurcan
    IntMap
    FloatMap
    SortedMap
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

(defn walk-int-nodes [^io.lacuna.bifurcan.nodes.IntMapNodes$Node n]
  (when n
    {:size (.size n)
     :prefix (.prefix n)
     :offset (.offset n)
     :entries (->> n
                .datamap
                io.lacuna.bifurcan.nodes.Util/masks
                iterator-seq
                (map #(.key n %)))
     :children (->> n
                 .nodemap
                 io.lacuna.bifurcan.nodes.Util/masks
                iterator-seq
                (map #(.node n %))
                (map walk-int-nodes))}))

(defn pprint-int-map [m]
  (pprint
    [(walk-int-nodes (.neg m))
     (walk-int-nodes (.pos m))]))

(defn walk-sorted-nodes [^io.lacuna.bifurcan.nodes.SortedMapNodes$Node n]
  (when n
    {:color (str (.c n))
     :size (.size n)
     :entry [(.k n) (.v n)]
     :children [(walk-sorted-nodes (.l n))
                (walk-sorted-nodes (.r n))]}))

(defn pprint-sorted-map [^SortedMap m]
  (pprint (walk-sorted-nodes (.root m))))

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
    (every? #(= % (->> (.indexOf m %) .getAsLong (.nth m) .key)))))

(defn valid-set-indices? [^ISet s]
  (->> s
    .elements
    .iterator
    iterator-seq
    (every? #(= % (->> (.indexOf s %) .getAsLong (.nth s))))))

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
   :difference   #(apply dissoc %1 %2)
   })

(def bifurcan-map
  {:put          #(.put ^IMap %1 %2 %3)
   :remove       #(.remove ^IMap %1 %2)
   :union        #(.union ^IMap %1 (Map/from ^java.util.Map (zipmap %2 %2)))
   :intersection #(.intersection ^IMap %1 (Map/from ^java.util.Map (zipmap %2 %2)))
   :difference   #(.difference ^IMap %1 (Map/from ^java.util.Map (zipmap %2 %2)))})

(def bifurcan-sorted-map
  {:put          #(.put ^IMap %1 %2 %3)
   :remove       #(.remove ^IMap %1 %2)
   :union        #(.union ^IMap %1 (SortedMap/from ^java.util.Map (zipmap %2 %2)))
   :intersection #(.intersection ^IMap %1 (SortedMap/from ^java.util.Map (zipmap %2 %2)))
   :difference   #(.difference ^IMap %1 (SortedMap/from ^java.util.Map (zipmap %2 %2)))
   })

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

;; Generators

(defn map-gen [init]
  (->> map-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-map))))

(defn int-map-gen [init]
  (->> map-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) int-map))))

(defn sorted-map-gen [init]
  (->> map-actions u/actions->generator (gen/fmap #(u/apply-actions %1 (init) bifurcan-sorted-map))))

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

(u/def-collection-check test-sorted-map iterations map-actions
  [a {} clj-map
   b (SortedMap.) bifurcan-sorted-map
   c (.linear (SortedMap.)) bifurcan-sorted-map]
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

(u/def-collection-check test-diff-map iterations map-actions
  [a {} clj-map
   b (DiffMap. Map/EMPTY) bifurcan-map]
  (map= a b))

(u/def-collection-check test-map-indices iterations map-actions
  [a (Map.) bifurcan-map]
  (valid-map-indices? a))

(u/def-collection-check test-sorted-map-indices iterations map-actions
  [a (SortedMap.) bifurcan-sorted-map]
  (valid-map-indices? a))

(u/def-collection-check test-int-map-indices iterations map-actions
  [a (IntMap.) int-map]
  (valid-map-indices? a))

(u/def-collection-check test-float-map-indices iterations float-map-actions
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

(u/def-collection-check test-diff-set iterations set-actions
  [a #{} clj-set
   b (DiffSet. Set/EMPTY) bifurcan-set]
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

(u/def-collection-check test-diff-list iterations list-actions
  [a [] clj-list
   b (DiffList. List/EMPTY) bifurcan-list
   c (.linear (DiffList. List/EMPTY)) bifurcan-list
   ]
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

;;; SortedMap

(defspec test-sorted-map-slice iterations
  (prop/for-all [start (gen/choose 1 1e4)
                 end (gen/choose 1 1e4)]
    (let [start (min start end)
          end   (max start end)
          s     (range (* 2 end))]
      (map=
        (->> s (drop start) (take (inc (- end start))) (map #(vector % %)) (into {}))
        (-> (->> s (map #(vector % %)) (into {})) SortedMap/from (.slice start end))))))

(defspec test-sorted-map-floor iterations
  (prop/for-all [m (sorted-map-gen #(SortedMap.))
                 k gen/pos-int]
    (= (->> m .keys .toSet (take-while #(<= % k)) last)
      (some-> m (.floor k) .key))))

(defspec test-sorted-map-ceil iterations
  (prop/for-all [m (sorted-map-gen #(SortedMap.))
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

(u/def-collection-check test-linear-map-split iterations map-actions
  [m (LinearMap.) bifurcan-map]
  (= m (-> m (.split 2) (map-union (LinearMap.)))))

(u/def-collection-check test-map-split iterations map-actions
  [m (Map.) bifurcan-map]
  (= m (-> m (.split 2) (map-union (Map.)))))

(u/def-collection-check test-sorted-map-split iterations map-actions
  [m (SortedMap.) bifurcan-sorted-map]
  (= m (-> m (.split 2) (map-union (SortedMap.)))))

(u/def-collection-check test-int-map-split iterations map-actions
  [m (IntMap.) int-map]
  (= m (-> m (.split 2) (map-union (IntMap.)))))

(u/def-collection-check test-float-map-split iterations float-map-actions
  [m (FloatMap.) float-map]
  (= m (-> m (.split 2) (map-union (FloatMap.)))))

(u/def-collection-check test-linear-list-split iterations list-actions
  [l (LinearList.) bifurcan-list]
  (= l (-> l (.split 2) into-array Lists/concat)))

(u/def-collection-check test-list-split iterations list-actions
  [l (List.) bifurcan-list]
  (= l (-> l (.split 2) into-array Lists/concat)))

(u/def-collection-check test-list-split2 iterations list-actions
  [l (Lists/from []) bifurcan-list]
  (= l (-> l (.split 2) into-array Lists/concat)))

(u/def-collection-check test-linear-set-split iterations set-actions
  [s (LinearSet.) bifurcan-set]
  (= s (-> s (.split 2) (set-union (LinearSet.)))))

(u/def-collection-check test-linear-set-split2 iterations set-actions
  [s (Set.) bifurcan-set]
  (= s (-> s (.split 2) (set-union (Set.)))))

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

(defspec test-concat-wrapper iterations
  (prop/for-all [a (list-gen #(Lists/from []))
                 b (list-gen #(Lists/from []))]
    (= (concat (->vec a) (->vec b))
      (->vec (.concat ^IList a b)))))

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
