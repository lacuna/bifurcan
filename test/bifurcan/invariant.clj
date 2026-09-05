(ns bifurcan.invariant
  "Bifurcan's tries maintain internal invariants which aren't visible through
  the public API: node sizes must agree with their children, entries must sit
  in the slot their hash selects, red-black trees must be balanced, and so on.
  Each node class has a private `checkInvariant` method which throws when one
  of these is violated. We reach them by reflection so that the tests can
  check structure directly, rather than inferring it from behavior."
  (:import (io.lacuna.bifurcan FloatMap
                               IntMap
                               IntSet
                               List
                               Map
                               Rope
                               Set
                               SortedMap
                               SortedSet)
           (io.lacuna.bifurcan.nodes IntMapNodes$Node
                                     ListNodes$Node
                                     MapNodes$Node
                                     RopeNodes$Node
                                     SortedMapNodes$Node)))

(defn- field-reader
  "Returns a function which reads a (possibly private) field from instances of
  a class."
  [^Class class, ^String field-name]
  (let [field (doto (.getDeclaredField class field-name)
                (.setAccessible true))]
    (fn [obj] (.get field obj))))

(defn- invariant-checker
  "Returns a function which invokes a node class' private `checkInvariant`."
  [^Class class & arg-types]
  (let [method (doto (.getDeclaredMethod class "checkInvariant" (into-array Class arg-types))
                 (.setAccessible true))]
    (fn [node & args]
      (try
        (.invoke method node (object-array args))
        (catch java.lang.reflect.InvocationTargetException e
          ;; unwrap, so callers see the invariant failure rather than the
          ;; reflection machinery
          (throw (.getCause e)))))))

;; Roots. These are memoized as vars rather than computed per call, because the
;; invariant checks run on every step of every generative test.

(def ^:private map-root       (field-reader Map "root"))
(def ^:private set-map        (field-reader Set "map"))
(def list-root                (field-reader List "root"))
(def ^:private int-map-neg    (field-reader IntMap "neg"))
(def ^:private int-map-pos    (field-reader IntMap "pos"))
(def ^:private int-set-map    (field-reader IntSet "m"))
(def ^:private sorted-root    (field-reader SortedMap "root"))
(def ^:private sorted-set-map (field-reader SortedSet "m"))
(def ^:private float-map-map  (field-reader FloatMap "map"))
(def ^:private rope-root      (field-reader Rope "root"))

(def ^:private check-map-node    (invariant-checker MapNodes$Node Integer/TYPE))
(def ^:private check-int-node    (invariant-checker IntMapNodes$Node))
(def ^:private check-list-node   (invariant-checker ListNodes$Node))
(def ^:private check-rope-node   (invariant-checker RopeNodes$Node))
(def ^:private check-sorted-node (invariant-checker SortedMapNodes$Node))

(defprotocol Checkable
  (-check! [coll]
    "Throws if the collection's internal structure is inconsistent."))

(extend-protocol Checkable
  Map
  (-check! [m] (check-map-node (map-root m) (int 0)))

  Set
  (-check! [s] (-check! (set-map s)))

  List
  (-check! [l] (check-list-node (list-root l)))

  IntMap
  (-check! [m]
    (check-int-node (int-map-neg m))
    (check-int-node (int-map-pos m)))

  IntSet
  (-check! [s] (-check! (int-set-map s)))

  FloatMap
  (-check! [m] (-check! (float-map-map m)))

  SortedMap
  (-check! [m] (check-sorted-node (sorted-root m)))

  SortedSet
  (-check! [s] (-check! (sorted-set-map s)))

  Rope
  (-check! [r] (check-rope-node (rope-root r)))

  Object
  ;; LinearMap, LinearSet and LinearList are flat, so there's nothing to check.
  (-check! [_] nil)

  nil
  (-check! [_] nil))

(defn check!
  "Throws if the collection's internal structure is inconsistent, otherwise
  returns the collection."
  [coll]
  (-check! coll)
  coll)

(defn valid?
  "Like `check!`, but returns true or false, printing the violation. Use this
  inside generative predicates, where a thrown exception is less legible than a
  failing assertion."
  [coll]
  (try
    (-check! coll)
    true
    (catch Exception e
      (println "invariant violation:" (.getMessage e))
      false)))
