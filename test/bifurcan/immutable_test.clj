(ns bifurcan.immutable-test
  "This test generates ASTs for simple programs involving sets (later maps
  etc.) and evaluates them both as Clojure and forked Bifurcan structures,
  testing that they are equivalent. This is already handled by collection-test,
  but we *also* check that the intermediate collections are left unchanged as
  we combine them in various ways. For example, this test can show that if you
  take a set and union it with another, that union function actually alters its
  arguments."
  (:refer-clojure :exclude [eval])
  (:require [bifurcan.invariant :as inv]
            [bifurcan.test-utils :as u]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [set :as set]
                     [string :as str]
                     [test :refer [deftest is are]]]

            [clojure.test.check [generators :as gen]
                                [properties :as prop]
                                [clojure-test :as ct :refer [defspec]]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]])
  (:import (java.util HashMap
                      HashSet
                      ArrayList
                      ArrayDeque
                      Collection)
           (io.lacuna.bifurcan.utils Encodings
                                     BitVector
                                     Bits
                                     Iterators)
           (io.lacuna.bifurcan.nodes ListNodes$Node)
           (io.lacuna.bifurcan ICollection
                               IntMap
                               IntSet
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
                               LinearSet
                               SortedSet)))

;(set! *warn-on-reflection* true)

(def iterations
  "Our tests are a lot more expensive, so we run fewer of them. There are also
  a lot of them now, one per collection implementation, so we favor breadth
  over depth."
  (/ u/iterations 50))

(def max-program-size
  "How long can programs be? This generator is very bad at shrinking, so when
  debugging you probably want to lower this."
  64)

(def max-basic-size
  "How many elements can we put in a basic collection, like Set.of(1,2,3...)?"
  ; TODO: you should raise this when it starts passing; this is just to get
  ; minimal examples
  32)

(def value-gen
  "Generator of basic values."
  gen/large-integer)

(defn fallback-expr
  "We use these to generate expressions of a given type when rewrite-var
  fails."
  [type]
  (case type
    :list :list/empty
    :set  :set/empty
    :map  :map/empty))

(defn expr-type
  "Given an expression, and optionally a program, returns the type of an
  expression in it. For instance, :set/empty is a :set, as is [:set/union]. The
  type of [:var 2] is the type of the third expression in the program."
  ([expr]
   (expr-type expr []))
  ([expr program]
   (if (vector? expr)
     (case (first expr)
       (:list/of
        :list/add-first
        :list/add-last
        :list/remove-first
        :list/remove-last
        :list/set
        :list/slice
        :list/concat)
       :list

       (:map/of
        :map/put
        :map/union
        :map/intersection
        :map/difference)
       :map

       (:set/of
         :set/add
         :set/remove
         :set/union
         :set/intersection
         :set/difference)
       :set

       :var
       (recur (nth program (second expr)) program))

     (case expr
       :list/empty :list
       :map/empty  :map
       :set/empty  :set))))

(defn var-gen
  "Generator of [:var 2 type] expressions. We just pick random numbers, and
  rewrite them later to refer to previous elements of the correct type, based
  on the program."
  [type]
  (gen/fmap (fn [x]
              [:var x type])
            gen/nat))

(defn or-var
  "Wraps a generator in one that emits variables of the given type."
  [type gen]
  (gen/one-of [gen (var-gen type)]))

(def basic-list-gen
  "Generators of basic list expressions."
  (gen/one-of
    [(gen/return :list/empty)
     (gen/fmap (fn [elements] (into [:list/of] elements))
               (gen/vector value-gen 1 max-basic-size))]))

(def basic-set-gen
  "Generators of basic set expressions."
  (gen/one-of
    [; Empty set
     (gen/return :set/empty)
     ; [:set/of 1 2 3]
     (gen/fmap (fn [elements]
                 (into [:set/of] elements))
               (gen/vector value-gen 1 max-basic-size))]))

(def basic-map-gen
  "Generator of basic map expressions."
  (gen/one-of
    [(gen/return :map/empty)
     (gen/fmap (fn [pairs]
                 (into [:map/of] (mapcat identity pairs)))
               (gen/vector (gen/tuple value-gen value-gen)
                           1 max-basic-size))]))

(def basic-list-gen+ (or-var :list basic-list-gen))
(def basic-set-gen+ (or-var :set basic-set-gen))
(def basic-map-gen+ (or-var :map basic-map-gen))

(def composite-list-gen
  "Generators of composite list expressions like [:list/add [:var 0] 5]"
  (gen/one-of
    [(gen/tuple (gen/elements [:list/add-first :list/add-last])
                basic-list-gen+
                value-gen)
     (gen/tuple (gen/elements [:list/remove-first :list/remove-last])
                basic-list-gen+)
     (gen/tuple (gen/elements [:list/concat])
                basic-list-gen+
                basic-list-gen+)
     #_(gen/tuple (gen/return :list/slice)
                basic-list-gen+
                gen/nat
                gen/nat)]))

(def composite-set-gen
  "Generators of composite set expressions like [:set/add [:var 2] 3]"
  (gen/one-of
    [(gen/tuple (gen/elements [:set/add
                               :set/remove])
                basic-set-gen+
                value-gen)
     (gen/tuple (gen/elements [:set/union
                               :set/intersection
                               :set/difference])
                basic-set-gen+
                basic-set-gen+)]))

(def composite-map-gen
  "Generators of composite map expressions like [:map/union [:var 2]
  :map/empty]"
  (gen/one-of
    [(gen/tuple (gen/return :map/put)
                basic-map-gen+
                value-gen
                value-gen)
     (gen/tuple (gen/elements [:map/union
                               :map/intersection
                               :map/difference])
                basic-map-gen+
                basic-map-gen+)]))

(defn expr-gen
  "Generator of expressions of the given types."
  [types]
  (->> types
       (mapcat {:list [basic-list-gen composite-list-gen]
                :set [basic-set-gen composite-set-gen]
                :map [basic-map-gen composite-map-gen]})
       vec
       gen/one-of))

(defn rewrite-vars
  "Takes an expression, and rewrites every [:var ...] form in it with `(f [:var
  ...])`."
  [f expr]
  (cond (not (vector? expr))
        expr

        (identical? :var (first expr))
        (f expr)

        ; Recur
        true
        (into [(first expr)]
              (map (partial rewrite-vars f) (rest expr)))))

(defn resolve-var
  "Given an index of types to vectors of available expression indices, and a
  var expression like [:var 2 :set], returns a var expression like [:var 5],
  where that variable is a set defined before index. If no var of that type is
  available, uses a fallback expression."
  [by-type [_ i type]]
  (let [; What indices in the program are of the right type?
        indices (get by-type type [])
        n (count indices)]
    (if (zero? n)
      (fallback-expr type)
      [:var (nth indices (mod i n))])))


(defn resolve-vars
  "Runs through a program and rewrites any occurrence of [:var 2 :set] to a
  previous set expression, or the empty set."
  ([program]
   ; We step through the program line by line, rewriting expressions and
   ; resolving their types.
   (loop [i       0
          by-type {} ; A map of types to vectors of expression indices
          program program]
     (if (= i (count program))
       program
       (let [expr     (rewrite-vars (partial resolve-var by-type)
                                    (nth program i))
             program  (assoc program i expr)
             type     (expr-type expr program)]
         (recur (inc i)
                (update by-type type
                        (fn [indices] (conj (or indices []) i)))
                program))))))

(defn program-gen
  "A generator of abstract programs. Takes a set of types. Each program is a
  vector of operations to be performed sequentially, which evaluate to that
  type. Operations can refer to earlier operations by their index, as if they
  were all variables in a `let` binding. For example:

  [[:set/of 1 2 3]
  [:set/empty]
  [:set/union [:var 0] [:var 1]]]

  This program computes a set containing 1, 2, and 3, then an empty set, then
  takes the union of those two."
  [types]
  (gen/fmap resolve-vars
            (gen/vector (expr-gen types) 1 max-program-size)))

(defn eval-expr-clj
  "Takes a trace of program results so far, and evaluates a new expression in
  that context."
  [trace expr]
  (if (vector? expr)
    (let [[f a b c] expr]
      (case f
        :list/of           (vec (rest expr))
        :list/add-first    (vec (cons b a))
        :list/add-last     (conj a b)
        :list/remove-first (if (seq a)
                             (subvec a 1)
                             [])
        :list/remove-last  (if (seq a)
                             (pop a)
                             [])
        :list/concat       (into a b)
        :list/slice        (subvec a b c)

        :map/of           (apply hash-map (rest expr))
        :map/put          (assoc a b c)
        :map/union        (merge a b)
        :map/intersection (select-keys a (keys b))
        :map/difference   (reduce dissoc a (keys b))

        :set/of           (set (rest expr))
        :set/add          (conj a b)
        :set/remove       (disj a b)
        :set/union        (set/union a b)
        :set/intersection (set/intersection a b)
        :set/difference   (set/difference a b)
        :var              (:res (nth trace a))))
    (case expr
      :list/empty []
      :map/empty {}
      :set/empty #{}
      expr)))

(defn- fold-set
  "Builds a set by adding elements one at a time, for the set types which have
  no static `from`."
  [empty]
  (fn [xs]
    (.forked ^ISet (reduce (fn [s x] (.add ^ISet s x))
                           (.linear ^ISet empty)
                           xs))))

(def default-impl
  "Every operation in an AST is an interface call, so the only thing which
  varies between collection types is construction. An impl maps each of :list,
  :map and :set to its empty value and a function building one from a Clojure
  collection."
  {:list {:empty List/EMPTY :of #(List/from ^Iterable (vec %))}
   :map  {:empty Map/EMPTY  :of #(Map/from ^java.util.Map %)}
   :set  {:empty Set/EMPTY  :of #(Set/from ^Iterable (vec %))}})

(def map-impls
  "The map types an AST of :map operations can be evaluated against. Keys are
  always integers, so every one of these accepts them."
  {"Map"       (:map default-impl)
   "IntMap"    {:empty (IntMap.)    :of #(IntMap/from ^java.util.Map %)}
   "SortedMap" {:empty (SortedMap.) :of #(SortedMap/from ^java.util.Map %)}})

(def set-impls
  "The set types an AST of :set operations can be evaluated against."
  {"Set"       (:set default-impl)
   "IntSet"    {:empty (IntSet.)    :of (fold-set (IntSet.))}
   "SortedSet" {:empty (SortedSet.) :of (fold-set (SortedSet.))}})

(defn eval-expr-bifurcan-with
  "Takes an impl (see `default-impl`), a trace of program results so far, and
  evaluates a new expression in that context."
  [impl trace expr]
  (if (vector? expr)
    (let [[f a b c] expr]
      (case f
        :list/of           ((-> impl :list :of) (rest expr))
        :list/add-first    (.addFirst ^IList a b)
        :list/add-last     (.addLast ^IList a b)
        :list/remove-first (.removeFirst ^IList a)
        :list/remove-last  (.removeLast ^IList a)
        :list/concat       (.concat ^IList a b)
        :list/slice        (.slice ^IList a b c)

        :map/of           ((-> impl :map :of) (apply hash-map (rest expr)))
        :map/put          (.put ^IMap a b c)
        :map/union        (.union ^IMap a b)
        :map/intersection (.intersection ^IMap a ^IMap b)
        :map/difference   (.difference ^IMap a ^IMap b)

        :set/of           ((-> impl :set :of) (rest expr))
        :set/add          (.add ^ISet a b)
        :set/remove       (.remove ^ISet a b)
        :set/union        (.union ^ISet a b)
        :set/intersection (.intersection ^ISet a b)
        :set/difference   (.difference ^ISet a b)
        :var              (:res (nth trace a))))
    (case expr
      :list/empty (-> impl :list :empty)
      :map/empty  (-> impl :map :empty)
      :set/empty  (-> impl :set :empty)
      expr)))

(def eval-expr-bifurcan
  "Evaluates against Map, Set and List."
  (partial eval-expr-bifurcan-with default-impl))

(defn print-program
  "Prints out a program without evaluating it."
  [program]
  (->> program
       (map-indexed (fn [i expr]
                      (printf "%3d %s\n" i (pr-str expr))))
       dorun))

(defn expr-str-bifurcan
  "Converts an expression in a program to a printable string. See
  print-program-bifurcan."
  [expr]
  (if (vector? expr)
    (let [r         expr-str-bifurcan
          [f a b c] expr]
      (case f
        :list/of           (str "(List/from "
                                (str/join " " (map r (rest expr)))
                                ")")
        :list/add-first    (str "(.addFirst " (r a) " " (r b) ")")
        :list/add-last     (str "(.addLast " (r a) " " (r b) ")")
        :list/remove-first (str "(.removeFirst " (r a) ")")
        :list/remove-last  (str "(.removeLast " (r a) ")")
        :list/concat       (str "(.concat " (r a) " " (r b) ")")
        :list/slice        (str "(.slice " (r a) " " (r b) " " (r c) ")")

        :map/of           (str "(Map/from "
                               (pr-str (apply hash-map (rest expr)))
                               ")")
        :map/put          (str "(.put " (r a) " " (r b) " " (r c) ")")
        :map/union        (str "(.union " (r a) " " (r b) ")")
        :map/intersection (str "(.intersection " (r a) " " (r b) ")")
        :map/difference   (str "(.difference " (r a) " " (r b) ")")

        :set/of           (str "(Set/from "
                               (pr-str (vec (rest expr)))
                               ")")
        :set/add          (str "(.add " (r a) " " (r b) ")")
        :set/remove       (str "(.remove " (r a) " " (r b) ")")
        :set/union        (str "(.union " (r a) " " (r b) ")")
        :set/intersection (str "(.intersection " (r a) " " (r b) ")")
        :set/difference   (str "(.difference " (r a) " " (r b) ")")
        :var              (str "x" a)))
    (case expr
      :list/empty "List/EMPTY"
      :map/empty "Map/EMPTY"
      :set/empty "Set/EMPTY"
      (pr-str expr))))

(defn print-bifurcan-preamble
  "Prints out some Clojure expressions as a preamble to print-program-bifurcan."
  []
  (println "(import '(io.lacuna.bifurcan Map Set))"))

(defn print-program-bifurcan
  "Prints out a program as Bifurcan expressions you can run at the REPL.
  Optionally takes a line number to begin at."
  ([program]
   (print-program-bifurcan 0 program))
  ([first-line program]
   (dorun
     (map-indexed (fn [i expr]
                    (println (str "(def x" (+ first-line i) " "
                                  (expr-str-bifurcan expr)
                                  ")")))
                  program))))

(defn print-trace
  "Prints out a trace."
  [trace]
  (->> trace
       (map-indexed (fn [i {:keys [expr res clj]}]
                      (println
                        (format "%3d" i)
                        (pr-str expr)
                        "->"
                        (pr-str clj))))
       dorun))


(defn eval-recursive
  "To simplify our node evaluators, they don't recur themselves. This takes an
  evaluation function, a trace, an expression, and evaluates it
  recursively--depth first, left to right."
  [eval-expr trace expr]
  (if (vector? expr)
    ; Do args first
    (let [args-evaled (into [(first expr)]
                            (mapv (partial eval-recursive eval-expr trace)
                                  (rest expr)))]
      (eval-expr trace args-evaled))
    ; Leaf node; evaluate directly
    (eval-expr trace expr)))

(defn eval-trace
  "Evaluates a program using the given function for evaluating an expression.
  Returns a trace: a vector of results from evaluating the program in order.
  Each trace element is a map like:

      {:expr  [:union ...]
       :res    #<Set ...>
       :clj    #{...}}

  The :res is the result of evaluating the expression. :clj is a datafied
  version of result. We expect that if Bifurcan is truly immutable, at the end
  of the evaluation (datafy bifurcan) = clj. The last element in the trace is
  the result of our top-level AST eval."
  [eval-expr program]
  (reduce (fn [trace expr]
            (let [res (eval-recursive eval-expr trace expr)]
              (conj trace
                    {:expr expr
                     :res res
                     :clj (datafy res)})))
          []
          program))

(defn eval-last
  "Evaluates a program and returns its final value."
  [eval-expr program]
  (-> (eval-trace eval-expr program)
      peek
      :res))

(defn eval-check-immutable
  "Evaluates a program using the given function for evaluating an expression.
  Ensures that at the end, expressions are left unchanged from their original
  values. Returns the trace."
  ([eval-expr program]
   (let [trace (eval-trace eval-expr program)]
     ; Check the trace elements are still stable.
     (doseq [{:keys [expr res clj]} trace]
       ; Structural invariants first: a violation here explains an equality or
       ; iteration failure further down.
       (is (inv/valid? res)
           (str (with-out-str (print-trace trace))
                "\n\nExpr:         " (pr-str expr)
                "\nRes:          " res))
       (is (= clj (datafy res))
           (when (instance? ICollection res)
             (str
               (with-out-str (print-trace trace))
               "\n\nExpr:         " (pr-str expr)
               "\nWas:          " (pr-str clj)
               "\nNow:          " (pr-str (datafy res))
               "\nRes:          " res
               "\nSize:         " (.size ^ICollection res)
               "\niterator-seq: " (pr-str (mapv datafy
                                                (iterator-seq (.iterator ^ICollection res))))
               "\nnths:         " (mapv #(try
                                           (.nth ^ICollection res %)
                                           (catch IndexOutOfBoundsException e
                                             :out-of-bounds))
                                        (range 0 (inc (max (count clj)
                                                      (.size ^ICollection res)))))

               ))))
     trace)))

(defn eval-compare
  "Evaluates a program using both Clojure and Bifurcan evaluators, testing that
  the two yield equivalent traces."
  ([program]
   (eval-compare default-impl program))
  ([impl program]
   (let [clj      (eval-check-immutable eval-expr-clj program)
         bifurcan (eval-check-immutable (partial eval-expr-bifurcan-with impl) program)]
     (mapv (fn [clj bifurcan]
             (is (= (:clj clj) (:clj bifurcan))))
           clj
           bifurcan)
     {:clj clj
      :bifurcan bifurcan})))

(defmacro def-imm-test
  "Defines a new test for immutability. Takes a var name, a text description,
  the number of iterations, and a vector of types. Optionally takes an impl to
  evaluate against, defaulting to Map, Set and List."
  ([name desc iterations types]
   `(def-imm-test ~name ~desc ~iterations ~types default-impl))
  ([name desc iterations types impl]
   `(deftest ~name
      (checking ~desc ~iterations
                [program# (program-gen ~types)]
                (eval-compare ~impl program#)))))

(def-imm-test list-test "Lists are immutable" iterations [:list])
(def-imm-test map-test  "Maps are immutable"  iterations [:map])
(def-imm-test set-test  "Sets are immutable"  iterations [:set])

;; The same programs, evaluated against the other map and set implementations.
;; Each has its own node code and its own notion of an editor token, so none of
;; them inherits Map's or Set's correctness.

(def-imm-test int-map-test "IntMaps are immutable" iterations [:map]
  {:map (get map-impls "IntMap")})

(def-imm-test sorted-map-test "SortedMaps are immutable" iterations [:map]
  {:map (get map-impls "SortedMap")})

(def-imm-test int-set-test "IntSets are immutable" iterations [:set]
  {:set (get set-impls "IntSet")})

(def-imm-test sorted-set-test "SortedSets are immutable" iterations [:set]
  {:set (get set-impls "SortedSet")})

(deftest iterator-oob-bug
  ; Test for a specific bug where iterators threw out-of-bounds exceptions
  (eval-compare
    [[:set/of 0 -5130]
     [:set/intersection [:var 0] [:var 0]]
     [:set/difference [:var 1] [:set/of 0]]]))

; These tests measure equality by generating a program which produces two equal
; data structures through different paths, and checking that they're equal.

(defn last-expr-index-of-type
  "Picks the index of the last expression of a given type from a program, or
  nil."
  [program type]
  (loop [i (dec (count program))]
    (when (<= 0 i)
      (if (= type (expr-type (nth program i)))
        i
        (recur (dec i))))))

(defn program-ending-in-type-gen
  "A generator which produces programs whose final value is of the given type."
  [type]
  (gen/fmap (fn [program]
              (let [i (last-expr-index-of-type program type)]
                (subvec program 0 (inc i))))
            (gen/such-that seq
                           (program-gen #{type}))))

(defn concat-programs
  "Concatenates two programs together, rewriting the second one's variable
  references."
  ([] [])
  ([program1 program2]
   (let [n (count program1)]
     (into program1
           (map (partial rewrite-vars (fn reindex [[f i]] [f (+ n i)]))
                program2)))))

(defn inline-expr
  "Takes a program and an expression in it. Returns that expression with all
  vars inlined."
  [program expr]
  (rewrite-vars (fn inline [[f i]]
                  (let [expr (nth program i)]
                    (if (and (vector? expr) (= :var (first expr)))
                      (recur expr)
                      expr)))
                expr))

(defn uninline-expr
  "Takes an expression and un-inlines nested expressions to vars, returning a
  program."
  [expr]
  (if (vector? expr)
    ; Turn each vector argument into a program
    (let [sub-programs (->> (rest expr)
                            (filter vector?)
                            (mapv uninline-expr))
          ; What will their result indices be?
          arg-vars (->> sub-programs
                        (map count)
                        (reductions + 0)
                        next
                        ; a sub-program's result is its last expression
                        (map dec))
          ; Which means our new expression is
          expr (loop [expr          (transient expr)
                      i             1
                      sub-program-i 0]
                 (cond ; Done iterating
                       (= i (count expr))
                       (persistent! expr)

                       ; Sub-program; rewrite to a variable
                       (vector? (nth expr i))
                       (let [var [:var (nth arg-vars sub-program-i)]]
                         (recur (assoc! expr i var)
                                (inc i)
                                (inc sub-program-i)))

                       ; Literal
                       true
                       (recur expr (inc i) sub-program-i)))]
      (conj (reduce concat-programs sub-programs) expr))
    ; Just a literal
    [expr]))

(defn set-program-gen
  "Takes a set and returns a generator of programs which produces that
  set as their final value."
  ([s]
   (->> [; Empty set
         (when (empty? s)
           (gen/return :set/empty))

         ; Build the set directly with set/of
         (gen/fmap (partial into [:set/of])
                   (gen/shuffle s))]
        (remove nil?)
        vec
        gen/one-of
        (gen/fmap uninline-expr))))

(defn map-program-gen
  "Takes a Clojure map and returns a generator of programs which produce that
  map as their final value."
  [m]
  (->> [(when (empty? m)
          (gen/return :map/empty))

        ; Build the map directly with map/of
        (gen/fmap (fn [pairs] (into [:map/of] (mapcat identity pairs)))
                  (gen/shuffle (vec m)))

        ; Build it one entry at a time, in a shuffled order
        (gen/fmap (fn [pairs]
                    (reduce (fn [expr [k v]] [:map/put expr k v])
                            :map/empty
                            pairs))
                  (gen/shuffle (vec m)))]
       (remove nil?)
       vec
       gen/one-of
       (gen/fmap uninline-expr)))

(defn list-program-gen
  "Takes a Clojure vector and returns a generator of programs which
  produce that list as their final value."
  [l]
  (->> [; Empty list
        (when (empty? l)
          (gen/return :list/empty))

        ; Build the list with list/of
        (gen/return (into [:list/of] l))

        ; Append one element at a time
        (gen/return (reduce (fn [expr x] [:list/add-last expr x])
                            :list/empty
                            l))

        ; Prepend one element at a time, from the back
        (gen/return (reduce (fn [expr x] [:list/add-first expr x])
                            :list/empty
                            (reverse l)))

        ; Split in two and concatenate
        (gen/fmap (fn [i]
                    [:list/concat
                     (into [:list/of] (subvec l 0 i))
                     (into [:list/of] (subvec l i))])
                  (gen/choose 0 (count l)))]
       (remove nil?)
       vec
       gen/one-of
       (gen/fmap uninline-expr)))

(defn value-program-gen
  "Takes a Clojure structure and returns a generator of programs which produce
  that structure as its final value."
  [x]
  (cond
    (set?    x) (set-program-gen x)
    (map?    x) (map-program-gen x)
    (vector? x) (list-program-gen x)))

(defprotocol IsEqual
  (is-equal [a b] "Are these two Bifurcan collections equal in various ways?
                  Performs clojure.test assertions and returns false if any
                  fail."))

(extend-protocol IsEqual
  IList
  (is-equal [a ^IList b]
    (and (is (= a b))
         (is (= (.size a) (.size b)))
         (is (= (.hashCode a) (.hashCode b)))
         (is (= (datafy a) (datafy b)))))

  IMap
  (is-equal [a ^IMap b]
    (and (is (= a b))
         (is (= (.size a) (.size b)))
         (is (= (.hashCode a) (.hashCode b)))
         (is (= (datafy a) (datafy b)))
         (is (= (set (datafy (.entries a)))
                (set (datafy (.entries b)))))
         (is (= (.keys a) (.keys b)))
         (is (= (set (.values a)) (set (.values b))))
         (is (.containsAll a b))
         (is (.containsAll b a))
         (every? (fn [k]
                   (and (is (.contains a k))
                        (is (.contains b k))))
                 (set/union (datafy (.keys a))
                            (datafy (.keys b))))))
  ISet
  (is-equal [a ^ISet b]
    (and (is (= a b))
         (is (= (.size a) (.size b)))
         (is (= (.hashCode a) (.hashCode b)))
         (is (= (datafy a) (datafy b)))
         (is (= (set (datafy (.elements a)))
                (set (datafy (.elements b)))))
         (is (.containsAll a b))
         (is (.containsAll b a))
         (every? (fn [x]
                   (and (is (.contains a x))
                        (is (.contains b x))))
                 (set/union (datafy a) (datafy b))))))

(defn- check-equal-programs
  "Evaluates two programs which should produce equal values, and checks that
  Bifurcan agrees. Equal collections must also agree on hashCode, since
  otherwise they can't be used together in a hashed collection."
  [impl v p1 p2]
  (let [eval-expr (partial eval-expr-bifurcan-with impl)
        v1        (eval-last eval-expr p1)
        v2        (eval-last eval-expr p2)]
    (when-not (is-equal v1 v2)
      (println "\n-----------------\n")
      (println "; Value: " (pr-str v))
      (prn)
      (println "; Program 1")
      (print-bifurcan-preamble)
      (print-program-bifurcan p1)
      (prn)
      (println "; Program 2")
      (print-program-bifurcan (count p1) p2))))

(defmacro def-equal-test
  "Defines a test that two differently constructed collections of the given
  type compare equal. Note that we intentionally *don't* generate a program
  which efficiently produces a single value v--we generate a program which
  happens to incidentally produce v, but probably does lots of other stuff. We
  want to make sure that those other operations don't inadvertently mess up v
  in some way!"
  ([name desc type]
   `(def-equal-test ~name ~desc ~type default-impl))
  ([name desc type impl]
   `(deftest ~name
      (checking ~desc iterations
                [p1# (program-ending-in-type-gen ~type)
                 :let [v# (eval-last eval-expr-clj p1#)]
                 p2# (value-program-gen v#)]
                (check-equal-programs ~impl v# p1# p2#)))))

(def-equal-test set-equal-test
  "two ways of constructing the same set should be equal" :set)

(def-equal-test map-equal-test
  "two ways of constructing the same map should be equal" :map)

(def-equal-test list-equal-test
  "two ways of constructing the same list should be equal" :list)

(def-equal-test int-map-equal-test
  "two ways of constructing the same IntMap should be equal" :map
  {:map (get map-impls "IntMap")})

(def-equal-test sorted-map-equal-test
  "two ways of constructing the same SortedMap should be equal" :map
  {:map (get map-impls "SortedMap")})

(def-equal-test int-set-equal-test
  "two ways of constructing the same IntSet should be equal" :set
  {:set (get set-impls "IntSet")})

(def-equal-test sorted-set-equal-test
  "two ways of constructing the same SortedSet should be equal" :set
  {:set (get set-impls "SortedSet")})
