(ns bifurcan.rope-tests
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)]
   [bifurcan.test-utils :as u]
   [bifurcan.collection-test :refer [iterations]])
  (:import
   [io.lacuna.bifurcan
    Rope]
   [io.lacuna.bifurcan.nodes
    RopeNodes$Node]
   [io.lacuna.bifurcan.utils
    UnicodeChunk]))

(defn str->chunk [s]
  (UnicodeChunk/from s))

(defn chunk->str [chunk]
  (str (UnicodeChunk/toCharSequence chunk)))

(defn codepoints->str [cs]
  (String. (int-array cs) 0 (count cs)))

(def gen-code-point
  (gen/such-that
    #(or (< 65536 %)
       ;; TODO: figure out why the first doesn't imply the second
       (and (Character/isBmpCodePoint %) (not (Character/isHighSurrogate (char %)))))
    (gen/choose 0 Character/MAX_CODE_POINT)))

(defspec test-ascii-roundtrip 1e4
  (prop/for-all [s gen/string-ascii]
    (= s (-> s str->chunk chunk->str))))

(defspec test-unicode-roundtrip 1e4
  (prop/for-all [s (gen/vector gen-code-point 0 32)]
    (let [s (codepoints->str s)]
      (= s (-> s str->chunk chunk->str)))))

(defspec test-concat 1e4
  (prop/for-all [a (gen/vector gen-code-point 0 32)
                 b (gen/vector gen-code-point 0 32)]
    (let [a (codepoints->str a)
          b (codepoints->str b)]
      (= (str a b)
        (-> (UnicodeChunk/concat (UnicodeChunk/from a) (UnicodeChunk/from b))
          UnicodeChunk/toCharSequence
          str)))))

;;;

(def gen-string-unicode
  (gen/fmap
    codepoints->str
    (gen/vector (gen/elements [0 0x80 0x800 0x10000]) 0 33)))

(def actions
  {:insert [gen/pos-int gen-string-unicode]
   :remove [gen/pos-int gen/pos-int]
   :concat [gen-string-unicode]})

(def rope-actions
  {:insert (fn [^Rope r idx s]
             (.insert r (min (.size r) idx) s))
   :remove (fn [^Rope r s e]
             (let [[s e] (sort [s e])
                   s (max 0 (min (dec (.size r)) s))
                   e (min (.size r) e)]
               (.remove r s e)))
   :concat (fn [^Rope r s]
             (.concat r (Rope/from s)))})

(def string-actions
  {:insert (fn [^String a idx b]
             (let [cnt (.codePointCount a 0 (count a))
                   idx (.offsetByCodePoints a 0 (min cnt idx))]
               (str (.substring a 0 idx) b (.substring a idx (count a)))))
   :remove (fn [^String a s e]
             (let [cnt (.codePointCount a 0 (count a))
                   [s e] (sort [s e])
                   s (max 0 (min (dec cnt) s))
                   e (min cnt e)
                   s (.offsetByCodePoints a 0 s)
                   e (.offsetByCodePoints a 0 e)]
               (str (.substring a 0 s) (.substring a e (count a)))))
   :concat str})

(u/def-collection-check test-rope iterations actions
  [a (Rope/from "") rope-actions
   b "" string-actions]
  (= (str a) b))

;;;

(defn compare-outcomes [actions]
  (let [a (u/apply-actions actions (Rope/from "") rope-actions)
        b (u/apply-actions actions "" string-actions)]
   [a b (= (str a) b)]))

(defn ->tree [^RopeNodes$Node n]
  (let [nodes (.numNodes n)]
    {:shift (.shift n)
     :units (take nodes (.unitOffsets n))
     :points (take nodes (.pointOffsets n))
     :nodes (->> n
              .nodes
              (take nodes)
              (map #(if (instance? RopeNodes$Node %)
                      (->tree %)
                      (UnicodeChunk/toString %))))}))
