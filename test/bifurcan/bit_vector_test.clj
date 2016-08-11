(ns bifurcan.bit-vector-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)])
  (:import
   [io.lacuna.bifurcan.utils
    Bits
    BitVector
    ChunkVector]))

(defn ->binary-string [x]
  (let [s (Long/toBinaryString x)]
    (str
      (->> "0"
        (repeat (- 64 (count s)))
        (apply str))
      s)))

(defn interleaved [bits a b]
  (->> [a b]
    (map ->binary-string)
    (map #(->> % (take-last bits) (apply str)))
    (apply interleave)
    (apply str)))

(defspec interleave-roundtrip 1e4
  (prop/for-all [a (gen/resize Short/MAX_VALUE gen/pos-int)
                 b (gen/resize Short/MAX_VALUE gen/pos-int)]
    (= (interleaved 16 a b)
      (->> (BitVector/interleave 16 (long-array [a b]))
        (map ->binary-string)
        (apply str)
        (take-last 32)
        (apply str)))))
