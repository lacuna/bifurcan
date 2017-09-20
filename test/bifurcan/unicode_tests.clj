(ns bifurcan.unicode-tests
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer (defspec)])
  (:import
   [io.lacuna.bifurcan
    Rope]
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
