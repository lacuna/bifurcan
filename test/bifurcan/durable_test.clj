(ns bifurcan.durable-test
  (:require
   [primitive-math :as p]
   [byte-streams :as bs]
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer [defspec]]
   [bifurcan.test-utils :as u :refer [iterations]])
  (:import
   [java.io
    ByteArrayOutputStream
    ByteArrayInputStream
    DataOutputStream
    DataInputStream]
   [java.nio
    ByteBuffer]
   [io.lacuna.bifurcan
    Map
    IEntry
    DurableInput
    DurableOutput
    DurableConfig
    DurableConfig$Codec]
   [io.lacuna.bifurcan.hash
    PerlHash]
   [io.lacuna.bifurcan.durable
    Util
    BlockPrefix
    BlockPrefix$BlockType
    HashTable
    HashTable$Entry
    HashTable$Writer
    SkipTable
    SkipTable$Writer
    SkipTable$Entry
    ByteChannelDurableInput
    DurableHashMap]))

(defn ->to-int-fn [f]
  (reify java.util.function.ToIntFunction
    (applyAsInt [_ x]
      (f x))))

(defn ->fn [f]
  (reify java.util.function.Function
    (apply [_ x]
      (f x))))

(def gen-pos-int
  (gen/such-that
    #(not= 0 %)
    (gen/fmap
      #(Math/abs (p/int %))
      gen/large-integer)))

(def gen-small-pos-int
  (gen/such-that
    #(not= 0 %)
    (gen/fmap
      #(Math/abs (p/int %))
      gen/int)))

;;; Util

(defspec test-vlq-roundtrip iterations
  (prop/for-all [n gen-pos-int]
    (let [out (ByteArrayOutputStream. 12)
          _   (Util/writeVLQ n (DataOutputStream. out))
          in  (->> out
                .toByteArray
                ByteArrayInputStream.
                DataInputStream.)]
      (= n (Util/readVLQ in)))))

(defspec test-prefixed-vlq-roundtrip iterations
  (prop/for-all [n gen-pos-int
                 bits (gen/choose 0 6)]
    (let [out (ByteArrayOutputStream. 12)
          _   (Util/writePrefixedVLQ 0 bits n (DataOutputStream. out))
          in  (->> out
                .toByteArray
                ByteArrayInputStream.
                DataInputStream.)]
      (= n (Util/readPrefixedVLQ (.readByte in) bits in)))))

;;; Prefix

(defspec test-prefix-roundtrip iterations
  (prop/for-all [n gen-pos-int
                 type (->> (BlockPrefix$BlockType/values)
                        (map gen/return)
                        gen/one-of)
                 checksum? gen/boolean
                 checksum (gen/fmap #(p/int %) gen/int)]
    (let [out (ByteArrayOutputStream. 12)
          p   (if checksum?
                (BlockPrefix. n type checksum)
                (BlockPrefix. n type))
          _   (BlockPrefix/write p (DataOutputStream. out))
          in  (->> out
                .toByteArray
                ByteArrayInputStream.
                DataInputStream.)
          p' (BlockPrefix/read in)]
      #_(prn p p')
      (= p p'))))

;;; HashTable

(defn hash-table-writer [n]
  (HashTable$Writer.
    [(ByteBuffer/allocate (HashTable/requiredBytes (Math/floor (/ n 2)) 0.95))
     (ByteBuffer/allocate (HashTable/requiredBytes (Math/ceil (/ n 2)) 0.95))]))

(defn put-entry! [^HashTable$Writer writer hash offset]
  (.put writer hash offset))

(defn encode-hash-table [entries]
  (let [hash->offset (into {} entries)
        writer       (hash-table-writer (count hash->offset))]
    (doseq [[hash offset] hash->offset]
      (put-entry! writer hash offset))
    (-> writer
      .buffers
      .iterator
      iterator-seq
      (ByteChannelDurableInput/from 1e3))))

(defn get-entry [^DurableInput in hash]
  (.seek in 0)
  (HashTable/get in hash))

(defspec test-durable-hash-table iterations
  (prop/for-all [entries (gen/such-that
                           (complement empty?)
                           (gen/list (gen/tuple gen-pos-int gen-pos-int)))]
    (let [in (encode-hash-table entries)]
      (every?
        (fn [[hash offset]]
          (= offset (.offset ^HashTable$Entry (get-entry in hash))))
        (into {} entries)))))

;; SkipTable

(defn skip-table-writer []
  (SkipTable$Writer.))

(defn append-entry! [^SkipTable$Writer writer index offset]
  (.append writer index offset))

(defn encode-skip-table [entries]
  (let [writer (skip-table-writer)
        entries (reductions #(map + %1 %2) entries)]
    (doseq [[index offset] entries]
      (append-entry! writer index offset))
    (-> writer
      .buffers
      .iterator
      iterator-seq
      (ByteChannelDurableInput/from 1e3))))

(defn print-skip-table [^DurableInput in]
  (->> (repeatedly #(when (pos? (.remaining in)) (.readVLQ in)))
    (take-while identity)))

(defn lookup-entry [^DurableInput in idx]
  (.seek in 0)
  (SkipTable/lookup in idx))

(defspec test-durable-skip-table iterations
  (prop/for-all [entries (gen/such-that
                           (complement empty?)
                           (gen/list (gen/tuple gen-small-pos-int gen-small-pos-int)))]
    (let [in (encode-skip-table entries)]
      (every?
        (fn [[index offset]]
          (let [^SkipTable$Entry e (lookup-entry in index)]
            (and
              (= index (.index e))
              (= offset (.offset e)))))
        (reductions #(map + %1 %2) entries)))))

;;; DurableHashMap

(def hash-fn
  (->to-int-fn hash))

(defspec test-sort-entries iterations
  (prop/for-all [entries (gen/list (gen/tuple gen-pos-int gen-pos-int))]
    (let [m (into {} entries)
          m' (->> (DurableHashMap/sortEntries
                    (Map/from ^java.util.Map m)
                    hash-fn)
               iterator-seq
               (map
                 (fn [^IEntry e]
                   [(.key e) (.value e)])))]
      (= (sort-by #(hash (key %)) m) m'))))
