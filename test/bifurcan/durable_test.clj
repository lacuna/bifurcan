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
    DurableInput
    DurableOutput]
   [io.lacuna.bifurcan.hash
    PerlHash]
   [io.lacuna.bifurcan.durable
    Util
    BlockPrefix
    BlockPrefix$BlockType
    HashTable
    HashTable$Entry
    HashTable$Writer
    ByteChannelDurableInput
    #_DurableHashMap
    #_DurableHashMap$Entry]))

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

(defn entry [hash offset]
  (HashTable$Entry. hash offset))

(defn table-writer [n]
  (HashTable$Writer.
    [(ByteBuffer/allocate (HashTable/requiredBytes (Math/floor (/ n 2)) 0.95))
     (ByteBuffer/allocate (HashTable/requiredBytes (Math/ceil (/ n 2)) 0.95))]))

(defn put-entry! [^HashTable$Writer writer entry]
  (.put writer entry))

(defn encode-table [entries]
  (let [hash->offset (into {} entries)
        writer       (table-writer (count hash->offset))]
    (doseq [[hash offset] hash->offset]
      (put-entry! writer (entry hash offset)))
    (-> writer
      .buffers
      .iterator
      iterator-seq
      (ByteChannelDurableInput/from 1e3))))

(defn get-entry [^DurableInput in hash]
  (.seek in 0)
  (HashTable/get in hash))

(defspec test-durable-hashtable iterations
  (prop/for-all [entries (gen/such-that
                           (complement empty?)
                           (gen/list (gen/tuple gen-pos-int gen-pos-int)))]
    (let [in (encode-table entries)]
      (every?
        (fn [[hash offset]]
          (= offset (.offset ^HashTable$Entry (get-entry in hash))))
        (into {} entries)))))

;;; DurableHashMap

(def serialize
  (->fn #(-> % pr-str bs/to-byte-buffer)))

(def hash-fn
  (->to-int-fn #(PerlHash/hash %)))

(defn deserialize [x]
  (-> x bs/to-string read-string))

#_(defspec test-chunk-sort 1e3
  (prop/for-all [entries (gen/list (gen/tuple gen-pos-int gen-pos-int))]
    (let [m (into {} entries)
          m' (->> (DurableHashMap/chunkSort
                    (Map/from ^java.util.Map m)
                    hash-fn
                    serialize
                    serialize
                    1e2
                    1e6)
               iterator-seq
               (map
                 (fn [^DurableHashMap$Entry e]
                   [(deserialize (.key e))
                    (deserialize (.value e))]))
               (into {}))]
      (= m m'))))
