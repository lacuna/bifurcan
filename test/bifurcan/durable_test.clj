(ns bifurcan.durable-test
  (:require
   [primitive-math :as p]
   [byte-streams :as bs]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :as ct :refer [defspec]]
   [bifurcan.collection-test :as coll]
   [bifurcan.test-utils :as u :refer [iterations]])
  (:import
   [java.nio
    ByteBuffer]
   [io.lacuna.bifurcan
    ICollection
    IDurableCollection
    IMap
    Map
    List
    Maps
    IEntry
    DurableInput
    DurableOutput
    DurableMap
    DurableList
    IDurableEncoding
    DurableEncodings
    DurableEncodings$Codec]
   [io.lacuna.bifurcan.utils
    Bits]
   [io.lacuna.bifurcan.durable.allocator
    GenerationalAllocator]
   [io.lacuna.bifurcan.hash
    PerlHash]
   [io.lacuna.bifurcan.durable.codecs
    HashMap
    SkipTable
    SkipTable$Writer
    SkipTable$Entry]
   [io.lacuna.bifurcan.durable.io
    DurableBuffer
    BufferInput]
   [io.lacuna.bifurcan.durable
    Util
    BlockPrefix
    BlockPrefix$BlockType
    ChunkSort
    Bytes]))

(set! *warn-on-reflection* true)

(def ^java.nio.file.Path test-dir (.toPath (io/file "/tmp/bifurcan-tests")))

(-> test-dir .toFile .mkdirs)

(defn clear-test-dir []
  (doseq [^java.io.File f (->> test-dir .toFile .listFiles)]
    (.delete f)))

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

(def ^IDurableEncoding edn-encoding
  (DurableEncodings/unityped
    (DurableEncodings/primitive
      "edn"
      4
      (DurableEncodings$Codec/undelimited
       (u/->bi-consumer
         (fn [o ^DurableOutput out]
           (.write out (.getBytes (pr-str o) "utf-8"))))
       (u/->bi-fn
         (fn [^DurableInput in root]
           (let [ary (byte-array (.remaining in))]
             (.readFully in ary)
             (edn/read-string (String. ary "utf-8")))))))))

(defn no-leaks? []
  (and
    (zero? (GenerationalAllocator/diskAllocations))
    (zero? (GenerationalAllocator/memoryAllocations))))

(defn free! [^DurableInput in]
  (.close in)
  (doto (no-leaks?)
    (assert
      (str "we have leaks! disk: " (GenerationalAllocator/diskAllocations)
        " mem: " (GenerationalAllocator/memoryAllocations)))))

;;; Util

(defspec test-uvlq-roundtrip iterations
  (prop/for-all [n gen/large-integer]
    (let [out (doto (DurableBuffer.)
                (.writeUVLQ n))
          in  (.toInput out)]
      (try
        (= n (.readUVLQ in))
        (finally
          (free! in))))))

(defspec test-vlq-roundtrip iterations
  (prop/for-all [n gen/large-integer]
    (let [out (doto (DurableBuffer.)
                (.writeVLQ n))
          in  (.toInput out)]
      (try
        (= n (.readVLQ in))
        (finally
          (free! in))))))

(defspec test-prefixed-vlq-roundtrip iterations
  (prop/for-all [n gen-pos-int
                 bits (gen/choose 0 6)]
    (let [out (DurableBuffer.)
          _   (Util/writePrefixedUVLQ 0 bits n out)
          in  (.toInput out)]
      (try
        (= n (Util/readPrefixedUVLQ (.readByte in) bits in))
        (finally
          (free! in))))))

;;; Prefix

(defspec test-prefix-roundtrip iterations
  (prop/for-all [n gen-pos-int
                 type (->> (BlockPrefix$BlockType/values)
                        (remove
                          #{BlockPrefix$BlockType/DIFF
                            BlockPrefix$BlockType/COLLECTION
                            BlockPrefix$BlockType/EXTENDED
                            BlockPrefix$BlockType/DEPENDENCY})
                        (map gen/return)
                        gen/one-of)]
    (let [out (DurableBuffer.)
          p   (BlockPrefix. n type)
          _   (.encode p out)
          in  (.toInput out)]
      (try
        (= p (BlockPrefix/decode in))
        (finally
          (free! in))))))

;; SkipTable

(defn create-skip-table [entries]
  (let [writer   (SkipTable$Writer.)
        _        (doseq [[index offset] entries]
                   (.append writer index offset))
        out      (DurableBuffer.)
        _        (.flushTo writer out)
        in       (.toInput out)
        contents (.sliceBlock in BlockPrefix$BlockType/TABLE)
        buf      (Bytes/allocate (.size contents))
        _        (.read contents buf)
        _        (free! in)]
    (SkipTable/decode (.pool (BufferInput. (.flip buf))) (.tiers writer))))

(defn print-skip-table [^DurableInput in]
  (->> (repeatedly #(when (pos? (.remaining in)) (.readUVLQ in)))
    (take-while identity)))

(defspec test-durable-skip-table iterations
  (prop/for-all [entries (gen/list (gen/tuple gen/int gen/int))]
    (let [entries  (->> entries (sort-by first) (partition-by first) (map first))
          m        (create-skip-table entries)
          expected (into {} entries)]
      (coll/map= expected m))))

;;; SortedChunk

(def hash-fn (u/->to-long-fn hash))

(defspec test-sort-map-entries iterations
  (prop/for-all [entries (gen/list (gen/tuple gen-pos-int gen-pos-int))]
    (let [m (into {} entries)
          m' (->> (HashMap/sortIndexedEntries
                    (Map/from ^java.util.Map m)
                    hash-fn)
               iterator-seq
               (map
                 (fn [^IEntry e]
                   [(.key e) (.value e)])))]
      (and
        (= (sort-by #(hash (key %)) m) m')
        (no-leaks?)))))

;;; DurableMap

(defn save [^ICollection c]
  (let [^IDurableCollection c' (.save c edn-encoding test-dir)]
    (-> c' .root .path .toFile .deleteOnExit)
    c'))

(def durable-map-actions
  (-> coll/map-actions
    (dissoc :diff-wrap)
    (assoc :save [])))

(def bifurcan-durable-map
  (assoc coll/bifurcan-map
    :save save))

(u/def-collection-check test-durable-map 1e6 durable-map-actions
  []
  [m (Map.) bifurcan-durable-map
   m' {} coll/clj-map]
  (try
    (coll/map= m' m)
    (finally
      (clear-test-dir))))

;;; DurableList

(def durable-list-actions
  (-> coll/list-actions
    (dissoc :diff-wrap)
    (assoc :save [])))

(def bifurcan-durable-list
  (assoc coll/bifurcan-list
    :save save))

(u/def-collection-check test-durable-list iterations durable-list-actions
  []
  [m (List.) bifurcan-durable-list
   m' [] coll/clj-list]
  (try
    (coll/list= m' m)
    (finally
      (clear-test-dir))))

 #_(defspec test-durable-list iterations
  (prop/for-all [l (coll/list-gen #(List.))]
    (let [out (DurableBuffer.)
          _   (DurableList/encode (.iterator ^Iterable l) edn-encoding out)
          in  (.toInput out)
          l'  (DurableList/decode edn-encoding nil (.pool in))]
      (and
        (= l l')
        (free! in)))))


(comment
  (def p (.toPath (io/file "/tmp")))
  (-> (Map.)
    (.put 1 1)
    (.save edn-encoding p)
    (.put 2 2)
    (.put 1 42)
    (.save edn-encoding p)))
