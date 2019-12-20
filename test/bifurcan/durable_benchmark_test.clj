(ns bifurcan.durable-benchmark-test
  (:require
   [potemkin :as p :refer (doary doit)]
   [clojure.string :as str]
   [clojure.java.shell :as sh]
   [clojure.java.io :as io]
   [byte-streams :as bs]
   [byte-transforms :as bt]
   [bifurcan.test-utils :as u])
  (:import
   [java.lang.management
    ManagementFactory]
   [java.nio
    ByteBuffer]
   [java.util.concurrent
    ThreadLocalRandom]
   [io.lacuna.bifurcan.durable.allocator
    SlabAllocator
    SlabAllocator$SlabBuffer]
   [io.lacuna.bifurcan
    IEntry
    DurableInput
    DurableOutput
    Map
    DurableMap
    DurableEncodings
    DurableEncodings$Codec]
   [io.lacuna.bifurcan.durable
    Util]
   [io.lacuna.bifurcan.hash
    PerlHash]
   [com.sleepycat.je
    EnvironmentConfig
    Environment
    DatabaseConfig
    Database
    DatabaseEntry
    LockMode
    OperationStatus]
   [org.rocksdb
    RocksDB
    Options
    FlushOptions]))

(RocksDB/loadLibrary)

;;; RocksDB

(defn rdb-open [dir create?]
  (.mkdirs (io/file dir))
  (RocksDB/open
    (doto (Options.)
      (.setCreateIfMissing create?))
    dir))

(defn rdb-get [^RocksDB db key]
  (.get db (bs/to-byte-array key)))

(defn rdb-put! [^RocksDB db key value]
  (.put db (bs/to-byte-array key) (bs/to-byte-array value)))

(defn rdb-scan [^RocksDB db]
  (with-open [it (doto (.newIterator db)
                   (.seekToFirst))]
    (while (.isValid it)
      (let [key (.key it)
            val (.value it)]
        (.next it)))))

(defn rdb-flush! [^RocksDB db]
  (.flush db
    (doto (FlushOptions.)
      (.setWaitForFlush true)
      (.setAllowWriteStall true))))

;;; BerkeleyDB

(defn bdb-open [dir create?]
  (let [f   (io/file dir)
        _   (.mkdirs f)
        env (Environment.
              f
              (doto (EnvironmentConfig.)
                (.setAllowCreate create?)
                (.setReadOnly false)
                (.setLocking false)
                (.setTransactional false)
                (.setSharedCache true)))]
    (.openDatabase env
      nil
      "bifurcan-benchmark"
      (doto (DatabaseConfig.)
        (.setAllowCreate create?)
        (.setReadOnly false)
        (.setDeferredWrite true)))))

(defn bdb-count [^Database db]
  (.count db))

(defn bdb-get [^Database db key]
  (let [k (DatabaseEntry. (bs/to-byte-array key))
        v (DatabaseEntry.)]
    (when (= OperationStatus/SUCCESS (.get db nil k v LockMode/READ_UNCOMMITTED))
      (.getData v))))

(defn bdb-put! [^Database db ^bytes key ^bytes value]
  (.put db nil (DatabaseEntry. key) (DatabaseEntry. value)))

(defn bdb-scan [^Database db]
  (with-open [cursor (.openCursor db nil nil)]
    (loop []
      (when (= OperationStatus/SUCCESS (.getNext cursor (DatabaseEntry.) (DatabaseEntry.) LockMode/READ_UNCOMMITTED))
        (recur)))))

(defn bdb-flush! [^Database db]
  (.sync db))

;;; I/O stats

(defn pidstat? []
  (zero? (:exit (sh/sh "which" "pidstat"))))

(defn pid []
  (-> (.getName (ManagementFactory/getRuntimeMXBean))
    (str/split #"@")
    first))

(defmacro io-stats
  "Measures how much actual disk usage was perfomed by `body`."
  [& body]
  (if-not (pidstat?)
    `(let [start# (System/nanoTime)]
       ~@body
       {:duration (/ (- (System/nanoTime) start#) 1e9)})
    `(let [pid#       (pid)
           p#         (.exec (Runtime/getRuntime) (into-array ["pidstat" "-d" "-p" pid# "1"]))
           pid-stats# (->> p#
                        .getInputStream
                        io/reader
                        line-seq
                        (drop 3)
                        (map (fn [line#]
                               (try
                                 (->> (str/split line# #"\s+")
                                   (remove empty?)
                                   (drop-while #(not= pid# %))
                                   (drop 1)
                                   (take 2)
                                   (map read-string)
                                   (zipmap [:kbs-read :kbs-written]))
                                 (catch Throwable e#
                                   nil))))
                        (remove nil?)
                        (reductions #(merge-with + %1 %2))
                        (partition 2 1))
           ref#       (atom nil)]

       ;; start capturing the stats, and wait until the first value is available
       (future
         (doseq [s# pid-stats#]
           (reset! ref# s#)))
       (while (not @ref#)
         (Thread/sleep 1))

       (let [init# (first @ref#)
             start# (System/nanoTime)]

         (try

           ;; do the thing
           ~@body

           (let [end# (System/nanoTime)]

             ;; pause, and then wait until quiescence
             (Thread/sleep 1)
             (while (not (apply = @ref#))
               (Thread/sleep 1))

             (assoc (merge-with - (first @ref#) init#)
               :duration (/ (- end# start#) 1e9)))

           (finally
             (.destroy p#)))))))

;;; benchmarking

(defn scale-stats [{:keys [kbs-read kbs-written duration]} factor]
  {:read-amplification (/ kbs-read factor)
   :write-amplification (/ kbs-written factor)
   :duration duration})

(defn hashed-key [n]
  (let [ary (byte-array 8)
        buf (ByteBuffer/wrap ary)]
    (.putLong buf (bt/hash (str n) :murmur64))
    ary))

(defn rand-array [n]
  (let [ary (byte-array n)
        buf (ByteBuffer/wrap ary)
        rng (ThreadLocalRandom/current)]
    (while (.hasRemaining buf)
      (.putLong buf (.nextLong rng)))
    ary))

(defn populate! [db put-fn flush-fn keys val-size]
  (doseq [k keys]
    (put-fn db (hashed-key k) (rand-array val-size)))
  (flush-fn db))

(defn random-reads [db get-fn keys]
  (doseq [k keys]
    (get-fn db (hashed-key k))))

(defn benchmark-db [db get-fn scan-fn put-fn flush-fn sizes]
  (into
    (sorted-map)
    (zipmap
      (map #(/ % (Math/pow 2 20)) sizes)
      (->> (cons 0 sizes)
        (partition 2 1)
        (map
          (fn [[a b]]
            ;; we write 8 byte keys + 1016 byte values so each entry is exactly 1kb
            (let [write      (io-stats (populate! db put-fn flush-fn (range a b) 1016))
                  random     (io-stats (random-reads db get-fn (range b)))
                  sequential (io-stats (scan-fn db))]
              {:write      (scale-stats write (- b a))
               :random     (scale-stats random b)
               :sequential (scale-stats sequential b)})))))))

(defn benchmark-databases [n log-steps]
  (sh/sh "rm" "-rf" "/tmp/rocks" "/tmp/bdb")
  (let [sizes (->> (u/log-steps n 2 log-steps)
                (drop (* 20 log-steps))
                (map long))
        rdb   (rdb-open "/tmp/rocks" true)
        bdb   (bdb-open "/tmp/bdb" true)]
    (prn sizes)
    (try
      {:rocksdb (benchmark-db
                  rdb
                  rdb-get
                  rdb-scan
                  rdb-put!
                  rdb-flush!
                  sizes)
       :bdb      (benchmark-db
                   bdb
                   bdb-get
                   bdb-scan
                   bdb-put!
                   bdb-flush!
                   sizes)}
      (finally
        (.close rdb)
        (.close bdb)
        (-> bdb .getEnvironment .close)))))

;;;

(def binary-encoding
  (DurableEncodings/primitive
    "binary"
    4
    (u/->to-int-fn
      (fn [^DurableInput in]
        (PerlHash/hash 0 (.duplicate in))))
    (u/->bi-predicate
      (fn [a b]
        (zero? (Util/compare a b))))
    (comparator
      (fn [a b]
        (Util/compare a b)))
    (u/->predicate
      (fn [_]
        false))
    (DurableEncodings$Codec/undelimited
      (u/->bi-consumer
        (fn [^DurableInput in ^DurableOutput out]
          (.transferFrom out in (.remaining in))))
      (u/->bi-fn
        (fn [^DurableInput in root]
          in)))))

(defn ->durable-input [^bytes ary]
  (DurableInput/from
    (SlabAllocator$SlabBuffer.
      (ByteBuffer/wrap
        ary))))

(defn create-hash-map [dir n]
  (.mkdirs (io/file dir))
  (DurableMap/from
    (->> (range n)
      (map
        (fn [i]
          (IEntry/of
            (->durable-input (hashed-key i))
            (->durable-input (rand-array 1016)))))
      .iterator)
    (DurableEncodings/map
      binary-encoding
      binary-encoding)
    (.toPath (io/file dir))
    1e5))
