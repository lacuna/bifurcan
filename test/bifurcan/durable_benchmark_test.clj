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
   [io.lacuna.bifurcan.durable.io
    BufferInput]
   [io.lacuna.bifurcan
    IEntry
    DurableInput
    DurableOutput
    IMap
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

;;;

(defn clear-directory [dir]
  (.mkdirs (io/file dir))
  (sh/sh "rm" "-rf" dir))

(defn directory-size [dir]
  (-> (sh/sh "du" "-k" (str dir "/"))
    :out
    bs/to-line-seq
    last
    (str/split #"\t")
    first
    read-string))

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
             (Thread/sleep 1000)
             (while (not (apply = @ref#))
               (Thread/sleep 1))

             (assoc (merge-with - (first @ref#) init#)
               :duration (/ (- end# start#) 1e9)))

           (finally
             (.destroy p#)))))))

;;; benchmarking

(defn scale-stats [{:keys [kbs-read kbs-written duration]} factor]
  {:read-amplification (when kbs-read (/ kbs-read factor))
   :write-amplification (when kbs-written (/ kbs-written factor))
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

(defn benchmark-db [name dir db get-fn scan-fn put-fn flush-fn sizes]
  (into
    (sorted-map)
    (zipmap
      (map #(/ % (Math/pow 2 20)) sizes)
      (->> (cons 0 sizes)
        (partition 2 1)
        (map
          (fn [[a b]]
            (prn name b)
            ;; we write 8 byte keys + 1016 byte values so each entry is exactly 1kb
            (let [write      (io-stats (populate! db put-fn flush-fn (range a b) 1016))
                  sequential (io-stats (scan-fn db))
                  random     (io-stats (random-reads db get-fn (range b)))]
              {:write      (scale-stats write (- b a))
               :random     (scale-stats random b)
               :sequential (scale-stats sequential b)
               :size-kbs   (directory-size dir)})))))))

;;;

(def binary-encoding
  (DurableEncodings/primitive
    "binary"
    2
    (u/->to-int-fn
      (fn [^DurableInput in]
        (PerlHash/hash 0 (.duplicate in))))
    (u/->bi-predicate
      (fn [a b]
        (zero? (Util/compareInputs a b))))
    (comparator
      (fn [a b]
        (Util/compareInputs a b)))
    (u/->predicate
      (fn [_]
        false))
    (DurableEncodings$Codec/undelimited
      (u/->bi-consumer
        (fn [^DurableInput in ^DurableOutput out]
          (.transferFrom out (-> in .duplicate (.seek 0)))))
      (u/->bi-fn
        (fn [^DurableInput in root]
          in)))))

(defn ->durable-input [^bytes ary]
  (BufferInput.
    (ByteBuffer/wrap
      ary)))

(defn open-hash-map [file]
  (DurableMap/open
    (.toPath (io/file file))
    (DurableEncodings/map
      binary-encoding
      binary-encoding)))

(defn create-hash-map [dir n val-size]
  (.mkdirs (io/file dir))
  (DurableMap/from
    (->> (range n)
      (map
        (fn [i]
          (IEntry/of
            (->durable-input (hashed-key i))
            (->durable-input (rand-array val-size)))))
      .iterator)
    (DurableEncodings/map
      binary-encoding
      binary-encoding)
    (.toPath (io/file dir))
    1e5))

(defn benchmark-bifurcan [dir sizes]
  (clear-directory dir)
  (try
    (into
     (sorted-map)
     (zipmap
       (map #(/ % (Math/pow 2 20)) sizes)
       (->> (cons 0 sizes)
         (map
           (fn [n]
             (prn 'bifurcan n)
             (let [m          (atom nil)
                   write      (io-stats
                                (reset! m
                                  (create-hash-map dir n 1016)))
                   sequential (io-stats
                                (doit [e @m]
                                  e))
                   random     (io-stats
                                (dotimes [i n]
                                  (.get @m (->durable-input (hashed-key i)))))]
               [n {:write      write
                   :random     random
                   :sequential sequential
                   :size-kbs   (directory-size dir)}])))
         (partition 2 1)
         (map
           (fn [[[a m-a] [b m-b]]]
             {:write      (scale-stats (merge-with - (:write m-b) (:write m-a)) (- b a))
              :random     (scale-stats (:random m-b) b)
              :sequential (scale-stats (:sequential m-b) b)
              :size-kbs   (:size-kbs m-b)})))))
    (finally
      (clear-directory dir))))

;;;

(defn benchmark-rocks [dir sizes]
  (clear-directory dir)
  (let [rdb (rdb-open dir true)]
    (try
      (benchmark-db
        'rocks
        dir
        rdb
        rdb-get
        rdb-scan
        rdb-put!
        rdb-flush!
        sizes)
      (finally
        (.close rdb)
        (clear-directory dir)))))

(defn benchmark-berkeley [dir sizes]
  (clear-directory dir)
  (let [bdb (bdb-open dir true)]
    (try
      (benchmark-db
        'berkeley
        dir
        bdb
        bdb-get
        bdb-scan
        bdb-put!
        bdb-flush!
        sizes)
      (finally
        (.close bdb)
        (-> bdb .getEnvironment .close)
        (clear-directory dir)))))

(defn benchmark-databases [n log-steps]
  (let [sizes (->> (u/log-steps n 2 log-steps)
                (drop (* 20 log-steps))
                (map long))]
    (prn sizes)
    {:rocksdb  (benchmark-rocks "/tmp/rocks" sizes)
     :bdb      (benchmark-berkeley "tmp/bdb" sizes)
     :bifurcan (benchmark-bifurcan "/tmp/bifurcan" sizes)}))
