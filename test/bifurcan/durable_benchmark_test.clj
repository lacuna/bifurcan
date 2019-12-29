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
    GenerationalAllocator
    SlabAllocator
    SlabAllocator$SlabBuffer]
   [io.lacuna.bifurcan.durable.io
    BufferInput
    DurableBuffer]
   [io.lacuna.bifurcan
    IEntry
    DurableInput
    DurableOutput
    IMap
    IList
    Map
    DurableMap
    DurableList
    DurableEncodings
    DurableEncodings$Codec]
   [io.lacuna.bifurcan.durable
    Bytes]
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

(defn scale-stats [{:keys [kbs-read kbs-written duration]} entries]
  {:read-amplification (when kbs-read (/ kbs-read entries))
   :write-amplification (when kbs-written (/ kbs-written entries))
   :us-per-entry (* 1e6 (/ duration entries))})

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

(defn random-reads [db get-fn n]
  (dotimes [_ (/ n 10)]
    (get-fn db (hashed-key (rand-int n)))))

(defn benchmark-db [name dir db get-fn scan-fn put-fn flush-fn sizes]
  (zipmap
    (map #(/ % (Math/pow 2 20)) sizes)
    (->> (cons 0 sizes)
      (partition 2 1)
      (map
        (fn [[a b]]
          (prn name b)
          ;; we write 8 byte keys + 1016 byte values so each entry is exactly 1kb
          (time
            (let [write      (io-stats (populate! db put-fn flush-fn (range a b) 1016))
                  sequential (io-stats (scan-fn db))
                  random     (io-stats (random-reads db get-fn b))
                  size       (directory-size dir)]
              {:write                 (scale-stats write (- b a))
               :random                (scale-stats random (/ b 10))
               :sequential            (scale-stats sequential b)
               :storage-amplification (/ (double size) b)})))))))

;;;

(def binary-encoding
  (DurableEncodings/primitive
    "binary"
    2
    (u/->to-int-fn
      (fn [^DurableInput in]
        (PerlHash/hash 0 (.duplicate in))))
    (u/->bi-predicate
      (fn [^DurableInput a ^DurableInput b]
        (zero? (Bytes/compareInputs a b))))
    (comparator
      (fn [a b]
        (Bytes/compareInputs a b)))
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

(defn create-list [dir n val-size]
  (.mkdirs (io/file dir))
  (DurableList/from
    (.iterator (repeatedly n #(->durable-input (rand-array val-size))))
    (DurableEncodings/list binary-encoding)
    (.toPath (io/file dir))))

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

(defn benchmark-bifurcan [dir name sizes create-fn get-fn]
  (clear-directory dir)
  (try
    (zipmap
      (map #(/ % (Math/pow 2 20)) sizes)
      (->> (cons 0 sizes)
        (map
          (fn [n]
            (prn name n)
            (time
              (let [m          (atom nil)
                    write      (io-stats
                                 (reset! m
                                   (create-fn dir n)))
                    sequential (io-stats
                                 (doit [e @m]
                                   e))
                    random     (io-stats
                                 (let [^IMap m @m]
                                   (dotimes [_ (/ n 10)]
                                     (get-fn m (rand-int n)))))
                    size       (directory-size dir)]
                (prn 'log-size (/ (GenerationalAllocator/logSize) (double (* 1024 1024 1024))))
                (clear-directory dir)
                [n {:write      write
                    :random     random
                    :sequential sequential
                    :size-kbs   size}]))))
        (partition 2 1)
        (map
          (fn [[[a m-a] [b m-b]]]
            {:write                 (scale-stats (merge-with - (:write m-b) (:write m-a)) (- b a))
             :random                (scale-stats (:random m-b) (/ b 10))
             :sequential            (scale-stats (:sequential m-b) b)
             :storage-amplification (/ (:size-kbs m-b) (double b))}))))
    (finally
      (clear-directory dir))))

;;;

(defn benchmark-rocks [dir sizes]
  (clear-directory dir)
  (let [^RocksDB rdb (rdb-open dir true)]
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
  (let [^Database bdb (bdb-open dir true)]
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

(def benchmark-edn "benchmarks/data/durable.edn")

(defn run-database-benchmarks [n log-steps]
  (let [sizes (->> (u/log-steps n 2 log-steps)
                (drop (* 20 log-steps))
                (map long))]
    (prn sizes)
    (io/make-parents benchmark-edn)
    (spit
      benchmark-edn
      (pr-str
        (merge-with
          #(or %2 %1)
          (try
            (read-string (slurp benchmark-edn))
            (catch Exception e
              nil))
          {"RocksDB"              (benchmark-rocks "/tmp/rocks" sizes)
           "Berkeley DB"          (benchmark-berkeley "tmp/bdb" sizes)
           "bifurcan.DurableMap"  (benchmark-bifurcan "/tmp/bifurcan" 'bifurcan-hash-map sizes
                                    #(create-hash-map %1 %2 1016)
                                    #(-> ^IMap %1
                                       (.get (->durable-input (hashed-key %2)))
                                       .get))
           "bifurcan.DurableList" (benchmark-bifurcan "/tmp/bifurcan" 'bifurcan-list sizes
                                    #(create-list %1 %2 1016)
                                    #(.nth ^IList %1 %2))})))))

(def benchmark-csvs
  {"durable_write_amplification"
   (fn [db-data]
     {" writes" (-> db-data :write :write-amplification)
      " reads"  (-> db-data :write :read-amplification)})

   "durable_write_duration"
   (fn [db-data]
     {"" (-> db-data :write :us-per-entry)})

   "durable_random_read_amplification"
   (fn [db-data]
     {"" (-> db-data :random :read-amplification)})

   "durable_random_read_duration"
   (fn [db-data]
     {"" (-> db-data :random :us-per-entry)})

   "durable_sequential_read_amplification"
   (fn [db-data]
     {"" (-> db-data :sequential :read-amplification)})

   "durable_sequential_read_duration"
   (fn [db-data]
     {"" (-> db-data :sequential :us-per-entry)})

   "durable_storage_amplification"
   (fn [db-data]
     {"" (-> db-data :storage-amplification)})})

(defn generate-csvs []
  (let [data  (read-string (slurp benchmark-edn))
        dbs   (keys data)
        sizes (-> data
                (get (first dbs))
                keys)]
    (doseq [[n f] benchmark-csvs]
      (let [db->size->data (reduce
                             #(assoc-in %1 %2 (f (get-in data %2)))
                             {}
                             (for [db dbs, size sizes] [db size]))
            field->path (->> db->size->data
                         (mapcat
                           (fn [[db size->data]]
                             (->> size->data
                               vals
                               first
                               keys
                               (map #(vector (str (name db) %) [db %])))))
                         (into {}))]
        (spit
          (str "benchmarks/data/" n ".csv")
          (with-out-str
           (println (->> field->path keys (cons "size") (interpose ",") (apply str)))
           (doseq [s sizes]
             (println
               (->> field->path
                 vals
                 (map
                   (fn [[db field]]
                     (get-in db->size->data [db s field])))
                 (cons s)
                 (interpose ",")
                 (apply str))))))))))

(defn -main [task & args]
  (case task
    "benchmark"
    (let [gbs (read-string (first args))
          steps (read-string (or (second args) "1"))]
       (run-database-benchmarks (* gbs 1024 1024) steps)
       (generate-csvs)))

  (flush)
  (Thread/sleep 100)
  (System/exit 0))
