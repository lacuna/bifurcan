(ns bifurcan.durable-benchmark-test
  (:require
   [clojure.string :as str]
   [clojure.java.shell :as sh]
   [clojure.java.io :as io]
   [byte-streams :as bs]
   [byte-transforms :as bt])
  (:import
   [java.lang.management
    ManagementFactory]
   [java.nio
    ByteBuffer]
   [java.util.concurrent
    ThreadLocalRandom]
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

;;; io stats

(defn pidstat? []
  (= 0 (:exit (sh/sh "which" "pidstat"))))

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

;;;

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
