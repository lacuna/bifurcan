(ns bifurcan.durable-benchmark-test
  (:import
   [org.rocksdb
    RocksDB
    Options]))

(RocksDB/loadLibrary)
