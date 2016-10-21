(ns bifurcan.linear-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u])
  (:import
   [io.lacuna.bifurcan
    LinearList
    LinearMap]))
