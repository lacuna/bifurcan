(ns bifurcan.utils-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u])
  (:import
   [java.util
    ArrayList]
   [io.lacuna.bifurcan.utils
    Iterators]))

(deftest iterators-equals-test
  (are [a b]
       (Iterators/equals (.iterator a) (.iterator b) (u/->bi-predicate =))
       [] []
       [1] [1]
       [2 2] [2 2]
       [1 3 2] [1 3 2]
       [nil 2] [nil 2])
  (are [a b]
       (not (Iterators/equals (.iterator a) (.iterator b) (u/->bi-predicate =)))
       [1 3 2] [1 2 3]
       [1 2] []
       [] [3]
       [nil] [4]
       [4] [nil]))
