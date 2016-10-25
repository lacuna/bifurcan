(ns bifurcan.linear-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u])
  (:import
   [io.lacuna.bifurcan
    LinearList
    LinearMap]))

;;;

(defn list-append [^LinearList l x]
  (.append l x))

(defn map-)

;;;

(defn set-actions [max-val]
  {:add (u/action [gen/pos-int] conj list-append)})

(u/def-collection-check test-linear-list 1e4 (set-actions Long/MAX_VALUE)
  [v []
   l (LinearList.)]
  (= (seq v) (-> l .iterator iterator-seq)))
