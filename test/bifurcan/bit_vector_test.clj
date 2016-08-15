(ns bifurcan.bit-vector-test
  (:require
   [clojure.test :refer :all]
   [clojure.test.check.generators :as gen]
   [bifurcan.test-utils :as u])
  (:import
   [java.util
    ArrayList]
   [io.lacuna.bifurcan.utils
    BitVector]))

;;;

(defn bit-vector-insert [[len v] bits idx val]
  [(+ len bits) (BitVector/insert v len val (* idx bits) bits)])

(defn bit-vector-remove [[len v] bits idx]
  (if (zero? len)
    [len v]
    [(- len bits) (BitVector/remove v len (* idx bits) bits)]))

(defn bit-vector-append [[len v] bits val]
  [(+ len bits) (BitVector/insert v len val len bits)])

(defn bit-vector-seq [bits count v]
  (map
    #(BitVector/get v (* % bits) bits)
    (range count)))

(defn bit-vector-count [bits]
  (fn [[len v]]
    (int (/ len bits))))

;;;

(defn list-insert [^ArrayList l idx val]
  (doto l (.add idx val)))

(defn list-remove [^ArrayList l idx]
  (when-not (empty? l)
    (.remove l (int idx)))
  l)

(defn list-append [^ArrayList l val]
  (doto l (.add val)))

;;;

(defn wrap-idx [count f]
  (fn [s idx & args]
    (apply f s (* (count s) (int (/ idx 1e3))) args)))

(def gen-idx (gen/choose 0 999))

(defn vector-actions [bits max-val]
  (let [gen-element (gen/large-integer* {:min 0, :max max-val})]
    {:remove (u/action [gen-idx]
               (wrap-idx count list-remove)
               (wrap-idx (bit-vector-count bits) #(bit-vector-remove %1 bits %2)))
     :insert (u/action [gen-idx gen-element]
               (wrap-idx count list-insert)
               (wrap-idx (bit-vector-count bits) #(bit-vector-insert %1 bits %2 %3)))
     :append (u/action [gen-element]
               list-append
               #(bit-vector-append %1 bits %2))}))

(defn construct-colls [bits actions]
  (let [[l [len v]] (u/apply-actions
                      (vector-actions bits 0)
                      actions
                      (ArrayList.)
                      [0 (BitVector/create 0)])]
    [l (bit-vector-seq bits (/ len bits) v)]))

(u/def-collection-check test-bit-vector-16 1e5 (vector-actions 16 16384)
  [a (ArrayList.)
   [len v] [0 (BitVector/create 0)]]
  (= a (bit-vector-seq 16 (/ len 16) v)))

(u/def-collection-check test-bit-vector-48 1e5 (vector-actions 48 Integer/MAX_VALUE)
  [a (ArrayList.)
   [len v] [0 (BitVector/create 0)]]
  (= a (bit-vector-seq 48 (/ len 48) v)))
