(ns bifurcan.graph-test
  (:import
   [io.lacuna.bifurcan
    DirectedGraph
    IGraph
    Graphs]))

(defn construct [vertex->out]
  (reduce
    (fn [g [v out]]
      (reduce #(.link ^IGraph %1 v %2) g out))
    (DirectedGraph.)
    vertex->out))
