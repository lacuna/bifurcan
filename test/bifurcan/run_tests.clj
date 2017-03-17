(ns bifurcan.run-tests
  (:require
   [bifurcan.collection-test :as ct]
   [eftest.runner :as e]))

(defn -main [& [iterations]]
  (when iterations
    (alter-var-root #'ct/iterations (constantly (read-string iterations))))
  (->> "test"
    e/find-tests
    (remove #(-> % meta (contains? :benchmark)))
    (e/run-tests #_{:report eftest.report.pretty/report})
    (#(if (= (:test %) (:pass %)) 0 1))
    System/exit))
