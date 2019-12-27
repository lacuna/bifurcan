(ns bifurcan.run-tests
  (:require
   [bifurcan.test-utils :as utils]
   [eftest.runner :as e]))

(defn -main [& [iterations]]
  (when iterations
    (alter-var-root #'utils/iterations (constantly (read-string iterations))))
  (->> "test"
    e/find-tests
    (remove #(-> % meta (contains? :benchmark)))
    (#(e/run-tests % #_{:report eftest.report.pretty/report, :capture-output? false}))
    (#(if (pos? (:fail %)) 1 0))
    System/exit))
