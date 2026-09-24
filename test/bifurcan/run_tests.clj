(ns bifurcan.run-tests
  (:require
   [bifurcan.test-utils :as utils]
   [eftest.runner :as e]))

(defn -main [& [iterations]]
  (when iterations
    (alter-var-root #'utils/iterations (constantly (read-string iterations))))
  (->> "test"
    e/find-tests
    ;; :known-bug tests document open issues, and are expected to fail
    (remove #(let [m (meta %)]
               (or (contains? m :benchmark)
                   (contains? m :known-bug))))
    (#(e/run-tests % #_{:report eftest.report.pretty/report, :capture-output? false}))
    (#(if (pos? (:fail %)) 1 0))
    System/exit))
