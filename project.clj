(defproject bifurcan "0.1.0-SNAPSHOT"
  :java-source-paths ["src"]
  :dependencies []
  :plugins [#_[lein-virgil "0.1.0"]]
  :test-selectors {:default #(not
                               (some #{:benchmark :stress}
                                 (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress :stress
                   :all (constantly true)}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]
                                  [org.clojure/test.check "0.9.0"]
                                  [criterium "0.4.3"]]}}
  :jvm-opts ^:replace ["-server" "-XX:+UseG1GC" "-Xmx1g" "-XX:-OmitStackTraceInFastThrow"])
