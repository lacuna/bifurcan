(defproject io.lacuna/bifurcan "0.1.0-alpha1"
  :java-source-paths ["src"]
  :dependencies []
  :test-selectors {:default #(not
                               (some #{:benchmark :stress}
                                 (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress :stress
                   :all (constantly true)}
  :profiles {:travis {:jvm-opts ^:replace ["-server" "-Xmx1g"]}
             :bench {:jvm-opts ^:replace ["-server" "-Xmx10g" "-XX:+UseParallelGC"]}
             :dev {:dependencies [[org.clojure/clojure "1.8.0"]
                                  [org.clojure/test.check "0.9.0"]
                                  [criterium "0.4.3"]
                                  [potemkin "0.4.3"]
                                  [proteus "0.1.6"]
                                  [byte-streams "0.2.2"]
                                  [eftest "0.1.4"]]}}
  :aliases {"partest" ["run" "-m" "bifurcan.run-tests"]
            "benchmark" ["with-profile" "bench,dev" "run" "-m" "bifurcan.benchmark-test" "benchmark"]}
  :jvm-opts ^:replace ["-server" "-XX:+UseG1GC" "-Xmx10g" "-XX:-OmitStackTraceInFastThrow"])
