;; this is to allow the insecure `usethesource` repository
(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory!
  "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))

(defproject io.lacuna/bifurcan "0.2.0-alpha5"
  :java-source-paths ["src"]
  :dependencies []
  :test-selectors {:default   #(not
                                 (some #{:benchmark :stress}
                                   (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress    :stress
                   :all       (constantly true)}
  :profiles {:low-mem {:jvm-opts ^:replace ["-server" "-Xmx1g" "-XX:MaxDirectMemorySize=2g" "-XX:+UseG1GC"]}
             :bench   {:jvm-opts ^:replace ["-server" "-Xmx10g" "-XX:+UseG1GC"]}
             :dev     {:dependencies [;; for tests
                                      [org.clojure/clojure "1.10.0"]
                                      [org.clojure/test.check "0.10.0"]
                                      [criterium "0.4.5"]
                                      [potemkin "0.4.5"]
                                      [proteus "0.1.6"]
                                      [byte-streams "0.2.4"]
                                      [byte-transforms "0.1.4"]
                                      [eftest "0.5.9"]
                                      [virgil "0.1.9"]

                                      ;; for comparative in-memory benchmarks
                                      [io.usethesource/capsule "0.6.3"]
                                      [org.pcollections/pcollections "3.1.2"]
                                      [io.vavr/vavr "0.10.2"]
                                      [org.scala-lang/scala-library "2.13.1"]
                                      [org.functionaljava/functionaljava "4.8.1"]
                                      [org.organicdesign/Paguro "3.1.2"]

                                      ;; for comparative durable benchmarks
                                      [org.rocksdb/rocksdbjni "6.4.6"]
                                      [com.sleepycat/je "18.3.12"]
                                      [org.lmdbjava/lmdbjava "0.7.0"]
                                      ]}}
  :aliases {"partest"              ["run" "-m" "bifurcan.run-tests"]
            "benchmark"            ["run" "-m" "bifurcan.benchmark-test" "benchmark"]
            "benchmark-collection" ["run" "-m" "bifurcan.benchmark-test" "benchmark-collection"]
            "benchmark-databases"  ["with-profile" "low-mem,dev" "run" "-m" "bifurcan.durable-benchmark-test" "benchmark"]}
  :jvm-opts ^:replace ["-server"
                       "-XX:+UseG1GC"
                       "-XX:-OmitStackTraceInFastThrow"
                       "-ea:io.lacuna..."
                       "-Xmx4g"

                       #_"-XX:+UnlockDiagnosticVMOptions"
                       #_"-XX:+PrintAssembly"
                       #_"-XX:CompileCommand=print,io.lacuna.bifurcan.nodes.Util::mergeState"
                       #_"-XX:CompileCommand=dontinline,io.lacuna.bifurcan.nodes.Util::mergeState"
                       ]

  :repositories {"usethesource" "http://nexus.usethesource.io/content/repositories/public/"}

  ;; deployment
  :url "https://github.com/lacuna/bifurcan"
  :description "impure functional data structures"
  :license {:name "MIT License"}
  :javac-options ["-target" "1.8" "-source" "1.8"]
  :deploy-repositories {"releases"  {:url   "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                                     :creds :gpg}
                        "snapshots" {:url   "https://oss.sonatype.org/content/repositories/snapshots/"
                                     :creds :gpg}}

  ;; Maven properties for the Maven God
  :scm {:url "git@github.com:lacuna/bifurcan.git"}
  :pom-addition [:developers [:developer
                              [:name "Zach Tellman"]
                              [:url "http://ideolalia.com"]
                              [:email "ztellman@gmail.com"]
                              [:timezone "-8"]]]
  :classifiers {:javadoc {:java-source-paths ^:replace []
                          :source-paths      ^:replace []
                          :resource-paths    ^:replace ["javadoc"]}
                :sources {:source-paths   ^:replace ["src"]
                          :resource-paths ^:replace []}})
