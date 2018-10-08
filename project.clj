;; this is to allow the insecure `usethesource` repository
(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory!
  "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))

(defproject io.lacuna/bifurcan "0.1.0-alpha6"
  :java-source-paths ["src"]
  :dependencies []
  :test-selectors {:default   #(not
                                 (some #{:benchmark :stress}
                                   (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress    :stress
                   :all       (constantly true)}
  :profiles {:travis {:jvm-opts ^:replace ["-server" "-Xmx1g"]}
             :bench  {:jvm-opts ^:replace ["-server" "-Xmx10g" #_"-XX:+UseParallelGC"]}
             :dev    {:dependencies [;; for tests
                                     [org.clojure/clojure "1.8.0"]
                                     [org.clojure/test.check "0.9.0"]
                                     [criterium "0.4.4"]
                                     [potemkin "0.4.5"]
                                     [proteus "0.1.6"]
                                     [byte-streams "0.2.3"]
                                     [eftest "0.5.2"]
                                     [virgil "0.1.8"]

                                     ;; for comparative benchmarks
                                     [io.usethesource/capsule "0.6.2"]
                                     [org.pcollections/pcollections "3.0.3"]
                                     [io.javaslang/javaslang "2.1.0-alpha"]
                                     [org.scala-lang/scala-library "2.12.7"]
                                     [org.functionaljava/functionaljava "4.8"]
                                     [org.eclipse.collections/eclipse-collections "9.2.0"]
                                     [org.organicdesign/Paguro "3.1.0"]]}}
  :aliases {"partest"   ["run" "-m" "bifurcan.run-tests"]
            "benchmark" ["run" "-m" "bifurcan.benchmark-test" "benchmark"]}
  :jvm-opts ^:replace ["-server"
                       "-XX:+UseG1GC"
                       "-XX:-OmitStackTraceInFastThrow"
                       "-ea:io.lacuna..."]

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
