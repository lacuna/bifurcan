;; this is to allow the insecure `usethesource` repository
(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory!
  "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))

(defproject io.lacuna/bifurcan "0.1.0-SNAPSHOT"
  :java-source-paths ["src"]
  :dependencies []
  :test-selectors {:default   #(not
                                 (some #{:benchmark :stress}
                                   (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress    :stress
                   :all       (constantly true)}
  :profiles {:travis {:jvm-opts ^:replace ["-server" "-Xmx1g"]}
             :bench  {:jvm-opts ^:replace ["-server" "-Xmx20g" "-XX:+UseParallelGC"]}
             :dev    {:dependencies [[org.clojure/clojure "1.8.0"]
                                     [org.clojure/test.check "0.9.0"]
                                     [criterium "0.4.4"]
                                     [potemkin "0.4.4"]
                                     [proteus "0.1.6"]
                                     [byte-streams "0.2.3"]
                                     [eftest "0.4.1"]
                                     [virgil "0.1.7"]
                                     [io.usethesource/capsule "0.6.1"]]}}
  :aliases {"partest"   ["run" "-m" "bifurcan.run-tests"]
            "benchmark" ["with-profile" "bench,dev" "run" "-m" "bifurcan.benchmark-test" "benchmark"]}
  :jvm-opts ^:replace ["-server" "-XX:+UseG1GC" "-XX:-OmitStackTraceInFastThrow"]

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
