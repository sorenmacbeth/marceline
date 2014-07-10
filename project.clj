(defproject yieldbot/marceline "0.2.1-SNAPSHOT"
  :description "A Clojure DSL for Storm/Trident."
  :url "https://github.com/yieldbot/marceline"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :profiles {:provided {:dependencies
                        [[org.apache.storm/storm-core "0.9.2-incubating"]
                         [org.clojure/clojure "1.5.1"]]}})
