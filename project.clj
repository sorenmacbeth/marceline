(defproject yieldbot/marceline "0.2.3-SNAPSHOT"
  :description "A Clojure DSL for Storm/Trident."
  :url "https://github.com/yieldbot/marceline"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.7" "-target" "1.7"]
  :profiles {:provided {:dependencies
                        [[org.apache.storm/storm-core "0.10.0-beta1"]
                         [org.clojure/clojure "1.6.0"]]}})
