(defproject yieldbot/marceline "0.2.0"
  :description "A Clojure DSL for Storm/Trident."
  :url "https://github.com/yieldbot/marceline"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :profiles {:provided {:dependencies [[storm "0.9.0.1"]]}}
  :dependencies [[org.clojure/clojure "1.4.0"]])
