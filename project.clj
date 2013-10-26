(defproject yieldbot/marceline "0.1.0-SNAPSHOT"
  :description "A Clojure DSL for Storm/Trident."
  :url "https://github.com/yieldbot/marceline"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :profiles {:provided {:dependencies [[storm "0.8.2"]]}}
  :dependencies [[org.clojure/clojure "1.4.0"]]
  :plugins [[s3-wagon-private "1.1.2"]]
  :repositories {"releases" {:url "s3p://maven.yieldbot.com/releases/"
                             :username :env :passphrase :env}
                 "snapshots" {:url "s3p://maven.yieldbot.com/snapshots/"
                              :username :env :passphrase :env}})

(cemerick.pomegranate.aether/register-wagon-factory!
 "s3p" #(eval '(org.springframework.aws.maven.PrivateS3Wagon.)))
