(ns marceline.topology.reach-test
  (:import [storm.trident.testing
            MemoryMapStateInitable$Factory
            FixedBatchSpout])
  (:require [backtype.storm.testing :as t]
            [marceline.storm.trident :as marceline])
  (:use clojure.test
        clojure.pprint
        [backtype.storm clojure config]
        storm.trident.testing
        marceline.topology.reach))

(def TWEETERS-DB
  {"foo.com/blog/1" ["sally" "bob" "tim" "george" "nathan"]
   "engineering.twitter.com/blog/5" ["adam" "david" "sally" "nathan"]
   "tech.backtype.com/blog/123" ["tim" "mike" "john"]})

(def FOLLOWERS-DB
  {"sally" ["bob" "tim" "alice" "adam" "jim" "chris" "jai"]
   "bob" ["sally" "nathan" "jim" "mary" "david" "vivian"]
   "tim" ["alex"]
   "nathan" ["sally" "bob" "adam" "harry" "chris" "vivian" "emily" "jordan"]
   "adam" ["david" "carissa"]
   "mike" ["john" "bob"]
   "john" ["alice" "nathan" "jim" "mike" "bob"]})


(deftest reach-drpc
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (let [topology (build-topology (MemoryMapStateInitable$Factory. TWEETERS-DB)
                                     (MemoryMapStateInitable$Factory. FOLLOWERS-DB)
                                     drpc)]
        (with-topology [cluster topology]
          (is (= 0
                 (ffirst (exec-drpc drpc
                                    "reach"
                                    "aaaa"))))
          (is (= 16
                 (ffirst (exec-drpc drpc
                                    "reach"
                                    "foo.com/blog/1"))))
          (is (= 14
                 (ffirst (exec-drpc drpc
                                    "reach"
                                    "engineering.twitter.com/blog/5")))))))))
