(ns marceline.storm.testing.trident-wordcount-test
  (:import [storm.trident.testing
            MemoryMapState$Factory
            FixedBatchSpout])
  (:require [backtype.storm.testing :as t]
            [marceline.storm.trident :as marceline])
  (:use clojure.test
        clojure.pprint
        [backtype.storm clojure config]
        storm.trident.testing
        marceline.storm.testing.trident-wordcount))

(def TEST-VALS [["the cow jumped over the moon"]
                ["four score and seven years ago"]
                ["how many can you eat"]
                ["to be or not to be the person"]])

(deftest wordcount-drpc
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (let [feeder (feeder-spout ["sentence"])
            topology (build-topology feeder drpc)]
        (with-topology [cluster topology]
          (feed feeder TEST-VALS)
          (is (= 4
                 (ffirst
                  (exec-drpc drpc
                             "words"
                             "cat dog the man jumped")))))))))
