(ns marceline.topology.wordcount-metrics-test
  (:require [backtype.storm.testing :as t]
            [marceline.storm.metrics.consumers :as m])
  (:use clojure.test
        backtype.storm.config
        storm.trident.testing
        marceline.storm.testing
        marceline.topology.wordcount-metrics))

(def TEST-VALS [["the cow jumped over the moon"]
                ["four score and seven years ago"]
                ["how many can you eat"]
                ["to be or not to be the person"]])

(deftest wordcount-drpc
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (let [feeder (feeder-spout ["sentence"])
            topology (build-topology feeder drpc)
            conf-with-consumers (m/log-consumer (m/log-consumer-no-cleanup {}))]
        (is (= 2
               (count (get conf-with-consumers TOPOLOGY-METRICS-CONSUMER-REGISTER))))
        (with-topology-conf [cluster topology conf-with-consumers]
          (feed feeder TEST-VALS)
          (is (= 4
                 (ffirst
                  (exec-drpc drpc
                             "words"
                             "cat dog the man jumped"))))
          (is (= 2
                 (ffirst
                  (exec-drpc drpc
                             "words"
                             "seven person")))))))))
