(ns marceline.storm.metrics.consumers
  (:import org.apache.storm.trident.TridentTopology
           [org.apache.storm.trident.operation.builtin MapGet]
           [org.apache.storm.trident.testing MemoryMapState$Factory FixedBatchSpout]
           [org.apache.storm LocalDRPC LocalCluster StormSubmitter])
  (:require [marceline.storm.trident :as t]
            [marceline.storm.metrics :as m]
            [clojure.string :as string :only [split]])
  (:use [org.apache.storm config])
  (:gen-class))

(m/defmetricsconsumer log-consumer
  ([this conf registation-arg context error-reporter]
     (println "log consumer initializing"))
  ([this task-info data-points]
     (println "log consumer handling data points: " data-points))
  ([this]
     (println "log consumer cleaning up")))

(m/defmetricsconsumer log-consumer-no-cleanup
  {:state 42}
  ([this conf registation-arg context error-reporter]
     (println "log consumer initializing" (.state this)))
  ([this task-info data-points]
     (println "log consumer handling data points: " data-points)))
