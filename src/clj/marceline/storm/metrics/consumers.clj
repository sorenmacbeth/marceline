(ns marceline.storm.metrics.consumers
  (:import storm.trident.TridentTopology
	   [storm.trident.operation.builtin MapGet]
	   [storm.trident.testing MemoryMapState$Factory FixedBatchSpout]
	   [backtype.storm LocalDRPC LocalCluster StormSubmitter])
  (:require [marceline.storm.trident :as t]
	    [marceline.storm.metrics :as m]
	    [clojure.string :as string :only [split]])
  (:use [backtype.storm config])
  (:gen-class))

(m/defmetricsconsumer log-consumer
  ([this conf registation-arg context error-reporter]
     (println "log consumer initializing"))
  ([this data-points]
     (println "log consumer handling data points: " data-points))
  ([this]
     (println "log consumer cleaning up")))

(m/defmetricsconsumer log-consumer-no-cleanup
  ([this conf registation-arg context error-reporter]
     (println "log consumer initializing"))
  ([this data-points]
     (println "log consumer handling data points: " data-points)))
