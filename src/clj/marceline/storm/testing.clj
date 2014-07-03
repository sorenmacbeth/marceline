(ns marceline.storm.testing
  (:require [backtype.storm [testing :as t]]
            [marceline.storm.trident :as trident])
  (:import [backtype.storm.generated KillOptions]
           [marceline.storm.trident ClojureTridentTuple]
           [storm.trident.testing
            MemoryMapState$Factory
            LRUMemoryMapState$Factory MockTridentTuple]))


(defn with-topology-conf* [cluster topo conf body-fn]
  (t/submit-local-topology (:nimbus cluster) "tester" conf (.build topo))
  (body-fn)
  (.killTopologyWithOpts (:nimbus cluster) "tester" (doto (KillOptions.) (.set_wait_secs 0))))

(defmacro with-topology-conf [[cluster topo conf] & body]
  `(with-topology-conf* ~cluster ~topo ~conf (fn [] ~@body)))

(defn memory-map-state-factory
  []
  (MemoryMapState$Factory.))

(defn lru-memory-map-state-factory
  [n]
  (LRUMemoryMapState$Factory. n))

(extend-type MockTridentTuple
  trident/ClojureTridentTuple
  (get [this field]
    (.getValueByField ^MockTridentTuple this (name field))))
