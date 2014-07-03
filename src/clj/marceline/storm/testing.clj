(ns marceline.storm.testing
  (:require [backtype.storm [testing :as t]]
            [marceline.storm.trident :as trident]
            [clojure.reflect :as reflect])
  (:import [backtype.storm.generated KillOptions]
           [marceline.storm.trident ClojureTridentTuple]
           [storm.trident.testing
            MemoryMapState$Factory
            LRUMemoryMapState$Factory MockTridentTuple]
           [storm.trident.operation.impl CaptureCollector]
           [storm.trident.operation TridentOperationContext]
           [storm.trident.tuple TridentTupleView$RootFactory TridentTuple$Factory]
           [backtype.storm.tuple Fields]
           [backtype.storm.task TopologyContext]))


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

(defn run-function
  "Runs a Trident function defined with deftridentfn. Returns what has been emitted to the collector (2 dimensional array))
  To pass in tuple values, they should come in the format of field, value, field value. e.g:
  (run-function my-func :a \"a-value\" :b \"b-value\" :c \"c what I did there?\")
  "
  [fn & args]
  (let [collector (CaptureCollector.)
        fields (map name (take-nth 2 args))
        values (take-nth 2 (rest args))
        mock (MockTridentTuple. fields values)
        ;; have to do it this way, as `[nil nil]` doesn't tell it which constructor to call.
        constructor (.getDeclaredConstructor TridentOperationContext (into-array Class [TopologyContext TridentTuple$Factory]))
        context (.newInstance constructor (into-array Object [nil nil]))]

    (.prepare fn {} context)
    (.execute fn mock collector)
    (.-captured collector)))

(extend-type MockTridentTuple
  trident/ClojureTridentTuple
  (first
    [this]
    (.getValue ^MockTridentTuple this 0))
  (count
    [this]
    (.size ^MockTridentTuple this))
  (get
    ([this field]
     (.getValueByField ^MockTridentTuple this (name field)))
    ([this field not-found]
     (or (.getValueByField ^MockTridentTuple this (name field))
         not-found)))
  (nth
    ([this index]
     (.getValue ^MockTridentTuple this index))
    ([this index not-found]
     (or (.getValue ^MockTridentTuple this index)
         not-found)))
  (vals [this]
    (.getValues ^MockTridentTuple this)))
