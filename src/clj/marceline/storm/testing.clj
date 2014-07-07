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

(defn new-mock-tuple
  "Create a new mock trident tuple. Field values should come in the format of field, value, field value. e.g:
   (new-mocj-tuple :a \"a-value\" :b \"b-value\" :c \"c what I did there?\")"
  [& args]
  (let [fields (map name (take-nth 2 args))
        values (take-nth 2 (rest args))]
    (MockTridentTuple. fields values)))

(defn- new-nil-context
  "Creates a TridentOperationContext with nil topology and tuple factory for testing."
  []
  ;; have to do it this way, as `[nil nil]` doesn't tell it which constructor to call.
  (let [constructor (.getDeclaredConstructor TridentOperationContext (into-array Class [TopologyContext TridentTuple$Factory]))]
    (.newInstance constructor (into-array Object [nil nil]))))

(defn run-function
  "Runs a Trident function defined with deftridentfn. Returns what has been emitted to the collector (2 dimensional array))
  To pass in tuple values, they should come in the format of field, value, field value. e.g:
  (run-function my-func :a \"a-value\" :b \"b-value\" :c \"c what I did there?\")
  "
  [fn & args]
  (let [collector (CaptureCollector.)
        mock (apply new-mock-tuple args)
        context (new-nil-context)]

    (.prepare fn {} context)
    (.execute fn mock collector)
    (.-captured collector)))

(defn run-filter
  "Runs a Trident filter, defined with deffilter. Returns the boolean value from the filter.
  To pass in tuple values, they should come in the format of field, value, field value. e.g:
  (run-filter my-filter :a \"a-value\" :b \"b-value\" :c \"c what I did there?\")"
  [filter & args]
  (let [mock (apply new-mock-tuple args)
        context (new-nil-context)]

    (.prepare filter {} context)
    (.isKeep filter mock)))

(defn run-combiner-aggregator
  "Runs a combiner aggregator, as defined with defcombineraggregator. Returns a map with the following results, under the keys:
  :zero for the result of the [] arity function result.
  :init for the result [tuple] arity function. This function will be passed tuple1 as it's arguments.
  :combine for the [t1 t2] arity function. This function will be passed the *results* of tuple1 and tuple2 being passed to the [tuple] arity function.
  :combine-zero for the [t1 t2] arity function. This function will be passed the *results* of tuple1 being passed to the [tuple] arity function,
                and the [] arity function, to test zero combinations.

  As per usual tuple1 and tuple2 should be vectors that define your two tuples for testing. Tuple values, should come in the format of field, value, field value. e.g:
  (run-combiner-aggregator my-aggregator [:a \"a-value\" :b \"b-value\" :c \"c what I did there?\"] [:a \"a-value2\" :b \"b-value2\" :c \"c what I didn't do there?\"])
  "

  [aggregator tuple1 tuple2]
  (let [mtuple1 (apply new-mock-tuple tuple1)
        mtuple2 (apply new-mock-tuple tuple2)]

    {:init         (.init aggregator mtuple1)
     :zero         (.zero aggregator)
     :combine      (.combine aggregator (.init aggregator mtuple1) (.init aggregator mtuple2))
     :combine-zero (.combine aggregator (.init aggregator mtuple1) (.zero aggregator))}))

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
