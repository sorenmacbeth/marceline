(ns marceline.topology.wordcount
  (:import storm.trident.TridentTopology
           [storm.trident.operation.builtin MapGet]
           [storm.trident.testing MemoryMapState$Factory FixedBatchSpout]
           [backtype.storm LocalDRPC LocalCluster StormSubmitter])
  (:require [marceline.storm.trident :as t]
            [clojure.string :as string :only [split]])
  (:use [backtype.storm config])
  (:gen-class))

(defn mk-fixed-batch-spout [max-batch-size]
  (FixedBatchSpout.
   (t/fields "sentence")
   max-batch-size
   (into-array (map t/values '("the cow jumped over the moon"
                               "the man went to the store and bought some candy"
                               "four score and seven years ago"
                               "how many can you eat"
                               "to be or not to be the person")))))

(t/defcombineraggregator
  count-words
  ([] 0)
  ([tuple] 1)
  ([t1 t2] (+ t1 t2)))

(t/defcombineraggregator
  sum
  ([] 0)
  ([tuple] (t/first tuple))
  ([t1 t2] (+ t1 t2)))

(t/deftridentfn split-args
  [tuple coll]
  (when-let [args (t/first tuple)]
    (let [words (string/split args #" ")]
      (doseq [word words]
        (t/emit-fn coll word)))))

(t/deffilter
  filter-null
  [tuple]
  (not (nil? (t/first tuple))))

(defn build-topology [spout drpc]
  (let [word-state-factory (MemoryMapState$Factory.)
        trident-topology (TridentTopology.)
        word-counts (-> (t/new-stream trident-topology "word-counts" spout)
                        (t/parallelism-hint 16)
                        (t/each ["sentence"]
                                split-args
                                ["word"])
                        (t/project ["word"])
                        (t/group-by ["word"])
                        (t/persistent-aggregate word-state-factory
                                                ["word"]
                                                count-words
                                                ["count"])
                        (t/parallelism-hint 16))
        count-query (-> (t/drpc-stream trident-topology "words" drpc)
                        (t/each ["args"]
                                split-args
                                ["word"])
                        (t/project ["word"])
                        (t/group-by ["word"])
                        (t/state-query word-counts
                                       ["word"]
                                       (MapGet.)
                                       ["count"])
                        (t/each ["count"] filter-null)
                        (t/aggregate ["count"]
                                     sum
                                     ["sum"])
                        (t/debug))]
    trident-topology))

(defn run-local! []
  (let [cluster (LocalCluster.)
        local-drpc (LocalDRPC.)
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (.submitTopology cluster "wordcounter"
                     {}
                     (.build
                      (build-topology
                       spout
                       local-drpc)))
    (Thread/sleep 10000)
    (.execute local-drpc "words" "cat dog the man jumped")
    (.shutdown cluster)
    (System/exit 0)))

(defn submit-topology! [env]
  (let [name "wordcounter"
        conf {TOPOLOGY-WORKERS 6
              TOPOLOGY-MAX-SPOUT-PENDING 20
              TOPOLOGY-MESSAGE-TIMEOUT-SECS 60
              TOPOLOGY-STATS-SAMPLE-RATE 1.00}
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (StormSubmitter/submitTopology
     name
     conf
     (.build (build-topology
              spout
              nil)))))

(defn -main
  ([]
     (run-local!))
  ([env & args]
     (submit-topology! (keyword env))))
