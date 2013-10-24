(ns marceline.storm.testing.trident-wordcount
  (:import [backtype.storm.tuple Values Fields]
           storm.trident.TridentTopology
           [storm.trident.operation.builtin MapGet FilterNull Sum Count]
           storm.trident.state.StateType
           [storm.trident.testing MemoryMapState$Factory FixedBatchSpout]
           [backtype.storm LocalDRPC LocalCluster StormSubmitter]
           [java.util ArrayList List])
  (:require [marceline.storm.trident :as t]
            [clojure.string :as string :only [join split]])
  (:use [backtype.storm clojure config]))

(defn mk-fixed-spout [max-batch-size]
  (FixedBatchSpout.
   (Fields. "sentence")
   max-batch-size
;   (ArrayList. (into [] (doall (map t/values '("the cow jumped over the moon"
;                                               "the man went to the store and bought some candy"
;                                               "four score and seven years ago"
;                                               "how many can you eat"
;                                               "to be or not to be the person")))))
   (Values. "the cow jumped over the moon")
   ))

(t/defcombineraggregator
  count-words
  ([] 0)
  ([tuple] 1)
  ([t1 t2] (+ t1 t2)))

(t/deftridentfn split-args
  [tuple coll]
  (when-let [args (t/first tuple)]
    (let [words (string/split args #" ")]
      (doseq [word words]
        (t/emit-fn coll word)))))

(defn build-topology [drpc]
  (let [spout (mk-fixed-spout 3)
        _ (.setCycle spout true)
        word-state-factory (MemoryMapState$Factory.)
        trident-topology (TridentTopology.)
        word-counts (-> (t/new-stream* trident-topology "word-counts" spout)
                        (t/parallelism-hint* 16)
                        (t/each* ["sentence"]
                                 split-args
                                 ["word"])
                        (t/group-by* ["word"])
                        (t/persistent-aggregate* word-state-factory
                                                 ["word"]
                                                 (Count.)
                                                 ["count"])
                        (t/parallelism-hint* 16))
        count-query (-> (t/drpc-stream* trident-topology "words" drpc)
                        (t/each* ["args"]
                                 split-args
                                 ["word"])
                        (t/group-by* ["word"])
                        (t/state-query* word-counts
                                        ["word"]
                                        (MapGet.)
                                        ["count"])
                        (t/each* ["count"] (FilterNull.))
                        (t/aggregate* ["count"]
                                      (Sum.)
                                      ["sum"]))]
    trident-topology))

(defn run-local! []
  (let [cluster (LocalCluster.)
        local-drpc (LocalDRPC.)]
    (.submitTopology cluster "wordcounter"
                     {}
                     (.build
                      (build-topology local-drpc)))
    (Thread/sleep 1000)
    (.execute local-drpc "words" "cat the dog jumped")
    (.shutdown cluster)
    (System/exit 0)))

(defn submit-topology! [env]
  (let [name "wordcounter"
        conf {TOPOLOGY-WORKERS 6
              TOPOLOGY-MAX-SPOUT-PENDING 20
              TOPOLOGY-MESSAGE-TIMEOUT-SECS 60
              TOPOLOGY-STATS-SAMPLE-RATE 1.00}]
    (StormSubmitter/submitTopology
     name
     conf
     (.build (build-topology
              nil)))))

(defn -main
  ([]
     (run-local!))
  ([env & args]
     (submit-topology! (keyword env))))
