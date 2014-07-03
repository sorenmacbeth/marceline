(ns marceline.topology.reach
  (:import storm.trident.TridentTopology
           [storm.trident.operation.builtin MapGet Sum]
           [storm.trident.testing MemoryMapStateInitable$Factory])
  (:require [marceline.storm.trident :as t])
  (:gen-class))


(t/defcombineraggregator one
  ([] 1)
  ([tuple] 1)
  ([v1 v2] 1))

(t/deftridentfn expand-list
  [tuple coll]
  (when-let [l (first tuple)]
    (doseq [obj l]
      (t/emit-fn coll obj))))


(defn build-topology [urls-state-factory followers-state-factory drpc]
  (let [trident-topology (TridentTopology.)
        urls->tweeters (t/new-static-state trident-topology urls-state-factory)
        tweeters->followers (t/new-static-state trident-topology followers-state-factory)]
    (-> (t/drpc-stream trident-topology "reach" drpc)
        (t/state-query urls->tweeters ["args"] (MapGet.) ["tweeters"])
        (t/each ["tweeters"] expand-list ["tweeter"])
        (t/shuffle)
        (t/state-query tweeters->followers ["tweeter"] (MapGet.) ["followers"])
        (t/each ["followers"] expand-list ["follower"])
        (t/group-by ["follower"])
        (t/aggregate one ["one"])
        (t/aggregate ["one"] (Sum.) ["reach"]))
    trident-topology))
