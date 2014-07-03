(ns marceline.topology.tfidf
  (:import storm.trident.TridentTopology
           [storm.trident.operation.builtin MapGet]
           [storm.trident.testing MemoryMapState$Factory])
  (:require [marceline.storm.trident :as t]
            [marceline.storm.ops :as o]
            [clojure.string :as string :only [split]])
  (:gen-class))

(defn split-first-tuple-on-ws
  [tuple]
  (when-let [args (t/first tuple)]
    (string/split args #" ")))

(t/deftridentfn expand-terms
  [tuple coll]
  (when-let [terms (split-first-tuple-on-ws tuple)]
    (doseq [term terms]
      (t/emit-fn coll term))))

(t/deftridentfn unpack-args
  [tuple coll]
  (when-let [args (split-first-tuple-on-ws tuple)]
    (apply t/emit-fn coll args)))

(defn tfidf-fn*
  [tf d df]
  (* tf (Math/log (/ d (+ 1.0 df)))))

(t/deftridentfn tfidf-fn
  [tuple coll]
  (let [[tf d df] (take 3 tuple)
        tfidf (tfidf-fn* tf d df)]
    (t/emit-fn coll tfidf)))

(defn build-topology [spout drpc]
  (let [tf-state-factory (MemoryMapState$Factory.)
        df-state-factory (MemoryMapState$Factory.)
        d-state-factory (MemoryMapState$Factory.)
        trident-topology (TridentTopology.)

        ;; source streams
        document-stream
        (-> (t/new-stream trident-topology "document-spout" spout)
            (t/project ["source" "doc-id" "terms"]))

        source-stream
        (-> document-stream (t/project ["source"]))

        doc-id-term-stream
        (-> document-stream
            (t/each ["terms"] expand-terms ["term"])
            (t/project ["doc-id" "term"]))

        distinct-term-per-doc-id-stream
        (-> doc-id-term-stream
            (t/group-by ["doc-id" "term"])
            (t/aggregate o/one ["one"])
            (t/project ["term"]))
        
        ;; stateful streams
        tf-stream
        (-> doc-id-term-stream
            (t/group-by ["doc-id" "term"])
            (t/persistent-aggregate tf-state-factory o/count ["tf"]))

        d-stream
        (-> source-stream
            (t/group-by ["source"])
            (t/persistent-aggregate d-state-factory o/count ["d"]))

        df-stream
        (-> distinct-term-per-doc-id-stream
            (t/group-by ["term"])
            (t/persistent-aggregate df-state-factory o/count ["df"]))

        ;; dprc queries
        tfidf-query
        (-> (t/drpc-stream trident-topology "tfidf" drpc)
            ;; destructure drpc query
            (t/each ["args"] unpack-args ["source" "doc-id" "term"])

            ;; state queries
            (t/state-query tf-stream ["doc-id" "term"] (MapGet.) ["tf"])
            (t/state-query d-stream ["source"] (MapGet.) ["d"])
            (t/state-query df-stream ["term"] (MapGet.) ["df"])
        
            ;; calculate tfidf
            (t/each ["tf" "d" "df"] tfidf-fn ["tfidf"])
            (t/project ["doc-id" "term" "tfidf"]))

        tf-query
        (-> (t/drpc-stream trident-topology "tf" drpc)
            (t/each ["args"] unpack-args ["doc-id" "term"])
            (t/state-query tf-stream ["doc-id" "term"] (MapGet.) ["tf"])
            (t/project ["tf"]))

        d-query
        (-> (t/drpc-stream trident-topology "d" drpc)
            (t/each ["args"] unpack-args ["source"])
            (t/state-query d-stream ["source"] (MapGet.) ["d"])
            (t/project ["d"]))

        df-query
        (-> (t/drpc-stream trident-topology "df" drpc)
            (t/each ["args"] unpack-args ["term"])
            (t/state-query df-stream ["term"] (MapGet.) ["df"])
            (t/project ["df"]))]
   trident-topology))
