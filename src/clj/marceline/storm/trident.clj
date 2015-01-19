(ns marceline.storm.trident
  (:import [storm.trident TridentState Stream]
           [storm.trident.operation.builtin Debug]
           [storm.trident.tuple TridentTupleView]
           [storm.trident.fluent GroupedStream]
           [backtype.storm.tuple Values Fields]
           [storm.trident.spout RichSpoutBatchExecutor]
           [backtype.storm.utils RotatingMap TimeCacheMap]
           [marceline.storm.trident.clojure
            ClojureFunction
            ClojureFilter
            ClojureAggregator
            ClojureCombinerAggregator
            ClojureReducerAggregator
            ClojureQueryFunction
            ClojureStateUpdater
            ClojureStateFactory
            ClojureBackingMap])
  (:require [backtype.storm.clojure :refer (to-spec normalize-fns)])
  (:refer-clojure :exclude [group-by get nth vals
                            first count partition-by shuffle filter merge])
  (:gen-class))


;; This is to make configuration easier
(def MAX_BATCH_SIZE_CONF RichSpoutBatchExecutor/MAX_BATCH_SIZE_CONF)

;; ## tridentfn
(defn clojure-tridentfn* [fn-var args]
  (ClojureFunction. (to-spec fn-var) args))

(defmacro clojure-tridentfn [fn-sym args]
  `(clojure-tridentfn* (var ~fn-sym) ~args))

(defmacro tridentfn [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.operation.Function
       ~@fns)))

(defmacro deftridentfn [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(deftridentfn ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          args (vec args)
                          prepargs [(gensym "conf") (gensym "context")]]
                      `(fn ~prepargs (tridentfn (~'execute ~args ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-tridentfn ~fn-name args#))
                    `(def ~name
                       (clojure-tridentfn ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; ## filter
(defn clojure-filter* [fn-var args]
  (ClojureFilter. (to-spec fn-var) args))

(defmacro clojure-filter [fn-sym args]
  `(clojure-filter* (var ~fn-sym) ~args))

(defmacro filter [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.operation.Filter
       ~@fns)))

(defmacro deffilter [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(deffilter ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          args (vec args)
                          prepargs [(gensym "conf") (gensym "context")]]
                      `(fn ~prepargs (filter (~'isKeep ~args ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-filter ~fn-name args#))
                    `(def ~name
                       (clojure-filter ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; ## aggregator
(defn clojure-aggregator* [fn-var args]
  (ClojureAggregator. (to-spec fn-var) args))

(defmacro clojure-aggregator [fn-sym args]
  `(clojure-aggregator* (var ~fn-sym) ~args))

(defmacro aggregator [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.operation.Aggregator
       ~@fns)))

(defmacro defaggregator [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defaggregator ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[[init-args & init-body] [agg-args & agg-body] [complete-args & complete-body]] impl
                          prepargs [(gensym "conf") (gensym "context")]]
                      `(fn ~prepargs (aggregator
                                     (~'init ~(vec init-args) ~@init-body)
                                     (~'aggregate ~(vec agg-args) ~@agg-body)
                                     (~'complete ~(vec complete-args) ~@complete-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-aggregator ~fn-name args#))
                    `(def ~name
                       (clojure-aggregator ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; ## combineraggregator
(defn clojure-combiner-aggregator* [fn-var args]
  (ClojureCombinerAggregator. (to-spec fn-var) args))

(defmacro clojure-combiner-aggregator [fn-sym args]
  `(clojure-combiner-aggregator* (var ~fn-sym) ~args))

(defmacro combiner-aggregator [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.operation.CombinerAggregator
       ~@fns)))

(defmacro defcombineraggregator [name & [opts & [zero-impl init-impl combine-impl] :as all]]
  (if-not (map? opts)
    `(defcombineraggregator ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          [zero-args & zero-body] zero-impl
          [init-args & init-body] init-impl
          [combine-args & combine-body] combine-impl
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-combiner-aggregator ~fn-name args#))
                    `(def ~name
                       (clojure-combiner-aggregator ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           (combiner-aggregator
            (~'zero ~(vec zero-args) ~@zero-body)
            (~'init ~(vec init-args) ~@init-body)
            (~'combine ~(vec combine-args) ~@combine-body)))
         ~definer))))

;; ## reduceraggregator
(defn clojure-reducer-aggregator* [fn-var args]
  (ClojureReducerAggregator. (to-spec fn-var) args))

(defmacro clojure-reducer-aggregator [fn-sym args]
  `(clojure-reducer-aggregator* (var ~fn-sym) ~args))

(defmacro reducer-aggregator [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.operation.ReducerAggregator
       ~@fns)))

(defmacro defreduceraggregator [name & [opts impl :as all]]
  (if-not (map? opts)
    `(defreduceraggregator ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-reducer-aggregator ~fn-name args#))
                    `(def ~name
                       (clojure-reducer-aggregator ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           ~impl)
         ~definer))))

;; ## queryfn
(defn clojure-queryfn* [fn-var args]
  (ClojureQueryFunction. (to-spec fn-var) args))

(defmacro clojure-queryfn [fn-sym args]
  `(clojure-queryfn* (var ~fn-sym) ~args))

(defmacro queryfn [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.state.QueryFunction
       ~@fns)))

(defmacro defqueryfn [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defqueryfn ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[[retrieve-args & retrieve-body] [execute-args & execute-body]] impl
                          prepargs [(gensym "conf") (gensym "context")]]
                      `(fn ~prepargs (queryfn
                                     (~'batchRetrieve ~(vec retrieve-args) ~@retrieve-body)
                                     (~'execute ~(vec execute-args) ~@execute-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-queryfn ~fn-name args#))
                    `(def ~name
                       (clojure-queryfn ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; ## stateupdater
(defn clojure-state-updater* [fn-var args]
  (ClojureStateUpdater. (to-spec fn-var) args))

(defmacro clojure-state-updater [fn-sym args]
  `(clojure-state-updater* (var ~fn-sym) ~args))

(defmacro state-updater [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.state.StateUpdater
       ~@fns)))

(defmacro defstateupdater [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defstateupdater ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          args (vec args)
                          prepargs [(gensym "conf") (gensym "context")]]
                      `(fn ~prepargs (state-updater (~'updateState ~args ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-state-updater ~fn-name args#))
                    `(def ~name
                       (clojure-state-updater ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; ## statefactory
(defn clojure-state-factory* [fn-var args]
  (ClojureStateFactory. (to-spec fn-var) args))

(defmacro clojure-state-factory [fn-sym args]
  `(clojure-state-factory* (var ~fn-sym) ~args))

(defmacro state-factory [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.state.StateFactory
       ~@fns)))

(defmacro defstatefactory [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defstatefactory ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          [args & impl-body] impl
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-state-factory ~fn-name args#))
                    `(def ~name
                       (clojure-state-factory ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           (state-factory
            (~'makeState ~(vec args) ~@impl-body)))
         ~definer))))

;; ## backingmap
(defn clojure-backing-map* [fn-var args]
  (ClojureBackingMap. (to-spec fn-var) args))

(defmacro clojure-backing-map [fn-sym args]
  `(clojure-backing-map* (var ~fn-sym) ~args))

(defmacro backing-map [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify storm.trident.state.map.IBackingMap
       ~@fns)))

(defmacro defbackingmap [name & [opts & [get-impl put-impl] :as all]]
  (if-not (map? opts)
    `(defbackingmap ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          [get-args & get-body] get-impl
          [put-args & put-body] put-impl
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-backing-map ~fn-name args#))
                    `(def ~name
                       (clojure-backing-map ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           (backing-map
            (~'multiGet ~(vec get-args) ~@get-body)
            (~'multiPut ~(vec put-args) ~@put-body)))
         ~definer))))

(defn values [& v]
  (Values. (into-array Object v)))

(defn fields [& v]
  (Fields. (into-array String (map name v))))


;; for use in defbasefn
(defn emit-fn
  [coll & vs]
  (.emit coll (apply values vs)))

;; helpers for building up streams
(defn each
  ([stream a b]
     (if (sequential? a)
       (.each stream
              (apply fields a)
              b)
       (.each stream
              a
              (apply fields b))))
  ([stream in-fields fn-inst fn-fields]
     (.each stream
            (apply fields in-fields)
            fn-inst
            (apply fields fn-fields))))

(defn shuffle
  [stream]
  (.shuffle stream))

(defn group-by
  [stream group-fields]
  (.groupBy stream (apply fields group-fields)))

(defn partition-by
  [stream group-fields]
  (.partitionBy stream (apply fields group-fields)))

(defn batch-global
  [stream]
  (.batchGlobal stream))

(defn partition-persist
  ([stream state-spec in-fields state-updater]
    (.partitionPersist stream
                       state-spec
                       (apply fields in-fields)
                       state-updater))
  ([stream state-spec in-fields state-updater out-fields]
    (.partitionPersist stream
                       state-spec
                       (apply fields in-fields)
                       state-updater
                       (apply fields out-fields))))

(defn persistent-aggregate
  ([stream state fn-inst fn-fields]
     (.persistentAggregate stream
                           state
                           fn-inst
                           (apply fields fn-fields)))
  ([stream state in-fields fn-inst fn-fields]
     (.persistentAggregate stream
                           state
                           (apply fields in-fields)
                           fn-inst
                           (apply fields fn-fields))))

(defn parallelism-hint
  [stream h]
  (.parallelismHint stream h))

(defn drpc-stream
  [topo name & [drpc]]
  (if drpc
    (.newDRPCStream topo name drpc)
    (.newDRPCStream topo name)))

(defn new-stream
  [topo name spout]
  (.newStream topo name spout))

(defn new-values-stream
  [state]
  (.newValuesStream state))

(defn new-static-state
  [topology state-factory]
  (.newStaticState topology state-factory))

(defn state-query
  [stream state in-fields query-fn fn-fields]
  (.stateQuery stream state
               (apply fields in-fields)
               query-fn
               (apply fields fn-fields)))

(defn project
  [stream keep-fields]
  (.project stream (apply fields keep-fields)))

(defn aggregate
  ([stream agg fn-fields]
     (.aggregate stream
                 agg
                 (apply fields fn-fields)))
  ([stream in-fields agg fn-fields]
     (.aggregate stream
                 (apply fields in-fields)
                 agg
                 (apply fields fn-fields))))

(defn stream-name
  [stream name-str]
  (.name stream name-str))

(defn debug
  [stream & [name]]
  (condp isa? (class stream)
    GroupedStream
    (let [grouped-fields (.getGroupFields stream)]
      (.groupBy (debug (.toStream stream) name)
                grouped-fields))
    TridentState (do (debug (.newValuesStream stream) name) stream)
    (.each stream
           (.getOutputFields stream)
           (if (not (nil? name))
             (Debug. name)
             (Debug.)))))

(defn broadcast
  [stream]
  (.broadcast stream))

(defn join
  [topo
   stream1 fields1
   stream2 fields2
   out-fields]
  (.join topo
         stream1 (apply fields fields1)
         stream2 (apply fields fields2)
         (apply fields out-fields)))

(defn merge
  [topo & streams]
  (.merge topo
          (into-array Stream streams)))

;; (trident/defqueryfn
;;   map-get
;;   ([this tuple res coll]
;;      (trident/emit-fn coll res))
;;   ([this mp kys]
;;      (.multiGet mp kys)))


(defn rotating-map
  [buckets]
  (RotatingMap. buckets))

(defn rotate!
  [rmap]
  (.rotate rmap))


(defn time-cache-map
  [expiration-secs]
  (TimeCacheMap. expiration-secs))


(defprotocol ClojureTridentTuple
  (first [this])
  (count [this])
  (get [this field] [this field not-found])
  (nth [this index] [this index not-found])
  (vals [this]))

(extend-type TridentTupleView
  ClojureTridentTuple
  (first
    [this]
    (.getValue ^TridentTupleView this 0))
  (count
    [this]
    (.size ^TridentTupleView this))
  (get
    ([this field]
       (.getValueByField ^TridentTupleView this (name field)))
    ([this field not-found]
       (or (.getValueByField ^TridentTupleView this (name field))
           not-found)))
  (nth
    ([this index]
       (.getValue ^TridentTupleView this index))
    ([this index not-found]
      (or (.getValue ^TridentTupleView this index)
        not-found)))
  (vals [this]
    (.getValues ^TridentTupleView this)))
