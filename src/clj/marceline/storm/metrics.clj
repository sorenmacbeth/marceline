(ns marceline.storm.metrics
  (:require [backtype.storm.clojure :refer (to-spec normalize-fns)])
  (:import [marceline.storm.trident.clojure ClojureMetricsConsumer])
  (:gen-class))

;; ## metrics consumer
(defn clojure-metrics-consumer* [fn-var args]
  (ClojureMetricsConsumer. (to-spec fn-var) args))

(defmacro clojure-metrics-consumer [fn-sym args]
  `(clojure-metrics-consumer* (var ~fn-sym) ~args))

(defmacro metrics-consumer [& body]
  (let [[base-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns base-fns)]
    `(reify backtype.storm.metric.api.IMetricsConsumer
       ~@fns)))

(defmacro defmetricsconsumer [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defmetricsconsumer ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          prepargs [(gensym "conf") (gensym "registation-argument") (gensym "context") (gensym "error-reporter")]]
                      `(fn ~prepargs (metrics-consumer
                                      (~'handleDataPoints ~(vec args) ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-metrics-consumer ~fn-name args#))
                    `(def ~name
                       (clojure-metrics-consumer ~fn-name [])))]
      `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))
