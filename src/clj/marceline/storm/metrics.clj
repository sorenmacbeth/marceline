(ns marceline.storm.metrics
  (:import [backtype.storm.metric.api IMetric CountMetric MultiCountMetric])
  (:require [backtype.storm.clojure :refer (to-spec normalize-fns)]
            [backtype.storm.config :refer (TOPOLOGY-METRICS-CONSUMER-REGISTER)])
  (:gen-class))

(defmacro defmetricsconsumer
  [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defmetricsconsumer ~name {} ~@all)
    (let [prefix (gensym)
          classname (str *ns* ".consumer." name)
          state "state"
          init "init"
          [prepare-impl handle-data-points-impl & [cleanup-impl?]] impl
          cleanup-impl (or cleanup-impl? `([_#]))
          ;; options
          parallelism (:p opts 1)
          init-state (:state opts {})]
      `(do
         (gen-class :name ~classname
                    :implements [backtype.storm.metric.api.IMetricsConsumer]
                    :prefix ~prefix
                    :state ~state
                    :init ~init)
         (defn ~(symbol (str prefix "init"))
           []
           [[] (atom ~init-state)])
         (defn ~(symbol (str prefix "prepare"))
           ~@prepare-impl)
         (defn ~(symbol (str prefix "handleDataPoints"))
           ~@handle-data-points-impl)
         (defn ~(symbol (str prefix "cleanup"))
           ~@cleanup-impl)
         ;; this should probably be a fn
         ;; can merge itself properly into any existing conf
         ;; and also take args
         (def ~name
           (fn [conf#]
             (merge-with
              concat
              conf#
              {~TOPOLOGY-METRICS-CONSUMER-REGISTER
               [{"class" ~classname
                 "parallelism.hint" ~parallelism
                 "argument" nil}]})))))))

;; I don't see the value of this argument
;; it's passed during `prepare`, which
;; is also passed the entire conf map.
;; so why not pull whatever arg out of
;; the rest of the conf?

(defmacro metric
  [get-value-and-reset-impl]
  `(reify IMetric
     (getValueAndReset [this]
       (~get-value-and-reset-impl))))

(defn defmetric
  [init-value update-fn]
  (let [s (atom init-value)]
    {:fn (partial swap! s update-fn)
     :m (metric (fn [] (let [v @s]
                         (reset! s init-value)
                         v)))}))

(defn count-metric
  []
  (let [cm (CountMetric.)]
    {:fn (fn [] (.incr cm))
     :m cm}))

(defn multi-count-metric
  []
  (let [mcm (MultiCountMetric.)]
    {:fn (fn [k] (.incr (.scope mcm k)))
     :m mcm}))

(defn register-metrics
  [topology-context metrics]
  (doseq [[name imetric periodicity] metrics]
    (.registerMetric topology-context name imetric (int periodicity))))

;; with-* metrics
(defmacro with-count
  [topology-context periodicity nm & body]
  `(let [m# (count-metric)
         ~nm (:fn m#)]
     (register-metrics ~topology-context [[(str (quote ~nm)) (:m m#) ~periodicity]])
     (do ~@body)))

(defmacro with-multi-count
  [topology-context periodicity nm & body]
  `(let [m# (multi-count-metric)
         ~nm (:fn m#)]
     (register-metrics ~topology-context [[(str (quote ~nm)) (:m m#) ~periodicity]])
     (do ~@body)))


;; helper fns
(defn conf-state!
  "Sets a consumer's state to be the conf map."
  [this-consumer conf]
  (reset! (. this-consumer state) conf))

(defn metric-vals
  [metric-name data-points]
  (doseq [metric-point (filter #(= metric-vals (.name %)) data-points)]
    (.value metric-point)))
