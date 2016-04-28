(ns marceline.storm.metrics
  (:import [org.apache.storm.metric.api IMetric CountMetric MultiCountMetric])
  (:require [org.apache.storm.clojure :refer [to-spec normalize-fns]]
            [org.apache.storm.config :refer [TOPOLOGY-METRICS-CONSUMER-REGISTER]])
  (:gen-class))

(defn register-consumer
  [conf class-or-classname & [parallelism arg]]
  (merge-with
   concat
   conf
   {TOPOLOGY-METRICS-CONSUMER-REGISTER
    [{"class" (condp isa? (class class-or-classname)
                java.lang.Class (.getName class-or-classname)
                java.lang.String class-or-classname)
      "parallelism.hint" parallelism
      "argument" arg}]}))

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
                    :implements [org.apache.storm.metric.api.IMetricsConsumer]
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
         (def ~name
           (fn [conf# & [arg#]]
             (register-consumer conf# ~classname ~parallelism arg#)))))))

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
    {:fn (fn
           ([] (.incr cm))
           ([v] (.incrBy cm (long v))))
     :m cm}))

(defn multi-count-metric
  []
  (let [mcm (MultiCountMetric.)]
    {:fn (fn
           ([k] (.incr (.scope mcm (name k))))
           ([k v] (.incrBy (.scope mcm (name k))
                           (long v))))
     :m mcm}))

(defn register-metrics
  [topology-context metrics]
  (doseq [[name imetric periodicity] metrics]
    (.registerMetric topology-context name imetric (int periodicity))))

;; for setting up metrics in components
(defn builtin-or-custom-metric
  [k]
  (if (map? k)
    k
    (case k
      :count (count-metric)
      :multi-count (multi-count-metric))))

(defmacro with-metric
  [topology-context periodicity title mt & body]
  `(let [m# (builtin-or-custom-metric ~mt)
         ~title (:fn m#)]
     (register-metrics ~topology-context [[(str (quote ~title)) (:m m#) ~periodicity]])
     (do ~@body)))

(defmacro with-metrics
  [topology-context periodicity mts & body]
  (let [p (partition 2 mts)]
    (if (< 1 (count p))
      `(with-metric ~topology-context ~periodicity ~@(first p)
         (with-metrics ~topology-context ~periodicity ~(apply concat (rest p)) ~@body))
      `(with-metric ~topology-context ~periodicity ~@(first p) ~@body))))


;; helper fns
(defn reset-state!
  "Sets a consumer's state to val."
  [this-consumer val]
  (reset! (. this-consumer state) val))

(defn metric-vals
  [metric-name data-points]
  (doseq [metric-point (filter #(= metric-vals (.name %)) data-points)]
    (.value metric-point)))
