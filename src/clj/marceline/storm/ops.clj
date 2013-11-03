(ns marceline.storm.ops
  (:require [marceline.storm.trident :as t])
  (:refer-clojure :exclude [count])
  (:gen-class))

(t/defcombineraggregator count
  ([] 0)
  ([tuple] 1)
  ([t1 t2] (+ t1 t2)))

(t/defcombineraggregator one
  ([] 1)
  ([tuple] 1)
  ([t1 t2] 1))
