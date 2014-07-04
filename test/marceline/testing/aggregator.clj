(ns marceline.testing.aggregator
  "Tests for combiner aggregators"
  (:require [marceline.storm.testing :refer :all]
            [marceline.storm.trident :as t]
            [clojure.test :refer :all]))

(t/defcombineraggregator sum-values
                         ([] 0)
                         ([tuple]
                          (reduce + tuple))
                         ([t1 t2]
                          (+ t1 t2)))

(deftest test-run-combine-aggregator
  (is (= {:zero 0 :init 6 :combine 3}) (run-combiner-aggregator sum-values :a 1 :b 2 :c 3)))
