(ns marceline.testing.aggregator
  "Tests for combiner aggregators"
  (:require [marceline.storm.testing :refer :all]
            [marceline.storm.trident :as t]
            [clojure.test :refer :all]))

;; basic aggregator
(t/defcombineraggregator sum-values
                         ([] 0)
                         ([tuple]
                          (reduce + tuple))
                         ([t1 t2]
                          (+ t1 t2)))

;; aggregator that doesn't use the same data type
(t/defcombineraggregator sum-character-ascii
                         ([] 0)
                         ([tuple]
                          (reduce + (map int tuple)))
                         ([t1 t2]
                          (+ t1 t2)))

(deftest test-run-combine-aggregator
  (is (= {:zero 0 :init 6 :combine 21 :combine-zero 21}) (run-combiner-aggregator sum-values [:a 1 :b 2 :c 3] [:a 4 :b 5 :c 6]))
  (is (= {:zero 0 :init 294 :combine (+ 303 294) :combine-zero 294}) (run-combiner-aggregator sum-character-ascii [:a \a :b \b :c \c] [:a \d :b \e :c \f])))




