(ns marceline.testing.filters
  "Tests for filters"
  (:require [marceline.storm.testing :refer :all]
            [marceline.storm.trident :as t]
            [clojure.test :refer :all]))

;;only keeps tuples, with even first values
(t/deffilter only-even
             [tuple]
             (even? (first tuple)))

(deftest test-run-filter
  (is (= true (run-filter only-even :a 2)))
  (is (= false (run-filter only-even :a 3))))


