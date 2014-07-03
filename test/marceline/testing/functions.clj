(ns marceline.testing.functions
  (:import [storm.trident.testing MockTridentTuple])
  (:require [marceline.storm.testing :refer :all]
            [marceline.storm.trident :as t]
            [clojure.test :refer :all]))

(deftest mock-tuple
  (let [^MockTridentTuple tuple (MockTridentTuple. ["a" "b" "c"] [1 "bar" 4.3])]
    (is (= 1 (first tuple)))
    (is (= 4.3 (nth tuple 2)))
    (is (= 3 (count tuple)))
    (is (= [1 "bar" 4.3] tuple))
    (is (= "bar" (t/get tuple "b")))))