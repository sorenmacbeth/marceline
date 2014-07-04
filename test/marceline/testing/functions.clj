(ns marceline.testing.functions
  (:import [storm.trident.testing MockTridentTuple])
  (:require [marceline.storm.testing :refer :all]
            [marceline.storm.trident :as t]
            [clojure.test :refer :all]))

(deftest mock-tuple
  (let [^MockTridentTuple tuple (MockTridentTuple. ["a" "b" "c"] [1 "bar" 4.3])]
    (is (= 1 (t/first tuple)))
    (is (= 4.3 (t/nth tuple 2)))
    (is (= 3 (t/count tuple)))
    (is (= [1 "bar" 4.3] (t/vals tuple)))
    (is (= "bar" (t/get tuple "b")))))

(deftest test-new-mock-tuple
  (let [tuple (new-mock-tuple :a 1 :b "bar" :c 4.3)]
    (is (= 1 (t/first tuple)))
    (is (= 4.3 (t/nth tuple 2)))
    (is (= 3 (t/count tuple)))
    (is (= [1 "bar" 4.3] (t/vals tuple)))
    (is (= "bar" (t/get tuple "b")))))

(t/deftridentfn t-double
                [tuple coll]
                (t/emit-fn coll (* 2 (first tuple))))

(t/deftridentfn multiply-x-y
                [tuple coll]
                (let [x (t/get tuple "x")
                      y (t/get tuple "y")]
                  (t/emit-fn coll (* x y))))

(t/deftridentfn emit-five
                [tuple coll]
                (dotimes [n 5]
                  (t/emit-fn coll n)))

(t/deftridentfn emit-orig-and-double
                [tuple coll]
                (t/emit-fn coll (first tuple) (* 2 (first tuple))))

(deftest test-run-function
  (is (= [[4]] (run-function t-double :a 2)))
  (is (= [[6]] (run-function multiply-x-y :x 2 :y 3)))
  (is (= [[0] [1] [2] [3] [4]] (run-function emit-five)))
  (is (= [[2 4]] (run-function emit-orig-and-double :a 2))))