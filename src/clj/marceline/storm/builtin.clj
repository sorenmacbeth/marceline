(ns marceline.storm.builtin
  (:import [org.apache.storm.trident.operation.builtin
            Count Equals FilterNull
            FirstN MapGet Negate
            SnapshotGet Sum
            TupleCollectionGet])
  (:require [marceline.storm.trident :as t])
  (:refer-clojure :exclude [count]))

(defn count
  []
  (Count.))

(defn equals
  []
  (Equals.))

(defn filter-null
  []
  (FilterNull.))

(defn first-n
  ([n sort-field]
     (FirstN. n sort-field))
  ([n sort-field reverse]
     (FirstN. n sort-field reverse)))

(defn map-get
  []
  (MapGet.))

(defn negate
  [filt]
  (Negate. filt))

(defn snapshot-get
  []
  (SnapshotGet.))

(defn sum
  []
  (Sum.))

(defn tuple-collection-get
  []
  (TupleCollectionGet.))
