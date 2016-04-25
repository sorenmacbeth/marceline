(ns marceline.topology.tfidf-test
  (:require [org.apache.storm.testing :as t])
  (:use clojure.test
        org.apache.storm.trident.testing
        marceline.topology.tfidf))

(def DOCUMENTS [["twitter" "0" "a b c d"]
                ["twitter" "1" "a e f g"]
                ["twitter" "2" "a h i j"]
                ["twitter" "3" "a b b k l l l l"]])

(deftest wordcount-drpc
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (let [feeder (feeder-spout ["source" "doc-id" "terms"])
            topology (build-topology feeder drpc)]
        (with-topology [cluster topology wordcount-topology]
          (feed feeder DOCUMENTS)
          (testing "must return correct tfidf value"
            (is (= (exec-drpc drpc "tfidf" "twitter 3 a")
                   [["3" "a" -0.2231435513142097]]))
            (is (= (exec-drpc drpc "tfidf" "twitter 1 a")
                   [["1" "a" -0.2231435513142097]]))
            (is (= (exec-drpc drpc "tfidf" "twitter 3 b")
                   [["3" "b" 0.5753641449035617]]))
            (is (= (exec-drpc drpc "tfidf" "twitter 2 h")
                   [["2" "h" 0.6931471805599453]]))
            (is (= (exec-drpc drpc "tfidf" "twitter 3 l")
                   [["3" "l" 2.772588722239781]])))
          (testing "must return correct value d for a given source"
            (is (= (ffirst (exec-drpc drpc "d" "twitter")) 4)))
          (testing "must return correct value df for a given term"
            (is (= (ffirst (exec-drpc drpc "df" "a")) 4))
            (is (= (ffirst (exec-drpc drpc "df" "b")) 2))
            (is (= (ffirst (exec-drpc drpc "df" "l")) 1)))
          (testing "must return correct value tf for a given [doc-id, term]"
            (is (= (ffirst (exec-drpc drpc "tf" "0 a")) 1))
            (is (= (ffirst (exec-drpc drpc "tf" "3 a")) 1))
            (is (= (ffirst (exec-drpc drpc "tf" "2 h")) 1))
            (is (= (ffirst (exec-drpc drpc "tf" "3 b")) 2))
            (is (= (ffirst (exec-drpc drpc "tf" "3 l")) 4))))))))
