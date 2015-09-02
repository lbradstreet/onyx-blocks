(ns onyx-blocks.core-test
  (:require [clojure.test :refer :all]
            [onyx-blocks.file-reader :as fr]
            [onyx-blocks.core :as c]))

(deftest check-build
  (testing "Build a file reader"
    (is (c/build-task fr/file-reader 
                      :a-task-name 
                      {:seq/elements-per-segment 1000} 
                      {:some-extra-lifecycle {:hey/there "hello"
                                              :lifecycle/calls :hello/there-calls
                                              :lifecycle/doc "hiii"}
                       :file-reader {:buffered-reader/n-lines 500
                                     :buffered-reader/filename "hithere.txt"}}))))
