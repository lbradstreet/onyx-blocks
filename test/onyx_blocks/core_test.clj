(ns onyx-blocks.core-test
  (:require [clojure.test :refer :all]
            [onyx-blocks.core :as c]))

(deftest check-build
  (testing "Build a file reader"
    (c/build-task c/file-read 
                  :some-reader 
                  {:seq/elements-per-segment 1000} 
                  {:some-extra-lifecycle {:hey/there "hello"
                                          :lifecycle/calls :hello/there-calls
                                          :lifecycle/doc "hiii"}
                   :file-reader {:buffered-reader/n-lines 500
                                 :buffered-reader/filename "hithere.txt"}})))
