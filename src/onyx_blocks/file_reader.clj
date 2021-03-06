(ns onyx-blocks.file-reader
  (:require [schema.core :as s]
            [onyx.schema :as so :refer [TaskName TaskMap Job Lifecycle]])
  (:import [java.io BufferedReader FileReader]))

(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. (:buffered-reader/filename lifecycle))
        n-lines (:buffered-reader/n-lines lifecycle)] 
    {:seq/rdr rdr
     :seq/seq (take n-lines (line-seq (BufferedReader. rdr)))}))

(defn close-reader [event lifecycle]
  (.close (:seq/rdr event))
  {})

(def reader-in-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})

(def file-reader 
  {:task-map {:base {:onyx/plugin :onyx.plugin.seq/input
                     :seq/elements-per-segment 10
                     :onyx/type :input
                     :onyx/medium :seq
                     :onyx/batch-size 100
                     :onyx/max-peers 1
                     :onyx/doc "Reads log data from line-seq"}
              :opts/schema {(s/optional-key :seq/elements-per-segment) s/Int
                            s/Any s/Any}}
   :lifecycles {:file-reader {:base {:lifecycle/calls ::reader-in-calls}
                              :opts/schema {:buffered-reader/filename s/Str
                                            :buffered-reader/n-lines s/Int
                                            s/Any s/Any}}
                :seq {:base {:lifecycle/calls :onyx.plugin.seq/reader-calls}
                      :opts/schema {s/Any s/Any}}}})

