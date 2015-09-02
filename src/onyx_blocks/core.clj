(ns onyx-blocks.core
  (:require [schema.core :as s]
            [onyx.schema :as so :refer [TaskName TaskMap Job Lifecycle]]))

(def file-read 
  {:task-map {:base {:onyx/plugin :onyx.plugin.seq/input
                     :seq/elements-per-segment 10
                     :onyx/type :input
                     :onyx/medium :seq
                     :onyx/batch-size 100
                     :onyx/max-peers 1
                     :onyx/doc "Reads log data from line-seq"}
              :opts-schema {(s/optional-key :seq/elements-per-segment) s/Int
                            s/Any s/Any}}
   :lifecycles {:file-reader {:base {:lifecycle/calls :mpm-onyx.lifecycles.file-reader/reader-in-calls}
                              :opts-schema {:buffered-reader/filename s/Str
                                            :buffered-reader/n-lines s/Int
                                            s/Any s/Any}}
                :seq {:base {:lifecycle/calls :onyx.plugin.seq/reader-calls}
                      :opts-schema {s/Any s/Any}}}})

(s/defn ^:always-validate process-lifecycle :- Lifecycle [task-name base opts-schema opts]
  (s/validate opts-schema opts)
  (-> base
      (assoc :lifecycle/task task-name)
      (merge opts)))

(s/defn ^:always-validate build-task :- {:task-map TaskMap
                                         :lifecycles [Lifecycle]} 
  [{:keys [task-map] :as library} task-name task-map-opts lifecycle-opts]
  (s/validate (:opts-schema task-map) task-map-opts)
  (let [task-map-base (:base task-map)
        task-map-full (-> task-map-base 
                          (assoc :onyx/name task-name)
                          (merge task-map-opts))]
    {:task-map task-map-full
     :lifecycles (reduce (fn [lifecycles [k lifecycle]]
                           (let [lifecycle-library (get (:lifecycles library) k)
                                 opts-schema (or (:opts-schema lifecycle-library)
                                                 {s/Any s/Any})] 
                             (conj lifecycles 
                                   (process-lifecycle task-name 
                                                      (:base lifecycle-library)
                                                      opts-schema
                                                      (get lifecycle-opts k))))) 
                         []
                         lifecycle-opts)}))

(build-task file-read 
            :some-reader 
            {:seq/elements-per-segment 1000} 
            {:some-extra-lifecycle {:hey/there "hello"
                                    :lifecycle/calls :hello/there
                                    :lifecycle/doc "hiii"}
             :file-reader {:buffered-reader/n-lines 500
                           :buffered-reader/filename "hithere.txt"}})
