(ns onyx-blocks.core
  (:require [schema.core :as s]
            [onyx.schema :as so :refer [TaskName TaskMap Job Lifecycle]]))

(s/defn ^:always-validate build-lifecycle :- Lifecycle [task-name base opts-schema opts]
  (s/validate opts-schema opts)
  (-> base
      (assoc :lifecycle/task task-name)
      (merge opts)))

(s/defn ^:always-validate build-task :- {:task-map TaskMap
                                         :lifecycles [Lifecycle]} 
  [{:keys [task-map] :as library} task-name task-map-opts lifecycle-opts]
  (s/validate (:opts/schema task-map) task-map-opts)
  (let [task-map-base (:base task-map)
        task-map-full (-> task-map-base 
                          (assoc :onyx/name task-name)
                          (merge task-map-opts))]
    {:task-map task-map-full
     :lifecycles (reduce (fn [lifecycles [k lifecycle]]
                           (let [lifecycle-library (get (:lifecycles library) k)
                                 opts-schema (or (:opts/schema lifecycle-library)
                                                 {s/Any s/Any})] 
                             (conj lifecycles 
                                   (build-lifecycle task-name 
                                                    (:base lifecycle-library)
                                                    opts-schema
                                                    (get lifecycle-opts k))))) 
                         []
                         lifecycle-opts)}))
