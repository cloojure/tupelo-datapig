(ns tst.loader.core
  (:require [schema.core            :as s]
            [clojure.java.jdbc      :as jdbc]
            [java-jdbc.ddl          :as ddl]
            [java-jdbc.sql          :as sql]
  )
  (:use clojure.test 
        loader.core
        tupelo.core))

(deftest pg-basic-t
  (prn 'pg-basic)
  (jdbc/db-do-commands db-spec "drop table if exists langs" )
  (jdbc/db-do-commands db-spec 
    (ddl/create-table :langs
     [:id     :serial   "primary key"]
     [:name   :text     "NOT NULL"]
     [:score  "real"] ))
  (jdbc/insert! db-spec :langs
    {:name "Clojure"  :score 9.8}
    {:name "Java"     :score 7.5} )
  (jdbc/query db-spec 
    (sql/select * :langs (sql/where {:name "Clojure"} )))

  )

(deftest pg-basic-v
  (when false
    (spyx (jdbc/db-do-commands db-spec "drop table if exists langs" ))
    (spyx (jdbc/db-do-commands db-spec 
            (spyx
              (ddl/create-table :langs
               [:id     :serial   "PRIMARY KEY"]
               [:name   :text     "NOT NULL"] ))))
    (spyx (jdbc/insert! db-spec :langs
            {:name "Clojure"}
            {:name "Java"} ))
    (spyx (jdbc/query db-spec 
            (spyx (sql/select * :langs (sql/where {:name "Clojure"} )))))))

(deftest data-load-validate-t
  (let [filename      "sample.json"
        sampleData    (forv [engagement (json->clj (slurp filename)) ]
                        (update engagement :metaData #(into (sorted-map) %))) ]
    (doseq [engagement sampleData]
      (print \.)
      (try 
        (s/validate Engagement engagement)
        (catch Exception ex (do (ppr engagement) (spyx ex) (System/exit 1)))))
    (newline)))

; #todo migrate to testing schema
#_(deftest load-1-t
  (create-tables)
  (let [filename "sample.json" ]
    (data-load filename))
)

  ; select  e.chatStart, e.chatID, a.agentid  from  engagements e natural join  agents a   
  ;           where a.agentid='bzdilma';
  ; select  count(*)                          from  engagements   natural join  agents     
  ;           where agentid='bzdilma';
  ; create index on agents (chatid);
  ; create index on agents (agentid);
