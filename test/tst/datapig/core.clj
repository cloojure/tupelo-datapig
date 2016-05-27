(ns tst.datapig.core
  (:use datapig.core
        clojure.test
        tupelo.core)
  (:require
    [clojure.java.jdbc :as jdbc]
    [clojure.string :as str]
    [clojure.test.check.clojure-test :as tst]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [java-jdbc.ddl :as ddl]
    [java-jdbc.sql :as sql]
    [tupelo.misc :as tm]
  ))

(spyx *clojure-version*)

(deftest t-01
  (is true)
)

#_(deftest pg-basic-t
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

#_(deftest pg-basic-v
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

#_(deftest data-load-validate-t
  (let [filename      "sample.json"
        sampleData    (forv [engagement (json->clj (slurp filename)) ]
                        (update engagement :metaData #(into (sorted-map) %))) ]
    (doseq [engagement sampleData]
      (print \.)
      (try 
        (s/validate Engagement engagement)
        (catch Exception ex (do (ppr engagement) (spyx ex) (System/exit 1)))))
    (newline)))

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
