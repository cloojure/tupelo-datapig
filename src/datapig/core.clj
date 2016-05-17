(ns datapig.core
  (:require
    [cheshire.core :as cc]
    [clojure.java.jdbc :as jdbc]
    [clojure.string :as str]
    [clojure.walk :refer [keywordize-keys]]
    [java-jdbc.ddl :as ddl]
    [java-jdbc.sql :as sql]
    [schema.core :as s]
    [tupelo.misc :as tm]
    [tupelo.schema :as ts] )
  (:use tupelo.core )
  (:import  com.mchange.v2.c3p0.ComboPooledDataSource )
)

(def db-spec
  { :classname    "org.postgresql.Driver"
    :subprotocol  "postgresql"
    :subname      "//localhost:5432/alan"    ; database="alan"
    ;; Not needed for a non-secure local database...
    ;;   :user      "bilbo"
    ;;   :password  "secret"
;   :user      "alan"
;   :password  "secret"
    } )

(defn drop-tables []
  (newline)
  (println "Dropping tables...")
  (jdbc/db-do-commands db-spec "drop table if exists dummy" ))

(defn create-tables []
  (println "Creating table:  dummy")
  (jdbc/db-do-commands db-spec
    (ddl/create-table :dummy
      [:name :text "PRIMARY KEY"]
      [:age :int "not null"]))
  (jdbc/db-do-commands db-spec "create index dummy__name on dummy (name) ;"))

(defn load-pg []
  (drop-tables)
  (create-tables)
  (jdbc/db-do-commands db-spec (format "insert into dummy (name, age) values ( '%s', '%d' );" "joe"   22) )
  (jdbc/with-db-transaction [db-conn db-spec]           ; or (jdbc/with-db-connection [db-conn db-spec] ...)
    (jdbc/db-do-commands db-conn (format "insert into dummy (name, age) values ( '%s', '%d' );" "mary"  11) ))
)

(defn -main []
  (println "-main enter")
  (try
    (load-pg)
    (catch Exception ex
      (do (spyx ex)
          (spyx (.getNextException ex))
          (System/exit 1)))))

(comment
  (defn pool
    [spec]
    (let [cpds (doto (ComboPooledDataSource.)
                 (.setDriverClass (:classname spec))
                 (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
                 (.setUser (:user spec))
                 (.setPassword (:password spec))
                 (.setMaxIdleTimeExcessConnections (* 30 60)) ;; expire excess connections after 30 minutes of inactivity:
                 (.setMaxIdleTime (* 3 60 60))              ;; expire connections after 3 hours of inactivity:
                 )]
      {:datasource cpds}))
  (def pooled-db (delay (pool db-spec)))
  (defn db-conn [] @pooled-db)
  (spyx db-conn)
  (spyx (.toString db-conn))

  (format "insert into engagements_json (chatid, json_raw) values ( '%s', '%s'::jsonb );" chatID engagement-json)

  (defn escape-single-quote
    [arg-str]
    (.replace arg-str "'" "''"))
)

