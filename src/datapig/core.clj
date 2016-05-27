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
)

(def db-spec
  { :classname    "org.postgresql.Driver"
    :subprotocol  "postgresql"
    :subname      "//localhost:5432/alan"    ; database="alan"
    ;; Not needed for a non-secure local database...
    ;;   :user      "bilbo"
    ;;   :password  "secret"
    } )

(defn drop-table [name-kw]
  (let [name-str (name name-kw)]
    (println "Dropping table:" name-str)
    (jdbc/db-do-commands db-spec (str "drop table if exists " name-str))))

(defn create-table [name-kw]
  (let [name-str (name name-kw) ]
  (println "Creating table:" name-str)
  (jdbc/db-do-commands db-spec
    (ddl/create-table name-kw
      [:value :text "PRIMARY KEY"]
      [:value2 :int "not null"]))
  (jdbc/db-do-commands db-spec
    (format "create index %s__value on dummy (value) ;" name-str ))))

(defn load-pg []
  (drop-table :dummy)
  (create-table :dummy)
  (jdbc/db-do-commands db-spec (format "insert into dummy (value, value2) values ( '%s', '%d' );" "joe"   22) )
  (jdbc/with-db-transaction [db-conn db-spec]           ; or (jdbc/with-db-connection [db-conn db-spec] ...)
    (jdbc/db-do-commands db-conn (format "insert into dummy (value, value2) values ( '%s', '%d' );" "mary"  11) ))
)

(defn -main []
  (println "-main enter")
  (try
    (load-pg)
    (catch Exception ex
      (do (spyx ex)
          (spyx (.getNextException ex))
          (System/exit 1)))))

