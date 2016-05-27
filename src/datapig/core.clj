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

(def ^:dynamic *conn*)                  ; dynamic var to hold the db connection

(defn drop-namespace [ns-name]
  (jdbc/db-do-commands *conn* (format "drop schema if exists %s" ns-name)))
(defn drop-namespace-force [ns-name]
  (jdbc/db-do-commands *conn* (format "drop schema if exists %s cascade" ns-name)))

(defn create-namespace [ns-name]
  (jdbc/db-do-commands *conn* (format "create schema %s" ns-name))
  (jdbc/db-do-commands *conn* (format "set search_path to %s" ns-name))) ; split out later


(defn drop-table [name-kw]
  (let [name-str (name name-kw)]
    (println "Dropping table:" name-str)
    (spyx (jdbc/db-do-commands *conn* (str "drop table if exists " name-str)))))

(defn create-table [name-kw]
  (let [name-str (name name-kw)]
    (println "Creating table:" name-str)
    (jdbc/db-do-commands *conn*
      (ddl/create-table name-kw
        [:value :text "PRIMARY KEY"]
        [:value2 :int "not null"]))
    (spyx (jdbc/db-do-commands *conn*
            (format "create index %s__value on dummy (value) ;" name-str)))
    (spyx (jdbc/db-do-commands *conn* (format "insert into dummy (value, value2) values ( '%s', '%d' );" "joe" 22)))
    (spyx (jdbc/with-db-transaction [db-tx *conn*]       ; or (jdbc/with-db-connection [db-conn db-spec] ...)
            (jdbc/db-do-commands db-tx (format "insert into dummy (value, value2) values ( '%s', '%d' );" "mary" 11))))
  ))


(defn load-pg []
  (drop-table :dummy)
  (create-table :dummy)
)

(defn -main []
  (println "-main enter")
  (try
    (load-pg)
    (catch Exception ex
      (do (spyx ex)
          (spyx (.getNextException ex))
          (System/exit 1)))))

