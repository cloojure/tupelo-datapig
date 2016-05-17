(ns datapig.save
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

