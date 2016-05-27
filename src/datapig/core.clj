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
  (println "creating namespace:" ns-name)
  (jdbc/db-do-commands *conn* (format "create schema %s" ns-name))
  (jdbc/db-do-commands *conn* (format "set search_path to %s" ns-name))
  (jdbc/db-do-commands *conn* "create sequence eid_seq")
  (jdbc/db-do-commands *conn*
    (ddl/create-table :entity [:eid :int8 "PRIMARY KEY"]))
) ; #todo split out later

(def type-map
  {:integer :int8
   :int     :int8
   :int8    :int8
   :string  :text
   :float   :float8
   :double  :float8
   :decimal :numeric
   :numeric :numeric} )

(defn attr-tbl-name [attr-kw]
  (str "attr__" (name attr-kw)))

; #todo cardinality:
;   one: unique eid in attr table: "unique (eid)"
;   many:   set:  unique value in attr table: "unique (eid, value)"
;           list: no restriction on dups
;   ident: unique in whole table: "unique (value)"
;   component: "on delete cascade"  (need "on delete restrict"?)
(defn create-attribute
  [-attr -type -props]
  ; #todo validate name, type, props
  (let [tbl-name  (attr-tbl-name -attr)
        db-type   (type-map -type)
        props-str -props]                                   ; #todo fix
    (println "creating attr:" -attr "  db-type" db-type)
    (jdbc/db-do-commands *conn*
      (ddl/create-table tbl-name
        [:eid    :int8   "not null references entity (eid)"]
        [:value  db-type "not null"] ))
    (jdbc/db-do-commands *conn*
      ; #todo: strip off "attr__" part of index name?
      (format "create index idx__%s__ev on %s (eid, value) ;" tbl-name tbl-name)
      (format "create index idx__%s__ve on %s (value, eid) ;" tbl-name tbl-name))
  ))

; #todo: can use transactions to implement tdp/with ?

(defn create-entity
  "Creates a new entity and returns the EID"
  [ attrvals-map ]
  (let [eid (-> (jdbc/query *conn* ["select nextval('eid_seq');"])
              (only)
              (:nextval))]
    (jdbc/db-do-commands *conn* (format "insert into entity (eid) values (%d);" eid))
    (spy :msg "create-entity:" eid)
    (doseq [ [attr value] (vec attrvals-map )]
      (println "attr=" attr "  value=" value)
      (jdbc/db-do-commands *conn* (spyx (str "insert into " (attr-tbl-name attr)
                                          " (eid, value) values ( '" eid "', '" value "' );")))
    )
  ))

(defn drop-table-force [name-kw]
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
    (spyx (jdbc/db-do-commands *conn* (format "insert into dummy (value, value2) values ( '%s', '%d' );" "joe" 11)))
    (spyx *conn*)
    (spyx (jdbc/with-db-transaction [db-tx *conn*]       ; or (jdbc/with-db-connection [db-conn db-spec] ...)
            (spyx db-tx)
            (jdbc/db-do-commands db-tx (format "insert into dummy (value, value2) values ( '%s', '%d' );" "mary" 22))))
  ))


(defn load-pg []
  (drop-table-force :dummy)
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

