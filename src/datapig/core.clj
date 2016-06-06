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

; #todo: can use transactions to implement tdp/with ?

; #todo need to always set this. how?
(def ^:dynamic *conn*)  ; dynamic var to hold the db connection

(defn set-transaction-isolation-serializable []
  "Programmatically sets the 'default_transaction_isolation' property for the database.
  May wish to set in /etc/postgresql/9.5/main/postgresql.conf"
  ; Affects subsequent sessions, but not current one.
  (jdbc/db-do-commands *conn*
    "ALTER DATABASE datapig SET default_transaction_isolation='serializable' ")
  ; Changes current session but not permanent
  (jdbc/db-do-commands *conn*  "SET default_transaction_isolation='serializable' "))

(defn drop-namespace-force [ns-name]
  (println "drop-namespace-force:" ns-name)
  (jdbc/db-do-commands *conn* (format "drop schema if exists %s cascade" ns-name)))

(defn create-namespace [ns-name]
  (println "create-namespace:" ns-name)
  (set-transaction-isolation-serializable)
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
  (set-transaction-isolation-serializable)
  (let [tbl-name  (attr-tbl-name -attr)
        db-type   (type-map -type)
        props-str -props]                                   ; #todo fix
    (println "create-attribute:" -attr "  db-type" db-type)
    (jdbc/db-do-commands *conn*
      (ddl/create-table tbl-name
        [:eid    :int8   "not null references entity (eid)"]
        [:value  db-type "not null"] ))
    (jdbc/db-do-commands *conn*
      ; #todo: strip off "attr__" part of index name?
      (format "create index idx__%s__ev on %s (eid, value) ;" tbl-name tbl-name)
      (format "create index idx__%s__ve on %s (value, eid) ;" tbl-name tbl-name))
  ))

(s/defn drop-table-force
  [table-name :- s/Keyword]
  (let [name-str (name table-name)]
    (println "drop-table-force:" name-str)
    (jdbc/db-do-commands *conn* (str "drop table if exists " name-str))))

(s/defn create-table
  [table-name :- s/Keyword]
  (let [name-str (name table-name)]
    (println "create-table:" name-str)
    (jdbc/db-do-commands *conn*
      (ddl/create-table table-name
        [:value :text "PRIMARY KEY"]
        [:value2 :int "not null"]))
    (jdbc/db-do-commands *conn*
      (format "create index %s__value on dummy (value) ;" name-str))
    (jdbc/db-do-commands *conn* (format "insert into dummy (value, value2) values ( '%s', '%d' );" "joe" 11))
    (spyx *conn*)
    (jdbc/with-db-transaction [db-tx *conn*] ; or (jdbc/with-db-connection [db-conn db-spec] ...)
      (spyx db-tx)
      (jdbc/db-do-commands db-tx (format "insert into dummy (value, value2) values ( '%s', '%d' );" "mary" 22)))
    ))

(s/defn create-entity
  "Creates a new entity and returns the EID"
  [ attrvals :- ts/KeyMap ]
  (let [eid (-> (jdbc/query *conn* ["select nextval('eid_seq');"])
              (only)
              (:nextval))]
    (println "create-entity:" eid "  attrvals:" attrvals)
    (jdbc/with-db-transaction [db-tx *conn*]
      (jdbc/db-do-commands db-tx (format "insert into entity (eid) values (%d);" eid))
      (doseq [[attr value] (vec attrvals)]
        (println "attr=" attr "  value=" value)
        (jdbc/db-do-commands db-tx (str "insert into " (attr-tbl-name attr)
                                     " (eid, value) values ( '" eid "', '" value "' );"))))))


(defn -main []
  (println "-main enter")
)

