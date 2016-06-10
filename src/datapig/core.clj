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

(defn set-transaction-isolation-serializable []
  "Programmatically sets the 'default_transaction_isolation' property for the database.
  May wish to set in /etc/postgresql/9.5/main/postgresql.conf"
  (try
    ; Affects subsequent sessions, but not current one.
    (jdbc/db-do-commands *conn* "ALTER DATABASE datapig
                                  SET default_transaction_isolation='serializable' ")
    ; Changes current session but not permanent
    (jdbc/db-do-commands *conn*  "SET default_transaction_isolation='serializable' ")
    (catch Exception ex
      (do (spyx ex)
          (spyx (.getNextException ex))
          (System/exit 1)))))

(s/defn drop-namespace-force
  [ns-name :- s/Keyword]
  (let [ns-str (name ns-name)]
    (println "drop-namespace-force:" ns-name)
    (jdbc/db-do-commands *conn* (format "drop schema if exists %s cascade" ns-str))))

(s/defn create-namespace
  [ns-name :- s/Keyword]
  (let [ns-str (name ns-name)]
    (println "create-namespace:" ns-name)
    (set-transaction-isolation-serializable)
    (jdbc/db-do-commands *conn* (format "create schema %s" ns-str))
    (jdbc/db-do-commands *conn* (format "set search_path to %s" ns-str))
    (jdbc/db-do-commands *conn* "create sequence seq__eid")
    (jdbc/db-do-commands *conn*
      (ddl/create-table :entity [:eid :int8 "PRIMARY KEY"])) ; #todo split out later
  ))

; #todo cardinality:
;   one: unique eid in attr table: "unique (eid)"
;   many:   set:  unique value in attr table: "unique (eid, value)"
;           list: no restriction on dups
;   ident: unique in whole table: "unique (value)"
;   component: "on delete cascade"  (need "on delete restrict"?)
(s/defn create-attribute
  [attr :- s/Keyword
   type :- s/Keyword
   props :- s/Str ]
  ; #todo validate name, type, props
  (set-transaction-isolation-serializable)
  (let [tbl-name  (attr-tbl-name attr)
        attr-str  (name attr)
        db-type   (type-map type)
        props-str props]                                   ; #todo fix
    (println "create-attribute:" attr "  db-type" db-type)
    (jdbc/db-do-commands *conn*
      (ddl/create-table tbl-name
        [:eid             :int8   "not null references entity (eid)"]
        [(keyword attr)   db-type "not null"] ))
    (jdbc/db-do-commands *conn*
      ; #todo: strip off "attr__" part of index name?
      (format "create index idx__%s__ev on %s (eid, %s) ;" tbl-name tbl-name attr-str)
      (format "create index idx__%s__ve on %s (%s, eid) ;" tbl-name tbl-name attr-str))
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
  (let [eid (-> (jdbc/query *conn* ["select nextval('seq__eid');"])
              (only)
              (:nextval))]
    (println "create-entity:" eid "  attrvals:" attrvals)
    (jdbc/with-db-transaction [db-tx *conn*]
      (jdbc/db-do-commands db-tx (format "insert into entity (eid) values (%d);" eid))
      (doseq [[attr value] (vec attrvals)]
        (let [attr-str (name attr)]
        (println "attr=" attr-str "  value=" value)
        (jdbc/db-do-commands db-tx (str "insert into " (attr-tbl-name attr)
                                     " (eid, " attr-str ") values ( '" eid "', '" value "' );")))))))


(defn -main []
  (println "-main enter"))

(comment
"  *** All versions below work correctly *****

> with eids as (select * from attr__age where (age=33))
    select * from ((attr__name natural join attr__age) natural join eids);

> with eids as (select * from attr__age where (age=33))
    select * from ((eids natural join attr__name) natural join attr__age);

> with eids as (select * from attr__age where (age=33))
    select * from (eids natural join (attr__name natural join attr__age));

> select * from ( attr__name  natural join
                  attr__age )
           where (age < 40);

> with eids as
    (select eid from attr__age where (age < 40))
  select * from ( eids        natural join
                  attr__name  natural join
                  attr__age );

 eid | name  | age
-----+-------+-----
   1 | jesse |  33

> with  eids_1 as (select eid from attr__age where (age < 40))
      , eids_2 as (select eid from attr__age where (age > 20))
  select * from (
    eids_1      natural join
    eids_2      natural join
    attr__name  natural join
    attr__age );

> with  res_1 as (select * from ( attr__name  natural join
                                  attr__age ) where (age < 40))
      , res_2 as (select * from ( attr__name  natural join
                                  attr__age ) where (age > 20))
  select * from ( res_1
    natural join  res_2 );

> with  res_1 as (select * from ( attr__name  natural join
                                  attr__age ) where (age < 40))
  select * from res_1 where (res_1.age > 20);


 eid |  name  | age
-----+--------+-----
   2 | bob    |  22
   3 | carrie |  33

ARRAYS:
  create  table ta ( xx int[] );
  insert into ta values ( '{1,2,3}' );
  insert into ta values ( array[2,3,4,5] );     ; *** preferred notation ***
  select * from ta;
        xx
    -----------
     {1,2,3}
     {2,3,4,5}

  select xx[1] from ta;    ; indexes are 1-based
     xx
    ----
      1
      2

  select xx[1:2] from ta;  ; slices are [n:m] notation
      xx
    -------
     {1,2}
     {2,3}

  select array_dims(xx) from ta;      ; returns dims for each element
     array_dims
    ------------
     [1:3]
     [1:4]

  select array_length(xx,1) from ta;    ; returns dims for each element (needs ',1' part)
      array_length
    --------------
                3
                4
  select cardinality(xx) from ta;     ; might be easier, but different for 2D arrays
     cardinality
    -------------
               3
               4

" )
