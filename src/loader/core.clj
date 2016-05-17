(ns loader.core
  (:require
    [cheshire.core :as cc]
    [clojure.java.jdbc :as jdbc]
    [clojure.string :as str]
    [clojure.walk :refer [keywordize-keys]]
    [java-jdbc.ddl :as ddl]
    [java-jdbc.sql :as sql]
    [schema.core :as s]
    [tupelo.misc :as tm]
    [tupelo.schema :as ts]
  )
  (:use tupelo.core
        criterium.core)
  (:import  com.mchange.v2.c3p0.ComboPooledDataSource
  )
)

(def data-files-limit 99)

(def db-spec
  { :classname    "org.postgresql.Driver"
    :subprotocol  "postgresql"
    :subname      "//localhost:5432/alan"    ; database="alan"
    ;; Not needed for a non-secure local database...
    ;;   :user      "bilbo"
    ;;   :password  "secret"
    :user      "alan"
    :password  "secret"
    } )

(defn drop-tables []
  (newline)
  (println "Dropping tables...")
  (jdbc/db-do-commands db-spec "drop table if exists dummy" ))

(defn create-tables []
  (println "Creating table:  dummy")
  (jdbc/db-do-commands db-spec
    (ddl/create-table :dummy
      [:name :text "not null"]
      [:age :int "not null"]))
  (jdbc/db-do-commands db-spec "create index dummy__name on dummy (name) ;"))

(defn load-pg []
  (drop-tables)
  (create-tables)
  (let [data-dir          "./loader-data" 
        files             (.listFiles (clojure.java.io/file data-dir)) 
        files-to-use      (take data-files-limit files)
  ]
    (println (format "Loading data (%d files)" (count files-to-use)))
    (tm/dots-config! {:decimation 100} )
    (tm/with-dots
      (doseq [file files-to-use]
        (data-load file)))))

(comment
  (defn data-load
    [file-spec]                                             ; a filename or File obj
    (let [engagements (json->clj (slurp file-spec))]
      (jdbc/with-db-transaction [db-conn db-spec]           ; or (jdbc/with-db-connection [db-conn db-spec] ...)
        (doseq [engagement engagements]
          (let [metaData        (glue (sorted-map) (grab :metaData engagement))
                metrics         (grab :metrics engagement)
                chatID          (grab :chatID metaData)
                metaData-keep   (select-keys metaData [:chatID
                                                       :chatEnd
                                                       :pageName
                                                       ])
                engagement-data (glue metaData-keep metrics-keep)
                ]
            (jdbc/insert! db-conn :engagements engagement-data)

            (let [engagement-json (filter-text (escape-single-quote (clj->json engagement)))
                  insert-sql      (format "insert into engagements_json (chatid, json_raw) values ( '%s', '%s'::jsonb );" chatID engagement-json)
                  ]
              (jdbc/db-do-commands db-spec insert-sql))     ; #todo need pig-squeal

            (let [agents (grab :agents metaData)]
              (doseq [agent agents]
                (let [agt-map (glue {:chatID chatID}
                                (select-keys agent [:agentID :name :alias])
                                ;  { :AgentLocation   (only (fetch-in agent [:attributes :AgentLocation]))
                                ;    :Team            (only (fetch-in agent [:attributes :Team])) }
                                )
                      ]
                  (jdbc/insert! db-conn :agents agt-map))))
            ))))))

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

  (defn create-engagements-json-tbl []
    (println "Creating table:  engagements-json")
    (jdbc/db-do-commands db-spec
      (ddl/create-table :engagements_json                   ; #todo need pig-squeal to automate this
        [:chatID :text "PRIMARY KEY"]                       ; #todo need pig-squeal to automate this
        [:json_raw :jsonb]                                  ; #todo need pig-squeal to automate this
        ))
    (jdbc/db-do-commands db-spec
      "create index engagements_json__chatID    on engagements_json             ( chatID  ) ;"
      "create index engagements_json__json_raw  on engagements_json USING GIN   ( json_raw  ) ;"
      ))

  (defn create-engagements-tbl []
    (println "Creating table:  engagements")
    (jdbc/db-do-commands db-spec
      (ddl/create-table :engagements
        ; metaData
        [:chatID :text "PRIMARY KEY"]
        [:chatEnd :text])))



  (defn create-agents-tbl []
    (println "Creating table:  agents")
    (jdbc/db-do-commands db-spec
      (ddl/create-table :agents
        [:chatID :text]
        [:agentID :text]
        [:name :text]
        [:alias "text"]
        ; [:AgentLocation    :text] ; #todo multi-valued
        ; [:Team             :text] ; #todo multi-valued
        )))


  (defn create-transcript-tbl []
    (println "Creating table:  transcript")
    (jdbc/db-do-commands db-spec
      (ddl/create-table :transcript
        ; required keys
        [:chatID :text "not null"]
        [:type :text "not null"]
        [:time :text "not null"]
        [:timeInMlSec :bigint "not null"]
        )))

  (defn escape-single-quote
    [arg-str]
    (.replace arg-str "'" "''"))

)

