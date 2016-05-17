(defproject datapig "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies   [ 
    [cheshire                         "5.6.1"]
    [com.mchange/c3p0                 "0.9.5.2"]
    [criterium                        "0.4.4"]
    [java-jdbc/dsl                    "0.1.3"]
    [org.apache.solr/solr-solrj       "6.0.0"]
    [org.clojure/clojure              "1.8.0"]
    [org.clojure/java.jdbc            "0.6.1"] 
    [org.postgresql/postgresql        "9.4.1208"]
    [prismatic/schema                 "1.1.1"]
    [tupelo                           "0.1.71"]
  ]
  :main ^:skip-aot loader.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
; :resource-paths   [ "local-jars/*" ]
)
