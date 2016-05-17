(defproject loader "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies   [ 
    [cheshire                         "5.5.0"]
    [com.mchange/c3p0                 "0.9.5.2"]
    [criterium                        "0.4.3"]
    [java-jdbc/dsl                    "0.1.3"]
    [org.apache.solr/solr-solrj       "5.4.0"]
    [org.clojure/clojure              "1.8.0"]
    [org.clojure/java.jdbc            "0.4.2"] 
    [org.postgresql/postgresql        "9.4-1206-jdbc42"]
    [prismatic/schema                 "1.0.4"]
    [tupelo                           "0.1.58"]
  ]
  :main ^:skip-aot loader.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
; :resource-paths   [ "local-jars/*" ]
)
