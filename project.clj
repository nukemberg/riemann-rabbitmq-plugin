(defproject riemann-rabbitmq-plugin "0.1.0-SNAPSHOT"
  :description "Cool new project to do things and stuff"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.novemberain/langohr "3.0.0-rc2"]
                 ]
  :plugins [[codox "0.6.1"]
            [lein-midje "3.0.0"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]
                                  [riemann "0.2.8"]
                                  ]}}
  :codox {:src-linenum-anchor-prefix "L"
  :src-dir-uri "https://github.com/avishai-ish-shalom/riemann-rabbitmq-plugin/blob/master"})
