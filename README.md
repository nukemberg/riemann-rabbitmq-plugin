# riemann-rabbitmq-plugin

A riemann RabbitMQ plugin. Usefull to listen to messages on an AMQP transport (e.g. graphite or logging events)

## Usage

In your riemann.config

```clojure

(load-plugins) ; will load plugins from the classpath

(rabbitmq-plugin/rabbitmq-consumer {
                       :exchange "messages-exchange" ; this is a mandatory parameter
                       :parser-fn #(json/parse-string (String. %) true) ; message parsing function, the sample function here is the default
                       :prefetch-count 100 ; this is the default
                       :queue-name "" ; the default is "" which means auto-generated queue name
                       :queue-opts {:durable false :auto-delete true} ; this is the default
                       :connection-opts {:host "rabbitmq-host" :port 5672 :username "guest" :passowrd "guest"} ; default is {}
                       })

```

## Installing

You will need to build this module for now and push it on riemann's classpath, for this
you will need a working JDK, JRE and [leiningen](http://leiningen.org).

First build the project:

```
lein uberjar
```

The resulting artifact will be in `target/riemann-rabbitmq-input-standalone-0.0.1.jar`.
You will need to push that jar on the machine(s) where riemann runs, for instance, in
`/usr/lib/riemann/riemann-rabbitmq-input.jar`.

If you have installed riemann from a stock package you will only need to tweak
`/etc/default/riemann` and change
the line `EXTRA_CLASSPATH` to read:

```
EXTRA_CLASSPATH=/usr/lib/riemann/riemann-rabbitmq-input.jar
```

You can then use exposed functions, provided you have loaded the plugin in your configuration.

## License

Copyright © 2014 Avishai Ish-Shalom

Distributed under the Apache V2 License
