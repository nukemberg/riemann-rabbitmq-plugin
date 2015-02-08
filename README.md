# riemann-rabbitmq-plugin

[![Build Status](https://travis-ci.org/avishai-ish-shalom/riemann-rabbitmq-plugin.svg?branch=master)](https://travis-ci.org/avishai-ish-shalom/riemann-rabbitmq-plugin)

A riemann RabbitMQ plugin. Usefull to listen to messages on an AMQP transport (e.g. graphite or logging events)

## Usage

In your riemann.config

```clojure

(load-plugins) ; will load plugins from the classpath

(rabbitmq-plugin/rabbitmq-consumer {
                       :parser-fn rabbitmq-plugin/logstash-parser ; message parsing function, the sample function here is the default
                       :prefetch-count 100 ; this is the default
                       :bindings [{
                         :opts {:durable false :auto-delete true} ; this is the default
                         :queue "" ; the default is "" which means auto-generated queue name
                         :bind-to {"exchange", ["binding-key"]} ; also works with single non-seq binding key
                         :tags ["amqp"] ; will be added to event tags
                       }]
                       :connection-opts {:host "rabbitmq-host" :port 5672 :username "guest" :passowrd "guest"} ; default is {}
                       })

```

The `logstash-parser` function will parse a logstash v1 or v0 formatted messages as riemann events. Note that :service or :host will not be extracted automatically if the field names in the message are different.

See the [API docs](http://avishai-ish-shalom.github.io/riemann-rabbitmq-plugin) for more info.

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
