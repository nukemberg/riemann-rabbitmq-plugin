(ns riemann-rabbitmq-plugin.core-test
  (:use midje.sweet
        [midje.util :only [expose-testables]])
  (:require [riemann-rabbitmq-plugin.core :refer :all]
            [riemann.service    :as service]
            [riemann.config :refer [service!]]
            [riemann.core       :as core]
            [riemann.logging    :as logging]
            [riemann.time       :refer [unix-time]]
            [langohr.core       :as rmq]
            [langohr.channel    :as lch]
            [langohr.queue      :as lq]
            [langohr.consumers  :as lc]
            [langohr.basic      :as lb])
  (:import [org.apache.log4j Level]))

(expose-testables riemann-rabbitmq-plugin.core)
(logging/init :console true)
(logging/set-level Level/DEBUG)

(comment
"Unfortunately, midje currently has issues with primitive type hints -
see https://groups.google.com/forum/#!topic/midje/xPN-eO_0poI or https://github.com/marick/Midje/issues/295 for more details.

We can work around this using `with-redefs` but that means we have no way to assert (withing midje) that calls to the stubbed function were made.
So, TODO: refactor the code to make it easier for testing or move to core.testing instead of midje"
)


(facts "about `AMQPInput` record"
       (let [prefetch-count (long (* (rand) 10000))
             bindings [{:queue "" :bind-to {..exchange.. [..binding-key..]}}
                       {:queue ..queue1.. :bind-to {..exchange2.. [..binding-key2.. ..binding-key3..]}}]
             amqp-input (->AMQPInput {:connection-opts ..connection-opts.. :bindings bindings :prefetch-count prefetch-count} (atom nil) (atom nil))]
         (fact "`start!` binds multiple queues with multiple bindings"
               (with-redefs [lb/qos (fn [ch ^long prefetch-count] nil)]
                 (service/start! amqp-input) => irrelevant
                 (provided
                  (rmq/connect ..connection-opts..) => ..conn..
                  (lch/open ..conn..) => ..ch..
                  (lq/declare ..ch.. "" {:exclusive true :auto-delete true}) => {:queue ..queue..}
                  (lq/declare ..ch.. ..queue1.. {:exclusive true :auto-delete true}) => {:queue ..queue1..}
                  (lq/bind ..ch.. ..queue.. ..exchange.. {:routing-key ..binding-key..}) => nil
                  (lq/bind ..ch.. ..queue1.. ..exchange2.. {:routing-key ..binding-key2..}) => nil
                  (lq/bind ..ch.. ..queue1.. ..exchange2.. {:routing-key ..binding-key3..}) => nil
                  (lc/subscribe ..ch.. ..queue.. anything) => nil
                  (lc/subscribe ..ch.. ..queue1.. anything) => nil)))))

(facts "about `message-handler`"
       (let [ack-called (atom false)
             payload (byte-array (map int "test"))
             event {:time 12345678 :host nil :service nil :tags []}
             delivery-tag (rand-int 100000)]
         (fact "calls core/stream! when event is correctly parsed"
               (with-redefs [lb/ack (fn [ch ^long delivery-tag] (reset! ack-called delivery-tag) nil)]
                 (message-handler --parser-fn-- ..tags.. (atom ..core..) ..ch.. ..props.. payload) => nil
                 (provided
                  ..props.. =contains=> {:delivery-tag delivery-tag}
                  ;(lb/ack ..ch.. ..delivery-tag..) => nil
                  (--parser-fn-- payload ..props..) => event
                  (core/stream! ..core.. event) => nil)))
         (fact "when event isn't correctly parsed, don't call `core/stream!` and reject the message"
               (with-redefs [lb/reject (fn [ch ^long delivery-tag requeue] nil)]
                 (message-handler --parser-fn-- ..tags.. (atom ..core..) ..ch.. ..props.. payload) => nil
                 (provided
                   ..props.. =contains=> {:delivery-tag delivery-tag}
                   (--parser-fn-- payload ..props..) => nil)))
         (fact "adds tags to event"
               (let [tagged-event (assoc event :tags [..tag..])]
                 (with-redefs [lb/ack (fn [ch ^long delivery-tag] nil)]
                   (message-handler --parser-fn-- [..tag..] (atom ..core..) ..ch.. ..props.. payload) => nil
                   (provided
                    (core/stream! ..core.. tagged-event) => nil
                    ..props.. =contains=> {:delivery-tag delivery-tag}
                    (--parser-fn-- payload ..props..) => event))))
               ))

(fact "`amqp-consumer` pre checks are satisfied by the example"
       (amqp-consumer {
                       :parser-fn #(String. %)
                       :prefetch-count 100
                       :bindings [{
                                   :opts {:durable false :auto-delete true}
                                   :queue ""
                                   :bind-to {"exchange", ["binding-key"]}
                                   }]
                       :connection-opts {:host "rabbitmq-host" :port 5672 :username "guest" :passowrd "guest"}
                       }
                      ) => nil
         (provided
           (service! (checker [rec] (satisfies? service/Service rec))) => nil))

(facts "about `parse-message`"
       (let [payload (byte-array (map int "test"))]
         (fact "returns nil if parser-fn return a non associate object"
               (parse-message (fn [_ _] "dddd") payload ..metadata..) => nil
               (parse-message (fn [_ _] 23123) payload ..metadata..) => nil)
         (fact "returns nil when parser-fn throws an exception"
               (parse-message (fn [_ _] (throw (Exception. "test exception"))) payload ..metadata..) => nil)
         (fact "auto fill current time if event doesn't contain :time"
               (parse-message (fn [_ _] {:host nil}) payload ..metadata..) => {:host nil :time ..time..}
               (provided (unix-time) => ..time..))))

(facts "about `logstash-parser`"
       (fact "parses message"
             (let [msg (byte-array (map int "{ \"tags\": [], \"type\": \"logstash\", \"@version\": \"1\", \"@timestamp\": \"2015-02-05T08:19:22.152Z\", \"source_host\": \"some-host\", \"message\": \"whatever\" }"))]
               (logstash-parser msg ..metadata..)) => (contains {:tags []
                                                                  :time 1423124362
                                                                  :source_host "some-host"
                                                                  :message "whatever"
                                                                  (keyword "@version") "1"
                                                                  :type "logstash"}))
       (fact "parses message with bad tags"
             (let [msg (byte-array (map int "{ \"tags\": \"bad-tag\", \"type\": \"logstash\", \"@version\": \"1\", \"@timestamp\": \"2015-02-05T08:19:22.152Z\", \"source_host\": \"some-host\", \"message\": \"whatever\" }"))]
               (logstash-parser msg ..metadata..) => (contains {:tags ["bad-tag"]})))
       (fact "when no timestamp is given use currrent time"
             (let [msg (byte-array (map int "{ \"tags\": [], \"type\": \"logstash\", \"@version\": \"1\", \"source_host\": \"some-host\", \"message\": \"whatever\" }"))]
               (logstash-parser msg ..metadata..) => (contains {:time ..time..})
               (provided
                (unix-time) => ..time..))))

(facts "about `logstash-v0-parser`"
       (fact "parses message correctly and fixes fields"
             (let [msg (byte-array (map int "{ \"@tags\": [], \"@type\": \"logstash\", \"@timestamp\": \"2015-02-05T08:19:22.152Z\", \"@source_host\": \"some-host\", \"@message\": \"whatever\", \"@fields\": {\"some-field\": \"some-value\"}}"))]
               (logstash-v0-parser msg ..metadata..) => (contains {:type "logstash" :host "some-host" :message "whatever" :some-field "some-value"})))
       (fact "When @fields is empty, behave"
             (let [msg (byte-array (map int "{ \"@tags\": [], \"@type\": \"logstash\", \"@timestamp\": \"2015-02-05T08:19:22.152Z\", \"@source_host\": \"some-host\", \"@message\": \"whatever\" }"))]
               (logstash-v0-parser msg ..metadata..) => (contains {:type "logstash" :host "some-host" :message "whatever"})))
       (fact "when no timestamp is given use currrent time"
             (let [msg (byte-array (map int "{ \"@tags\": [], \"@type\": \"logstash\", \"@source_host\": \"some-host\", \"@message\": \"whatever\" }"))]
               (logstash-parser msg ..metadata..) => (contains {:time ..time..})
               (provided
                (unix-time) => ..time..))))
