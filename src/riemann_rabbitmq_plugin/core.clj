(ns riemann-rabbitmq-plugin.core
  (:require
   [langohr.core                      :as rmq]
   [langohr.channel                   :as lch]
   [langohr.queue                     :as lq]
   [langohr.consumers                 :as lc]
   [langohr.basic                     :as lb]
   [riemann.core                      :as core]
   [riemann.service                   :refer [Service ServiceEquiv]]
   [riemann.time                      :refer [unix-time]]
   [riemann.common                    :refer [iso8601->unix]]
   [clojure.tools.logging             :refer [warn error info infof debug]]
   [clojure.string                    :as string]
   [clojure.set                       :refer [rename-keys]]
   [cheshire.core                     :as json]
   [riemann.config                    :refer [service!]]
   [riemann.pool                      :refer [with-pool]]
   [riemann-rabbitmq-plugin.publisher :as publisher]))


(defn- parse-json [p] (json/parse-string p true))

(defn- fix-time [msg]
   (if-let [time (get msg (keyword "@timestamp"))]
     (assoc msg :time (iso8601->unix time))
     (assoc msg :time (unix-time))))

(defn- ensure-tag-vec [event]
  (if (sequential? (:tags event))
    event
    (assoc event :tags (remove empty? [(str (:tags event))]))))

(defn- fields-to-attributes [event]
  (let [fields (get event (keyword "@fields") {})]
    (reduce (fn [e [k v]] (assoc e k v))
            (dissoc event (keyword "@fields"))
            fields)))

(defn- v0-to-v1 [event]
  (rename-keys event {(keyword "@message") :message,
                      (keyword "@tags") :tags,
                      (keyword "@type") :type,
                      (keyword "@source_host") :host,
                      (keyword "@source_path") :path}))

(defn logstash-parser
  "A parser function for the Logstash v1 format. Recieves a byte array of json encoded data and returns a map suitable for use with riemann"
  [payload metadata]
  (-> payload
      String.
      parse-json
      fix-time
      ensure-tag-vec))

(defn logstash-v0-parser
  "A parser function for the Logstash v0 format. Recieves a byte array of json encoded data and returns a map suitable for use with riemann"
  [payload metadata]
  (-> payload
      String.
      parse-json
      fix-time
      ensure-tag-vec
      fields-to-attributes
      v0-to-v1))

(def mandatory-opts [:bindings])
(def default-opts {:prefetch-count 100 :connection-opts {}
                   :parser-fn logstash-parser
                   :tags []})

(defn- ^{:testable true} parse-message
  "Safely run the parser function and verify the resulting event"
  [parser-fn message metadata]
  (debug "Parsing message with parser function; msg:" (String. message))
  (try
    (let [event (parser-fn message metadata)]
      (if (and (instance? clojure.lang.Associative event) (every? keyword? (keys event)))
        (if (number? (:time event)) event
            (assoc event :time (unix-time)))
        (do
          (warn "Check yer parser, message not parsed to a proper map like object. Dropping" event)
          nil)))
    (catch Exception e
      (warn e "Failed to parse message"))))


(defn- ^{:testable true} message-handler
  "AMQP Consumer message handler. Will ack messages after submitting to riemann core and reject unparsable messages"
  [parser-fn tags core ch {delivery-tag :delivery-tag :as props} payload]
  (let [event (parse-message parser-fn payload props)]
        (if event
          (do
            (debug "Submitting event to Riemann core" event)
            (core/stream! @core
                          (update-in event [:tags] (comp concat) tags))
            (lb/ack ch delivery-tag))
          (do ;else
            (warn "Invalid event, rejecting")
            (lb/reject ch delivery-tag false))
          )))

(defrecord AMQPInput [opts core killer]
  ServiceEquiv
  (equiv? [this {other-opts :opts}]
    (= opts other-opts))
  Service
  (conflict? [this other] false)
  (reload! [this new-core]
    (reset! core new-core))
  (start! [this]
    (locking this
      (when-not @killer
        (debug "Openning new RabbitMQ connection")
        (let [{:keys [parser-fn bindings exchange connection-opts prefetch-count tags]
               :as opts
              } opts
              conn (rmq/connect connection-opts)
              ch (lch/open conn)
             ]
          (lb/qos ch prefetch-count)
          (doseq [binding-spec bindings]
            (let [queue-name (:queue (lq/declare ch (get binding-spec :queue "") (get binding-spec :opts {:auto-delete true :exclusive true})))]
              (doseq [[exchange binding-keys] (:bind-to binding-spec)
                      binding-key (if (seq binding-keys) binding-keys [binding-keys])]
                (infof "binding queue %s to exchange %s with key %s" queue-name exchange binding-key)
                (lq/bind ch queue-name exchange {:routing-key binding-key}))
              (info "Starting RabbitMQ consumer thread")
              (lc/subscribe ch queue-name (partial message-handler parser-fn tags core))))
          (reset! killer (fn []
                           (try
                             (when (lch/open? ch) (lch/close ch))
                             (when (rmq/open? conn) (rmq/close conn))
                             (catch Exception e
                               (error e "Exception while trying to close RabbitMQ connection")))))))))
  (stop! [this]
         (when (fn? (@killer))
           (locking this
             (@killer)
             (reset! killer nil)))))

(defn amqp-consumer
  "Create an AMQP consumer instance. Usage:
  (amqp-consumer {:parser-fn logstash-parser :connection-opts {:host \"rabbitmq.example.com\" :port 5672} :prefetch-count 100 :bindings [{:queue \"riemann\" :bind-to {\"logs-exchange\" [\"#\"] :opts {:durable false}}]})

  Options:

  :connection-opts Langhor connection options, see langohr.core/connect for more info
  :bindings a list/vector of binding specs
  :parser-fn A function to parse raw messages to clojure maps with valid keys. function signature is (parser-fn [^bytes message-payload ^IPersistentMap message-metadata]).
     message-payload is byte array of the message, message-metadata is a map of message and delivery metadata - see langohr docs (http://clojurerabbitmq.info/articles/queues.html)
     Defaults to `logstash-parser`
  :prefetch-count - the number of messages to prefetch
  :tags - tags to add to a message

  binding specs: a map with binding specifications:
  :queue - the queue name to use. if empty an auto-generated queue name will be used
  :opts - options to use when declaring the queue. See langohr docs for details.  e.g. {:auto-delete false, :durable true, :exclusive false}
  :bind-to - a map of exchange -> [binding-keys] pairs. E.g. {\"exchange-name\" [\"logs.#\"]}
"
	[opts]
  {:pre [(every? opts mandatory-opts)
         (sequential? (:bindings opts))
         (every? map? (map :bind-to (:bindings opts)))]}
  (service! (AMQPInput. (merge default-opts opts) (atom nil) (atom nil))))

(defn amqp-publisher
  "Create an AMQP publisher stream.

(let [amqp (amqp-publisher {:exchange \"events\" :routing-key #(str (:host %) \".\" (:service %)) :encoding-fn cheshire.core/generate-string :message-opts {:persistent false}))]
  (streams
...
    amqp))

options:

:exchange - the exchange to publish to
:routing-key - the routing key to use when publishing. Can be a function or a string
:encoding-fn - a function which will be used to encode the output. encoding-fn recieves a single map object (a riemann event map) and must return an encoded byte array.
"
  [{:keys [exchange routing-key encoding-fn message-opts] :as opts}]
  {:pre [(every? opts [:exchange :routing-key :encoding-fn])
         (fn? encoding-fn)]}
  (let [pool (publisher/get-pool (:pool-opts opts))]
    (fn [event]
      (with-pool [publisher-client pool (get opts :claim-timeout 5)]
        (let [routing-key (if (fn? routing-key) (routing-key event) routing-key)]
          (publisher/publish publisher-client exchange routing-key (encoding-fn event) message-opts))))))
