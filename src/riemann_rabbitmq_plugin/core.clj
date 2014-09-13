(ns riemann-rabbitmq-plugin.core
  (:require
   [langohr.core       :as rmq]
   [langohr.channel    :as lch]
   [langohr.queue      :as lq]
   [langohr.consumers  :as lc]
   [langohr.basic      :as lb]
   [riemann.core       :as core]
   [riemann.service    :refer [Service ServiceEquiv]]
   [clojure.tools.logging :refer [warn error info infof debug]]
   [clojure.string     :as string]
   [cheshire.core      :as json]
   [riemann.config     :refer [service!]]
   [riemann.pool       :refer [with-pool]]
   [riemann-rabbitmq-plugin.publisher :as publisher]))

(def mandatory-opts [:exchange])
(def default-opts {:prefetch-count 100 :queue-name "" :binding-keys ["#"] :connection-opts {}
                   :parser-fn #(json/parse-string (String. %) true)})

(defn- parse-message
  "Safely run the parser function and verify the resulting event"
  [parser-fn message]
  (debug "Parsing message with parser function" (String. message))
  (try
    (let [event (parser-fn message)]
      (if (and (instance? clojure.lang.Associative event) (every? keyword? (keys event)))
        event
        (do
          (warn "Check yer parser, message not parsed to a proper map like object. Dropping" event)
          nil)))
    (catch Exception e
      (warn e "Failed to parse message"))))


(defn- message-handler
  "AMQP Consumer message handler. Will ack messages after submitting to riemann core and reject unparsable messages"
  [parser-fn core ^com.rabbitmq.client.Channel ch {delivery-tag :delivery-tag :as props} ^bytes payload]
  (let [event (parse-message parser-fn payload)]
        (if event
          (do
            (debug "Submitting event to Riemann core" event)
            (core/stream! @core event)
            (lb/ack ch delivery-tag))
          (do ;else
            (warn "Invalid event, rejecting")
            (lb/reject ch delivery-tag false))
          )))

(defrecord AMQInput [opts core killer]
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
        (let [{:keys [parser-fn queue-name exchange queue-opts connection-opts prefetch-count binding-keys]
               :as opts
              } opts
              conn (rmq/connect connection-opts)
              ch (lch/open conn)
              {:keys [queue]} (lq/declare ch queue-name queue-opts)
             ]
          (lb/qos ch prefetch-count)
          (doseq [binding-key binding-keys]
            (infof "binding queue %s to exchange %s with key %s" queue exchange binding-key)
            (lq/bind ch queue-name exchange {:routing-key binding-key}))
          (info "Starting RabbitMQ consumer thread")
          (lc/subscribe ch queue (partial message-handler parser-fn core))
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
  (amqp-consumer {:parser-fn #(cheshire.core/parse-string (String. %) true) :connection-opts {:host \"rabbitmq.example.com\" :port 5672} :queue-name \"some-q\" :queue-opts {:durable true :auto-delete false} :exchange \"events\" :prefetch-count 100 :binding-keys [\"#\"]})

  Options:

  :connection-opts Langhor connection options, see langohr.core/connect

  :queue-name The queue to declare and consume from. Empty string means auto generated queue name (anonymous queue)

  :queue-opts Queue options, see langohr.queue/declare

  :exchange AMQP exchange to bind to

  :binding-keys routing keys to use when binding the queue to the exchange

  :parser-fn A function to parse raw messages to clojure maps with valid keys. function signature is (parser-fn [^bytes message])"
	[opts]
  {:pre [(every? opts mandatory-opts)]}
  (service! (AMQInput. (merge default-opts opts) (atom nil) (atom nil))))

(defn amqp-publisher [{:keys [exchange routing-key encoding-fn message-opts] :as opts}]
  {:pre [(every? opts [:exchange :routing-key :encoding-fn])
         (fn? encoding-fn)]}
  (let [pool (publisher/get-pool (:pool-opts opts))]
    (fn [event]
      (with-pool [publisher-client pool (get opts :claim-timeout 5)]
        (let [routing-key (if (fn? routing-key (routing-key event) routing-key))]
          (publisher/publish publisher-client exchange routing-key (encoding-fn event) message-opts))))))
