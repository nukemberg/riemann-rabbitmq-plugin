(ns riemann-rabbitmq-plugin.core
  (:import [java.util.concurrent Executors])
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
   ))

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
	"Create an AMQP consumer instance"
	[opts]
  {:pre [(every? opts mandatory-opts)]}
  (service! (AMQInput. (merge default-opts opts) (atom nil) (atom nil))))
