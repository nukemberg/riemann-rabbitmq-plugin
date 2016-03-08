(ns riemann-rabbitmq-plugin.publisher
  (:require
   [langohr.core       :as rmq]
   [langohr.channel    :as lch]
   [langohr.basic      :as lb]
   [riemann.pool       :refer [with-pool fixed-pool]]))

(def default-opts {:pool-size 1 :block-start true :regenerate-interval 5 :connection-opts {}})

(defprotocol ClientPool
  (open [this])
  (close [this])
  (publish [this exchange routing-key message opts] [this exchange routing-key message]))

(defrecord RabbitMQPublisher [connection-opts]
  ClientPool
  (open [this]
    (let [conn (rmq/connect connection-opts)
          ch (lch/open conn)]
      (assoc this :conn conn :ch ch)))
  (close [{:keys [ch conn] :as this}]
    (when (lch/open? ch) (lch/close ch))
    (when (rmq/open? conn) (rmq/close conn)))
  (publish [{ch :ch} exchange routing-key message opts]
    (lb/publish ch exchange routing-key message opts))
  (publish [this exchange routing-key message]
    (publish this exchange routing-key message {:mandatory false :immediate false})))

(defn get-pool [opts]
  "The opts map can contain the following items:
   :pool-size passed to riemann.pool/fixed-pool as :size
   :block-start passed to riemann.pool/fixed-pool
   :regenerate-interval passed to riemann.pool/fixed-pool
   :connection-opts passed to langohr.core/connect.
  "
  (let [opts (merge default-opts opts)]
    (fixed-pool
      (fn rabbitmq-publisher-open []
        (open (RabbitMQPublisher. (:connection-opts opts))))
      (fn rabbitmq-publisher-close [publisher]
        (close publisher))
      {:size                 (:pool-size opts)
       :block-start          (:block-start opts)
       :regenerate-interval  (:regenerate-interval opts)})))
