(ns noise.core
  (:require [noise.transport :as t]
            [noise.server :as s]
            [clojure.walk :as walk])
  (:import [java.net Socket]))

(defn server 
  [& opts]
  (apply s/start-server opts))

(defn client 
  [& {:keys [port bind transport-fn] :or {port 0 transport-fn t/bencode}}]
  (transport-fn (Socket. bind port)))

(defn transmit [client data]
  (let [recv (fn [timeout trans] (t/recv trans timeout))]
    (try (->> data
              walk/stringify-keys
              (t/send client)
              (recv 1000)
              walk/keywordize-keys))))
