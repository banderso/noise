(ns noise.server
  (:require [noise.transport :as t])
  (:import [java.net Socket ServerSocket InetSocketAddress]))

(defn- handle*
  [msg handler transport]
  (try (handler (assoc msg :transport transport))
       (catch Throwable t
         (binding [*out* *err*]
           (println (str "Unhandled handler exception processing message '" 
                         msg
                         "' "
                         t))))))

(defn- handle
  [handler transport]
  (let [msg (t/recv transport)]
    (when msg
      (future (handle* msg handler transport))
      (recur handler transport))))

(defn- accept-connection
  [{:keys [^ServerSocket socket-server open-transports transport handler]
    :as server}]
  (when-not (.isClosed socket-server)
    (let [sock (.accept socket-server)]
      (future (let [transport (transport sock)]
                (try (swap! open-transports conj transport)
                     (handle handler transport)
                     (catch Throwable t
                       (binding [*out* *err*]
                         (println (str "Caught unhandled error: " t))))
                     (finally (swap! open-transports disj transport)
                              (.close transport)))))
      (future (accept-connection server)))))

(defn- safe-close
  [^java.io.Closeable x]
  (try (.close x)
       (catch java.io.IOException e
         (binding [*out* *err*]
           (println (str "Failed to close " e))))))

(defn stop-server 
  [{:keys [open-transports ^ServerSocket socket-server] :as server}]
  (.close socket-server)
  (swap! open-transports
         #(reduce (fn [s t] (if (instance? java.io.Closeable t)
                              (do (safe-close t)
                                  (disj s t))
                              s))
                  % %)))

(defn default-handler
  [& additional-middlewares]
  (fn [msg]
    (let [transport (:transport msg)
          msg (dissoc msg :transport)]
      (t/send transport msg))))

(defrecord Server [socket-server port open-transports transport handler]
  java.io.Closeable
  (close [this] (stop-server this)))

(defn start-server
  [& {:keys [port bind transport-fn handler] :or {port 0}}]
  (let [bind-addr (if bind 
                    (InetSocketAddress. bind port) 
                    (InetSocketAddress. port))
        ss (ServerSocket. port 0 (.getAddress bind-addr))
        server (assoc (Server. ss
                               (.getLocalPort ss)
                               (atom #{})
                               (or transport-fn t/bencode)
                               (or handler (default-handler)))
                 :ss ss)]
    (future (accept-connection server))
    server))