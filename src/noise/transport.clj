(ns noise.transport
  (:require [clojure.java.io :as io]
            [noise.bencode :as be])
  (:refer-clojure :exclude (send))
  (:import (java.io InputStream OutputStream PushbackInputStream
                    PushbackReader IOException EOFException)
           (java.net Socket SocketException)
           (java.util.concurrent SynchronousQueue LinkedBlockingQueue
                                 BlockingQueue TimeUnit)
           clojure.lang.RT))

(defprotocol Transport
  (recv [this] [this timeout])
  (send [this msg]))

(deftype FnTransport [recv-fn send-fn close]
  Transport
  (send [this msg] (send-fn msg) this)
  (recv [this] (.recv this Long/MAX_VALUE))
  (recv [this timeout] (recv-fn timeout))
  
  java.io.Closeable
  (close [this] (close)))

(defn fn-transport
  ([read write] (fn-transport read write nil))
  ([read write close]
     (let [read-queue (SynchronousQueue.)]
       (future (try (while true
                      (.put read-queue (read)))
                    (catch Throwable t
                      (.put read-queue t))))
       (FnTransport.
        (let [failure (atom nil)]
          #(if @failure
             (throw @failure)
             (let [msg (.poll read-queue % TimeUnit/MILLISECONDS)]
               (if (instance? Throwable msg)
                 (do (reset! failure msg)
                     (throw msg))
                 msg))))
        write
        close))))

(defmulti ^:private <bytes class)

(defmethod <bytes :default
  [input]
  input)

(defmethod <bytes (RT/classForName "[B")
  [#^"[B" input]
  (String. input "UTF-8"))

(defmethod <bytes clojure.lang.IPersistentVector
  [input]
  (vec (map <bytes input)))

(defmethod <bytes clojure.lang.IPersistentMap
  [input]
  (->> input
       (map (fn [[k v]] [k (<bytes v)]))
       (into {})))

(defmacro ^:private rethrow-on-disconnection
  [^Socket s & body]
  `(try ~@body
        (catch EOFException e#
          (throw (SocketException. "EOF Exception")))
        (catch Throwable e#
          (if (and ~s (not (.isConnected ~s)))
            (throw (SocketException. "The server isn't connected"))
            (throw e#)))))

(defn bencode
  ([^Socket s] (bencode s s s))
  ([in out & [^Socket s]]
     (let [in (PushbackInputStream. (io/input-stream in))
           out (io/output-stream out)]
       (fn-transport
        #(let [payload (rethrow-on-disconnection s (be/read-bencode in))
               unencoded (<bytes 
                          (payload "-unencoded"))
               to-decode (apply dissoc payload "-unencoded" unencoded)]
           (merge (dissoc payload "-unencoded")
                  (when unencoded {"-unencoded" unencoded})
                  (<bytes to-decode)))
        #(rethrow-on-disconnection s
                                   (locking out
                                     (doto out
                                       (be/write-bencode %)
                                       .flush)))
        (fn []
          (.close in)
          (.close out)
          (when s (.close s)))))))