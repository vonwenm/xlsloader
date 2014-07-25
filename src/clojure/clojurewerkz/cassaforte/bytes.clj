;; Copyright (c) 2012-2014 Michael S. Klishin, Alex Petrov, and the ClojureWerkz Team
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns clojurewerkz.cassaforte.bytes
  "Facility functions to use with serialization, handle deserialization of all the data types
   supported by Cassandra."
  (:import java.nio.ByteBuffer java.util.Date
           [com.datastax.driver.core DataType DataType$Name]))

(declare serializers)

(defn #^bytes to-bytes
  [^ByteBuffer byte-buffer]
  (let [bytes (byte-array (.remaining byte-buffer))]
    (.get byte-buffer bytes 0 (count bytes))
    bytes))

(def ^:private deserializers
  {DataType$Name/ASCII     (DataType/ascii)
   DataType$Name/BIGINT    (DataType/bigint)
   DataType$Name/BLOB      (DataType/blob)
   DataType$Name/BOOLEAN   (DataType/cboolean)
   DataType$Name/COUNTER   (DataType/counter)
   DataType$Name/DECIMAL   (DataType/decimal)
   DataType$Name/DOUBLE    (DataType/cdouble)
   DataType$Name/FLOAT     (DataType/cfloat)
   DataType$Name/INET      (DataType/inet)
   DataType$Name/INT       (DataType/cint)
   DataType$Name/TEXT      (DataType/text)
   DataType$Name/TIMESTAMP (DataType/timestamp)
   DataType$Name/UUID      (DataType/uuid)
   DataType$Name/VARCHAR   (DataType/varchar)
   DataType$Name/VARINT    (DataType/varint)
   DataType$Name/TIMEUUID  (DataType/timeuuid)})

(defn get-deserializer
  [^DataType t]
  (let [type-name (.getName t)]
    (if-let [deserializer (get deserializers type-name)]
      deserializer
      (cond
       (= type-name (DataType$Name/LIST)) (DataType/list (get-deserializer (-> t (.getTypeArguments) (.get 0))))
       (= type-name (DataType$Name/SET))  (DataType/set (get-deserializer (-> t (.getTypeArguments) (.get 0))))
       (= type-name (DataType$Name/MAP))  (DataType/map (get-deserializer (-> t (.getTypeArguments) (.get 0)))
                                                        (get-deserializer (-> t (.getTypeArguments) (.get 1))))
       :else                              (throw (Exception. (str "Can't find matching deserializer for: " t)))))))

(defn deserialize
  [^DataType dt bytes]
  (.deserialize (get-deserializer dt) bytes))
