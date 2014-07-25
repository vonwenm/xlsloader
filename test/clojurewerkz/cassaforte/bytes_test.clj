(ns clojurewerkz.cassaforte.bytes-test
  (:require [clojurewerkz.cassaforte.bytes :refer :all]
            [clojure.test :refer :all])
  (:import java.nio.ByteBuffer))
(comment
  (deftest t-serializer-roundtrip
    (are [type value]
         (= value (deserialize type (to-bytes (encode value))))
         "Int32Type" (Integer. 1)
         "FloatType" (float 1)
         "DecimalType" 1.2M
         "IntegerType" (java.math.BigInteger. "123456789")
         "LongType" (Long. 100)
         "UTF8Type" "some fancy string"
         "AsciiType" "some fancy string"
         "BooleanType" false
         "BooleanType" true
         "org.apache.cassandra.db.marshal.DateType" (java.util.Date.)
         "DoubleType" (java.lang.Double. "123")
         "ListType(UTF8Type)" ["a" "b" "c"]
         "MapType(UTF8Type,UTF8Type)" {"a" "b"}
         "CompositeType(UTF8Type,UTF8Type,UTF8Type)" (composite "a" "b" "c"))))
