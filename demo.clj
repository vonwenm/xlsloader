(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]))

(def conn (cc/connect ["127.0.0.1"]))

(cql/drop-keyspace conn "new_cql_keyspace")
                                                    
(cql/create-keyspace conn "new_cql_keyspace"
                                           (with {:replication
                                                  {:class "SimpleStrategy"
                                                 :replication_factor 1 }}))
                                                 
(cql/use-keyspace conn "new_cql_keyspace")                                    
                                                 
(cql/drop-table conn "users" )

(cql/create-table conn "users"
                (column-definitions {:name :varchar
                                     :age  :int
                                     :primary-key [:name]}))
    
(cql/drop-index conn :users_age )

(cql/create-index conn "users" :age 
		(index-name :users_age) 
                 (if-not-exists))
 
                                                                   
(cql/insert conn "users" {:name "Alex" :age (int 19)})
(cql/insert conn "users" {:name "Alexa" :age (int 29)})
(cql/insert conn "users" {:name "Andy" :age (int 28)})
(cql/insert conn "users" {:name "Robert" :age (int 28)})

(cql/select conn "users")

(cql/select conn "users" (where :name "Alex"))

(cql/select conn "users" (where :name [:in ["Alex" "Robert"]]))

(cql/select conn "users"
          (columns :name)
          (order-by [:name :desc]))
          
(cql/select conn "users"
          (where :age [> 19 ]))
          
 
(cql/select conn "users"
          (columns :name))
          
(cql/select conn "users" (limit 1))


;;===============================

(cql/drop-table conn "feed_event" )

(cql/create-table conn "feed_event"
                (column-definitions {:feedid :varchar
                                     :postid  :int
                                     :content :varchar
                                     :primary-key [:userid :postid]}))


;;===============================

(cql/drop-table conn "entity_delta" )

(cql/create-table conn "entity_delta"
                (column-definitions {	:feedid  :int
                			:refdom  :varchar
					:refid   :varchar
					:vtbl  	 :varchar
					:vcol    :varchar
                                     	:content :varchar
                                     	:primary-key [ (:feedid :refdom :refid) :vtbl :vcol ]}))
                                     	
                                     	
(cql/insert conn "entity_delta" {	:feedid (int 1)   
                			:refdom  "ISIN"
					:refid   "GB1234567890"
					:vtbl  	 "I"
					:vcol    "MAT-DATE"
                                     	:content "12/12/2022"})
                                     	
(cql/insert conn "entity_delta" {	:feedid (int 1)   
                			:refdom  "ISIN"
					:refid   "GB1234567890"
					:vtbl  	 "IDN"
					:vcol    "SCHEME"
                                     	:content "SHORT-NAME"})	
                                     	
(cql/insert conn "entity_delta" {	:feedid (int 1)   
                			:refdom  "ISIN"
					:refid   "GB1234567890"
					:vtbl  	 "IDN"
					:vcol    "VALUE"
                                     	:content "BARCLAYS ORD SHARES"})	
                                     	
                                     
(cql/select conn "entity_delta")                                    
                                   
(cql/select conn "entity_delta"
          (where :feedid  (int 1) :refdom  "ISIN"  :refid   "GB1234567890" ))
          
(cql/select conn "entity_delta"
          (where :feedid  (int 1) :refdom  "ISIN"  :refid   "GB1234567890"  :vtbl  	 "IDN" ))
 
 (cql/select conn "entity_delta"
 	  (columns :vcol :content)
          (where :feedid  (int 1) :refdom  "ISIN"  :refid   "GB1234567890"  :vtbl  	 "IDN" ))
          
          

DROP TABLE "entity_delta" ;

TRUNCATE "entity_delta" ;

CREATE TABLE entity_delta (             feedid  	bigint,
                			refdom  	varchar,
					refid   	varchar,
					vtbl  	 	varchar,
					vcol    	varchar,
                                     	content 	varchar,
  PRIMARY KEY ((feedid, refdom, refid), vtbl, vcol)
);                                  	
 
SELECT * FROM "entity_delta" ;

SELECT * FROM "entity_delta" WHERE  feedid =  1 ;    


SELECT * FROM "entity_delta" WHERE feedid = 1 and refdom = 'ISIN' and  refid = 'GB1234567890' ;     