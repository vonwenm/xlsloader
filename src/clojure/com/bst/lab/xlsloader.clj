(ns com.bst.lab.xlsloader
  "Main namespace for working with XLSLOADER."
  (:require [clojurewerkz.cassaforte.cql    :as cql]
  	    [qbits.hayt.cql 		    :as hayt]
            [clojurewerkz.cassaforte.query  :as q]
            [clojurewerkz.cassaforte.client :as cc]
            [dk.ative.docjure.spreadsheet   :as as])
  (:import 	(com.datastax.driver.core.Session)
   		(java.io FileOutputStream FileInputStream)
   		(java.util Date Calendar)
   		(org.apache.poi.xssf.usermodel XSSFWorkbook)
   		(org.apache.poi.ss.usermodel Workbook Sheet Cell Row
                                WorkbookFactory DateUtil
                                IndexedColors CellStyle Font
                                CellValue)
   		(org.apache.poi.ss.util CellReference AreaReference)))


(defn  write-entity [conn, _feedid, _refdom, _refid, _vtbl, _vcol, _content ]
  (let 	[	ent-rec {		:feedid 	_feedid   
                			:refdom  	_refdom
					:refid   	_refid
					:vtbl  	 	_vtbl
					:vcol    	_vcol
                                     	:content 	_content}]
	(cql/insert conn "entity_delta" ent-rec)))
	
	
	
	
(defn  write-pt [conn,ptrec]                              	
  (let 	[	feedid 	(int 1 )   
                refdom  "CUSIP"
		refid   (ptrec :CUSIP)
		vtable  "PT"]  
  (write-entity conn feedid refdom refid vtable "PURP-TYPE-CODE" 	(ptrec :PURP-TYPE-CODE))
  (write-entity conn feedid refdom refid vtable "PURP-CLASS-CODE" 	(ptrec :PURP-CLASS-CODE))
  (write-entity conn feedid refdom refid vtable "PURP-SUB-CLASS-CODE" 	(ptrec :PURP-SUB-CLASS-CODE))))	
		
(defn  write-ra [conn,rarec]                              	
  (let 	[	feedid 	(int 1 )   
                refdom  "CUSIP"
		refid   (rarec :CUSIP)
		vtable  "RA"]  
  (write-entity conn feedid refdom refid vtable "RATG-AGENCY-TYPE" 	(rarec :RATG-AGENCY-TYPE))
  (write-entity conn feedid refdom refid vtable "RATG-RATING" 		(rarec :RATG-RATING))
  (write-entity conn feedid refdom refid vtable "RATG-RATING-TYPE" 	(rarec :RATG-RATING-TYPE))))
  
  
(defn  load-pt  [ wbfile ] 
  (->>  (as/load-workbook wbfile)
  	(as/select-sheet "PT")
     	(as/select-columns {:A :CUSIP, :B :PURP-TYPE-CODE, :C :PURP-CLASS-CODE, :D :PURP-SUB-CLASS-CODE })))

(defn  load-ra [ wbfile ] 
  (->>  (as/load-workbook wbfile)
  	(as/select-sheet "RA")
     	(as/select-columns {:A :CUSIP, :B :RATG-AGENCY-TYPE, :C :RATG-RATING, :D :RATG-RATING-TYPE })))	
     	


(defn  capture-pt [wbfile conn]
(let 	[	ptdata	(load-pt wbfile )
        	ptwrite (fn [ptrec] (write-pt conn ptrec))]      	
	(map ptwrite ptdata )))
	
(defn  capture-ra [wbfile conn]
(let 	[	radata	(load-ra wbfile )
        	rawrite (fn [rarec] (write-ra conn rarec))]      	
	(map rawrite radata )))

(defn  capture-SNP_ALL [wbfile]
(do
   (let [	conn1 	(cc/connect ["127.0.0.1"])
                keysp1	(cql/use-keyspace conn1 "new_cql_keyspace")]
   	(capture-pt wbfile conn1)
   	)
   (let [	conn2 	(cc/connect ["127.0.0.1"])
                keysp2	(cql/use-keyspace conn2 "new_cql_keyspace")]
   	(capture-pt wbfile conn2)   	
   	)
   	))

(defn  capture-SNP_DEMO []
   	(capture-SNP_ALL "GDS360.xlsx") )
   	
   	
;;==============================


(defn  write-rec [conn, table, rec]
	(cql/insert conn table rec))

(defn  write-recset [ table, conn, recset]
(let 	[	recwrite (fn [rec] (write-rec conn rec))]      	
	(map recwrite recset )))
	
(defn  write-recsets [server, keyspace, table, & recsets ]
(let 	[	conn 	(cc/connect [server])
		keysp	(cql/use-keyspace conn keyspace)
		setwrite (fn [rset] (write-recset table, conn, rset))]      	
	(map setwrite recsets )))

(defn  load-recs [ wbfile, sheet, colmap ] 
  (->>  (as/load-workbook 	wbfile)
  	(as/select-sheet 	sheet)
     	(as/select-columns 	colmap )))	
     	
(defn  make-colmap [ cols, keys ]
	(into {} 
	  (for [[col key] (zipmap cols keys)] [(keyword col) (keyword key)])))
	  

    
