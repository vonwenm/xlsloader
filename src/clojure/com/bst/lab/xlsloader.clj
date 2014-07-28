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
	  
	  
	  
;;================================
;;==  LOAD JOB MANAGEMENT       ==


(defn demojobs []
  (execLoadJobs "JOBS.xlsx")
)

(def  execLoadJobs_JobListSheet "JOBLIST")
(def  execLoadJobs_JobListCols '( B, A ))
(def  execLoadJobs_JobListColNames '( JOB, STATUS ))
(def  execLoadJobs_JobSpecCols '( B, A ))
(def  execLoadJobs_JobSpecColNames '( SETTING, ITEM ))

(defn activeJob? [Job]
  (let 	[ status (Job :STATUS)]
  (if (= status "GO") true  false)))
  
 
(defn extractJobName [Job]
   (:JOB Job))
   
  
(defn extractJobSpec [wbfile, Job, ColMap]
  (load-recs wbfile Job ColMap))  
  
(defn xformJobSpec2Map   [jobSpec]
  (let [	Items		(map #(% :ITEM) 	jobSpec)
  		Settings	(map #(% :SETTING) 	jobSpec)]
  (into {} (for [[item setting] (zipmap Items Settings)] [(keyword item) setting]))))


(defn makeLoadTask [feedFile, feedFormat]
  (let	[ directoryCols 	'( E, D, C, B, A )
          directoryColNames 	'( DESCR, NAME, LINK, SHEET, TABLE)
          directoryColMap	(make-colmap directoryCols directoryColNames)
  	  allFormatTbls	(load-recs feedFormat "DIRECTORY" directoryColMap)
          ]
  nil
 ))  
 
(defn execJobSpec [wbfile, Job, ColMap]
  (let	[ jobSpec  	(extractJobSpec wbfile Job ColMap)
          jobMap   	(xformJobSpec2Map 	jobSpec)
          feedFormat  	(:FEED_FORMAT jobMap )
          feedFile  	(:FEED_FILE	(xformJobSpec2Map 	jobSpec))
          jobName  	(:JOB_NAME	(xformJobSpec2Map 	jobSpec)) 
          feedDate  	(:FEED_DATE	(xformJobSpec2Map 	jobSpec))
          directoryCols 	'( B, A )
          directoryColNames 	'( DESCR, TABLE)
          directoryColMap	(make-colmap directoryCols directoryColNames)
  	  allFormatTbls	(load-recs feedFormat "DIRECTORY" directoryColMap)
  	  makeTask  (fn [sheet]{:FEED_FILE feedFile, :FEED_SHEET sheet , :COLS directoryCols, :COLNAMES directoryColNames}) 
          ]
  (makeLoadTask  feedFile feedFormat)
 ))  
         


            
(defn execLoadJobs [wbfile]
  (let 	[  	joblistcolmap	(make-colmap execLoadJobs_JobListCols execLoadJobs_JobListColNames)
  		alljobs (load-recs wbfile execLoadJobs_JobListSheet joblistcolmap)
  		activejobs (filter activeJob? alljobs)
  		activeJobNames (map extractJobName activejobs)
  		jobspeccolmap	(make-colmap execLoadJobs_JobSpecCols execLoadJobs_JobSpecColNames)
  		execJob (fn [job] (execJobSpec wbfile, job, jobspeccolmap))]
  (into [] (map execJob  activeJobNames))
  		))
	  


