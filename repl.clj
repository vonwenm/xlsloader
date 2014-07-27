(use 'dk.ative.docjure.spreadsheet)       

;; Create a spreadsheet and save it
(let [wb (create-workbook "Price List"
                          [["Name" "Price"]
                           ["Foo Widget" 100]
                           ["Bar Widget" 200]])
      sheet (select-sheet "Price List" wb)
      header-row (first (row-seq sheet))]
  (do
    (set-row-style! header-row (create-cell-style! wb {:background :yellow,
                                                       :font {:bold true}}))
    (save-workbook! "spreadsheet.xlsx" wb)))
    
    
    
    (use 'dk.ative.docjure.spreadsheet)       
    
    ;; Load a spreadsheet and read the first two columns from the 
    ;; price list sheet:
    (->> (load-workbook "spreadsheet.xlsx")
         (select-sheet "Price List")
     (select-columns {:A :name, :B :price}))
    
    
;;################################################################################


;; ## TABLE TBL_REF ##
;;===============

(use 'dk.ative.docjure.spreadsheet)       
     
;; Load a spreadsheet and read 
(->> (load-workbook "GDS360.xlsx")
	(select-sheet "TBL_REF")
     	(select-columns {:A :Spreadsheet, :B :Contents }))
     	
     	
;; ## TABLE Reference Layer _Issuer ##
;;===============

(use 'dk.ative.docjure.spreadsheet)       
     
;; Load a spreadsheet and read the columns from the ___ sheet:
(->> (load-workbook "GDS360.xlsx")
	(select-sheet "Reference Layer _Issuer")
     	(select-columns {:A :ISSUER, :B :LANG_CODE, :C :COMPANY_NAME, :D :DOM, :E :TK_DOM, :F :UPDATE, :G :DATE }))


;; ## TABLE Reference Layer _Issue ##
;;===============

(use 'dk.ative.docjure.spreadsheet)       
     
;; Load a spreadsheet and read the first two columns from the 
;; price list sheet:
(->> (load-workbook "GDS360.xlsx")
	(select-sheet "Reference Layer _Issue")
     	(select-columns {:A :ISSUER, :B :ISSUE_NUM, :C :ISSUE_CHECK, :D :ISSUE_PRINCIPAL_LANG, :E :ISSUE_PRINCIPAL_DESC, :F :ISSUE_ISO_DOMICILE, :G :ISSUE_ISO_CURRENCY, :H :CURRENCY, :I :CFI_CODE_FLAG, :J :DOMICILE,  :K :ISO_CFI, :L :ORIG_INDENTURE_YEAR, :M :MATURITY_DATE, :N :PAR_VALUE, :O :PARENT_KEY, :P :PRELIMINARY_OFFER, :Q :RATE, :R :SECURITY_TYPE, :S :ISSUE_DATE, :T :ISSUE_UPDATE_TYPE, :U :ISSUE_CHANGE_DATE }))
     	
																			
     	
     	
 
;; ## TABLE PT ##
;;===============

(use 'dk.ative.docjure.spreadsheet)       
     
;; Load a spreadsheet and read the first two columns from the 
;; price list sheet:
(->> (load-workbook "GDS360.xlsx")
	(select-sheet "PT")
     	(select-columns {:A :CUSIP, :B :PURP-TYPE-CODE, :C :PURP-CLASS-CODE, :D :PURP-SUB-CLASS-CODE }))
     	
     	
     	
(ns cassaforte.docs
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]
            [dk.ative.docjure.spreadsheet   :as as]     
            ))     	
     	
     	
(defn  load-pt  [ wbfile ] 
  (->>  (as/load-workbook wbfile)
  	(as/select-sheet "PT")
     	(as/select-columns {:A :CUSIP, :B :PURP-TYPE-CODE, :C :PURP-CLASS-CODE, :D :PURP-SUB-CLASS-CODE })))
  
(load-pt "GDS360.xlsx")
     

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

(defn  capture-pt []
(let 	[	conn 	(cc/connect ["127.0.0.1"])
        	ptdata	(load-pt "GDS360.xlsx")
        	ptwrite (fn [ptrec] (write-pt conn ptrec))]      	
	(cql/use-keyspace conn "new_cql_keyspace")
	(map ptwrite ptdata )))
	
;;============================


(defn  load-ra [ wbfile ] 
  (->>  (as/load-workbook wbfile)
  	(as/select-sheet "RA")
     	(as/select-columns {:A :CUSIP, :B :RATG-AGENCY-TYPE, :C :RATG-RATING, :D :RATG-RATING-TYPE })))	
	
		
(defn  write-ra [conn,ptrec]                              	
  (let 	[	feedid 	(int 1 )   
                refdom  "CUSIP"
		refid   (ptrec :CUSIP)
		vtable  "RA"]  
  (write-entity conn feedid refdom refid vtable "RATG-AGENCY-TYPE" 	(ptrec :RATG-AGENCY-TYPE))
  (write-entity conn feedid refdom refid vtable "RATG-RATING" 	(ptrec :RATG-RATING))
  (write-entity conn feedid refdom refid vtable "RATG-RATING-TYPE" 	(ptrec :RATG-RATING-TYPE))))
  
(defn  capture-ra []
(let 	[	conn 	(cc/connect ["127.0.0.1"])
        	ptdata	(load-ra "GDS360.xlsx")
        	ptwrite (fn [ptrec] (write-ra conn ptrec))]      	
	(cql/use-keyspace conn "new_cql_keyspace")
	(map ptwrite ptdata )))
	
(capture-ra)


(defn  capture-all []
   (do 
   	(capture-ra)
   	(capture-pt)))
   	
   	
(defn  capture-all []
   (do 
   	(capture-pt)
   	(capture-ra)))

(ns com.bst.lab.xlsloader 
  (:use com.bst.lab.xlsloader )  )
  
(use 'com.bst.lab.xlsloader)  
(use 'com.bst.lab) 

(use 'com.bst.lab.xlsloader)  
(capture-SNP_DEMO)

(com.bst.lab.xlsloader/capture-SNP_DEMO)