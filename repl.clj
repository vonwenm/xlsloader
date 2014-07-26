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