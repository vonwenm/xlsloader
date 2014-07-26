(ns dk.ative.docjure.spreadsheet
  (:import
   (java.io FileOutputStream FileInputStream)
   (java.util Date Calendar)
   (org.apache.poi.xssf.usermodel XSSFWorkbook)
   (org.apache.poi.ss.usermodel Workbook Sheet Cell Row
                                WorkbookFactory DateUtil
                                IndexedColors CellStyle Font
                                CellValue)
   (org.apache.poi.ss.util CellReference AreaReference)))

(defmacro assert-type [value expected-type]
  `(when-not (isa? (class ~value) ~expected-type)
     (throw (IllegalArgumentException.
             (format "%s is invalid. Expected %s. Actual type %s, value: %s"
                     (str '~value) ~expected-type (class ~value) ~value)))))

(defn cell-reference [^Cell cell]
  (.formatAsString (CellReference. (.getRowIndex cell) (.getColumnIndex cell))))

(defmulti read-cell-value (fn [^CellValue cv date-format?] (.getCellType cv)))
(defmethod read-cell-value Cell/CELL_TYPE_BOOLEAN  [^CellValue cv _]  (.getBooleanValue cv))
(defmethod read-cell-value Cell/CELL_TYPE_STRING   [^CellValue cv _]  (.getStringValue cv))
(defmethod read-cell-value Cell/CELL_TYPE_NUMERIC  [^CellValue cv date-format?]
  (if date-format?
    (DateUtil/getJavaDate (.getNumberValue cv))
    (.getNumberValue cv)))

(defmulti read-cell #(.getCellType ^Cell %))
(defmethod read-cell Cell/CELL_TYPE_BLANK     [_]     nil)
(defmethod read-cell Cell/CELL_TYPE_STRING    [^Cell cell]  (.getStringCellValue cell))
(defmethod read-cell Cell/CELL_TYPE_FORMULA   [^Cell cell]
  (let [evaluator (.. cell getSheet getWorkbook
                      getCreationHelper createFormulaEvaluator)
        cv (.evaluate evaluator cell)]
    (read-cell-value cv false)))
(defmethod read-cell Cell/CELL_TYPE_BOOLEAN   [^Cell cell]  (.getBooleanCellValue cell))
(defmethod read-cell Cell/CELL_TYPE_NUMERIC   [^Cell cell]
  (if (DateUtil/isCellDateFormatted cell)
    (.getDateCellValue cell)
    (.getNumericCellValue cell)))

(defn load-workbook
  "Load an Excel .xls or .xlsx workbook from a file."
  [^String filename]
  (with-open [stream (FileInputStream. filename)]
    (WorkbookFactory/create stream)))

(defn save-workbook!
  "Save the workbook into a file."
  [^String filename ^Workbook workbook]
  (assert-type workbook Workbook)
  (with-open [file-out (FileOutputStream. filename)]
    (.write workbook file-out)))

(defn sheet-seq
  "Return a lazy seq of the sheets in a workbook."
  [^Workbook workbook]
  (assert-type workbook Workbook)
  (for [idx (range (.getNumberOfSheets workbook))]
    (.getSheetAt workbook idx)))

(defn sheet-name
  "Return the name of a sheet."
  [^Sheet sheet]
  (assert-type sheet Sheet)
  (.getSheetName sheet))

(defn- find-sheet
  [matching-fn ^Workbook workbook]
  (assert-type workbook Workbook)
  (->> (sheet-seq workbook)
       (filter matching-fn)
       first))

(defmulti select-sheet
  "Select a sheet from the workbook by name, regex or arbitrary predicate"
  (fn [predicate ^Workbook workbook]
    (class predicate)))

(defmethod select-sheet String
  [name ^Workbook workbook]
  (find-sheet #(= name (sheet-name %)) workbook))

(defmethod select-sheet java.util.regex.Pattern
  [regex-pattern ^Workbook workbook]
  (find-sheet #(re-find regex-pattern (sheet-name %)) workbook))

(defmethod select-sheet :default
  [matching-fn ^Workbook workbook]
  (find-sheet matching-fn workbook))

(defn row-seq
  "Return a lazy sequence of the rows in a sheet."
  [^Sheet sheet]
  (assert-type sheet Sheet)
  (iterator-seq (.iterator sheet)))

(defn- cell-seq-dispatch [x]
  (cond
   (isa? (class x) Row) :row
   (isa? (class x) Sheet) :sheet
   (seq? x) :coll
   :else :default))

(defmulti cell-seq
  "Return a seq of the cells in the input which can be a sheet, a row, or a collection
   of one of these. The seq is ordered ordered by sheet, row and column."
  cell-seq-dispatch)
(defmethod cell-seq :row  [^Row row] (iterator-seq (.iterator row)))
(defmethod cell-seq :sheet [sheet] (for [row (row-seq sheet)
                                         cell (cell-seq row)]
                                     cell))
(defmethod cell-seq :coll [coll] (for [x coll,
                                       cell (cell-seq x)]
                                   cell))


(defn into-seq
  [^Iterable sheet-or-row]
  (vec (for [item (iterator-seq (.iterator sheet-or-row))] item)))

(defn- project-cell [column-map ^Cell cell]
  (let [colname (-> cell
                    .getColumnIndex
                    org.apache.poi.ss.util.CellReference/convertNumToColString
                    keyword)
        new-key (column-map colname)]
    (when new-key
      {new-key (read-cell cell)})))

(defn select-columns [column-map ^Sheet sheet]
  "Takes two arguments: column hashmap and a sheet. The column hashmap 
   specifies the mapping from spreadsheet columns dictionary keys:
   its keys are the spreadsheet column names and the values represent 
   the names they are mapped to in the result.

   For example, to select columns A and C as :first and :third from the sheet

   (select-columns {:A :first, :C :third} sheet)
   => [{:first \"Value in cell A1\", :third \"Value in cell C1\"} ...] "
  (assert-type sheet Sheet)
  (vec
   (for [row (into-seq sheet)]
     (->> (map #(project-cell column-map %) row)
          (apply merge)))))

(defn string-cell? [^Cell cell]
  (= Cell/CELL_TYPE_STRING (.getCellType cell)))

(defn- date-or-calendar? [value]
  (let [cls (class value)]
    (or (isa? cls Date) (isa? cls Calendar))))

(defn apply-date-format! [^Cell cell ^String format]
  (let [workbook (.. cell getSheet getWorkbook)
        date-style (.createCellStyle workbook)
        format-helper (.getCreationHelper workbook)]
    (.setDataFormat date-style
                    (.. format-helper createDataFormat (getFormat format)))
    (.setCellStyle cell date-style)))

(defn set-cell! [^Cell cell value]
  (if (nil? value)
    (let [^String null nil]
      (.setCellValue cell null)) ;do not call setCellValue(Date) with null
    (let [converted-value (cond (number? value) (double value)
                                :else value)]
      ;;The arg to setCellValue takes multiple types so no type-hinting here
      ;; See http://poi.apache.org/apidocs/org/apache/poi/ss/usermodel/Cell.html
      (.setCellValue cell converted-value)
      (if (date-or-calendar? value)
        (apply-date-format! cell "m/d/yy")))))

(defn add-row! [^Sheet sheet values]
  (assert-type sheet Sheet)
  (let [row-num (if (= 0 (.getPhysicalNumberOfRows sheet))
                  0
                  (inc (.getLastRowNum sheet)))
        row (.createRow sheet row-num)]
    (doseq [[column-index value] (partition 2 (interleave (iterate inc 0) values))]
      (set-cell! (.createCell row column-index) value))
    row))

(defn add-rows! [^Sheet sheet rows]
  "Add rows to the sheet. The rows is a sequence of row-data, where
   each row-data is a sequence of values for the columns in increasing
   order on that row."
  (assert-type sheet Sheet)
  (doseq [row rows]
    (add-row! sheet row)))

(defn add-sheet!
  "Add a new sheet to the workbook."
  [^Workbook workbook name]
  (assert-type workbook Workbook)
  (.createSheet workbook name))


(defn create-workbook
  "Create a new workbook with a single sheet and the data specified.
   The data is given a vector of vectors, representing the rows
   and the cells of the rows.

   For example, to create a workbook with a sheet with
   two rows of each three columns:

   (create-workbook \"Sheet 1\"
                    [[\"Name\" \"Quantity\" \"Price\"]
                     [\"Foo Widget\" 2 42]])
   "
  [sheet-name data]
  (let [workbook (XSSFWorkbook.)
        sheet    (add-sheet! workbook sheet-name)]
    (add-rows! sheet data)
    workbook))

(defn create-font!
  "Create a new font in the workbook.

   Options are

       :bold    true/false   bold or normal font

   Example:

      (create-font! wb {:bold true})
   "
  [^Workbook workbook options]
  (let [defaults {:bold false}
        cfg (merge defaults options)]
    (assert-type workbook Workbook)
    (let [f (.createFont workbook)]
      (doto f
        (.setBoldweight (if (:bold cfg) Font/BOLDWEIGHT_BOLD Font/BOLDWEIGHT_NORMAL)))
      f)))


(defn create-cell-style!
  "Create a new cell-style.
   Options is a map with the cell style configuration:

      :background     the name of the background colour (as keyword)

   Valid keywords are the colour names defined in
   org.apache.ss.usermodel.IndexedColors as lowercase keywords, eg.

     :black, :white, :red, :blue, :green, :yellow, ...

   Example:

   (create-cell-style! wb {:background :yellow})
  "
  ([^Workbook workbook] (create-cell-style! workbook {}))

  ([^Workbook workbook styles]
     (assert-type workbook Workbook)
     (let [cs (.createCellStyle workbook)
           {background :background, font-style :font} styles
           font (create-font! workbook font-style)]
       (do
         (.setFont cs font)
         (when background
           (let [bg-idx (.getIndex (IndexedColors/valueOf
                                    (.toUpperCase (name background))))]
             (.setFillForegroundColor cs bg-idx)
             (.setFillPattern cs CellStyle/SOLID_FOREGROUND)))
         cs))))

(defn set-cell-style!
  "Apply a style to a cell.
   See also: create-cell-style!.
  "
  [^Cell cell ^CellStyle style]
  (assert-type cell Cell)
  (assert-type style CellStyle)
  (.setCellStyle cell style)
  cell)

(defn set-row-style!
  "Apply a style to all the cells in a row.
   Returns the row."
  [^Row row ^CellStyle style]
  (assert-type row Row)
  (assert-type style CellStyle)
  (dorun (map #(.setCellStyle ^Cell % style) (cell-seq row)))
  row)

(defn get-row-styles
  "Returns a seq of the row's CellStyles."
  [^Row row]
  (map #(.getCellStyle ^Cell %) (cell-seq row)))

(defn set-row-styles!
  "Apply a seq of styles to the cells in a row."
  [^Row row styles]
  (let [pairs (partition 2 (interleave (cell-seq row) styles))]
    (doseq [[^Cell c s] pairs]
      (.setCellStyle c s))))

(defn row-vec
  "Transform the row struct (hash-map) to a row vector according to the column order.
   Example:

     (row-vec [:foo :bar] {:foo \"Foo text\", :bar \"Bar text\"})
     > [\"Foo text\" \"Bar text\"]
  "
  [column-order row]
  (vec (map row column-order)))

(defn remove-row!
  "Remove a row from the sheet."
  [^Sheet sheet ^Row row]
  (do
    (assert-type sheet Sheet)
    (assert-type row Row)
    (.removeRow sheet row)
    sheet))

(defn remove-all-rows!
  "Remove all the rows from the sheet."
  [sheet]
  (doall
   (for [row (doall (row-seq sheet))]
     (remove-row! sheet row)))
  sheet)

(defn- named-area-ref [^Workbook workbook n]
  (let [index (.getNameIndex workbook (name n))]
    (if (>= index 0)
      (->> index
           (.getNameAt workbook)
           (.getRefersToFormula)
           (AreaReference.))
      nil)))

(defn- cell-from-ref [^Workbook workbook ^CellReference cref]
  (let [row (.getRow cref)
        col (int (.getCol cref))
        sheet (->> cref (.getSheetName) (.getSheet workbook))]
    (-> sheet (.getRow row) (.getCell col))))

(defn select-name
  "Given a workbook and name (string or keyword) of a named range, select-name returns a seq of cells or nil if the name could not be found."
  [^Workbook workbook n]
  (if-let [^AreaReference aref (named-area-ref workbook n)]
    (map (partial cell-from-ref workbook) (.getAllReferencedCells aref))
    nil))

(defn add-name! [^Workbook workbook n string-ref]
  (let [the-name (.createName workbook)]
    (.setNameName the-name (name n))
    (.setRefersToFormula the-name string-ref)))
