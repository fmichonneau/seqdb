
library(RSQLite)

db_structure <- list(
    plate_data =
        "CREATE TABLE `plate_data` (
          `plate`	TEXT,
	  `row`	TEXT,
	  `column`	TEXT,
	  `phylum`	TEXT,
	  `ufid_unique`	TEXT,
	  `ufid`	TEXT,
	  `filled_by`	TEXT,
	  `notes`	TEXT,
          PRIMARY KEY(plate, row, column)
        );",
    sequence_data =
        "CREATE TABLE `sequence_data` (
	   `sequence_plate`	text,
	   `sequence_row`	text,
	   `sequence_column`	text,
           `sequence`           text,
           `sequence_attempt`   text,
           FOREIGN KEY (sequence_plate, sequence_row, sequence_column)  REFERENCES plate_data(plate, row, column)
        );",
    sequence_info =
        "CREATE TABLE `sequence_info` (
           `sequence_plate`        text,
           `sequence_row`	   text,
	   `sequence_column`	   text,
           `matrix_plate_barcode`  text,
           `sequence_successful`   boolean,
           `sequence_contaminated` boolean,
           `sequence_pseudogene`   boolean,
           `sequence_note`         text,
         FOREIGN KEY (sequence_plate, sequence_row, sequence_column)  REFERENCES plate_data(plate, row, column)
        );",
    dictionary_phyla_id =
        "CREATE TABLE `dictionary_phyla_id` (
	`phyla_ID`	TEXT,
	`phylum`	TEXT
        );",
    plate_tracking =
        "CREATE TABLE `plate_tracking` (
	`plate`	        TEXT,
	`is_full`	TEXT DEFAULT '(null)',
	`sent_on`	TEXT,
	`received_on`	TEXT,
	`extracted_on`	TEXT,
        `notes`         TEXT
        );")

    ## ufdb =
    ##     "CREATE TABLE `ufdb` (
    ##     `phyla_ID`	TEXT,
    ##     `uf_id`	TEXT,
    ##     `class_`	TEXT,
    ##     `family`	TEXT,
    ##     `taxon`	TEXT,
    ##     `id_confidence`	TEXT,
    ##     `accession_ID`	TEXT,
    ##     `previous_numbers`	TEXT,
    ##     `station_number`	TEXT,
    ##     `continent_ocean`	TEXT,
    ##     `country_archipelago`	TEXT,
    ##     `primary_subdivision`	TEXT,
    ##     `secondary_subdivision`	TEXT,
    ##     `locality`	TEXT,
    ##     `latitude`	TEXT,
    ##     `longitude`	TEXT,
    ##     `fixatives`	TEXT,
    ##     `preservatives`	TEXT,
    ##     `collector`	TEXT,
    ##     `collection_method`	TEXT,
    ##     `year_collected`	TEXT,
    ##     `month_collected`	TEXT,
    ##     `day_collected`	TEXT,
    ##     `yr_coll_precision`	TEXT,
    ##     `month_coll_precision`	TEXT,
    ##     `day_coll_precision`	TEXT,
    ##     `habitat`	TEXT,
    ##     `microhabitat`	TEXT,
    ##     `depth`	TEXT,
    ##     `depth2`	TEXT,
    ##     `units`	TEXT,
    ##     `type_description`	TEXT,
    ##     `specimen_notes`	TEXT,
    ##     `on_loan`	TEXT,
    ##     `research_vessel`	TEXT,
    ##     `expedition`	TEXT
    ##     );")


create_database <- function(con, db_str) {
    res <- lapply(db_str, function(qry) {
        dbSendQuery(conn=con, statement=qry)
        })
    crtd <- sapply(names(db_str), function(x) {
        dbExistsTable(conn=con, x)
    })
    crtd
}


##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##' @title
##' @param con
##' @param file csv file with headers
##' @param table
##' @param ...
##' @return
##' @author Francois Michonneau
add_data <- function(con, file, table, ...) {
    if (!file.exists(file)) {
        stop(file, " doesn't exist.")
    }
    dbWriteTable(conn=con, value=file, name=table, row.names=FALSE,
                 overwrite=FALSE, append=TRUE, header=TRUE)
}

import_fasta <- function(file, prefix="FMSLI13", db_prefix="FMSL13_plate_",
                         attempt=1, con) {
    fcon <- readLines(file)
    titles <- grep(">", fcon)
    seqData <- fcon[setdiff(seq_len(length(fcon)), titles)]
    if (length(titles) != length(seqData)) {
        stop("Each sequence should be on a single line.")
    } else {
        ids <- lapply(strsplit(fcon[titles], "_"), function(x) {
            yy <- grep(prefix, x)
            if(length(yy) > 0) {
                plate <- x[yy+1]
                rowL <- gsub("([A-H]{1})(\\d{2})(.*)", "\\1", x[yy+2])
                colL <- gsub("([A-H]{1})(\\d{2})(.*)", "\\2", x[yy+2])
                c(plate, rowL, colL)
            }
        })
        title_pb <- sapply(ids, is.null)
        if (any(title_pb)) {
            warning(paste(fcon[titles][title_pb], collapse=", "), " don't have the prefix")
            seqData <- seqData[!title_pb]
        }
        res <- do.call("rbind", ids)
        res <- data.frame(sequence_plate = paste0(db_prefix, gsub("^0{1}", "", res[, 1])),
                          sequence_row = res[, 2],
                          sequence_column = gsub("^0{1}", "", res[, 3]),
                          sequence = seqData,
                          sequence_attempt = attempt,
                          stringsAsFactors=FALSE)
        stopifnot(all(res$sequence_row %in% LETTERS[1:8]) &&
                  all(res$sequence_column %in% as.character(1:12)))
        dbWriteTable(conn=con, value=res, name="sequence_data", overwrite=FALSE,
                     row.names=FALSE, append=TRUE)
    }
}


import_annotations <- function(file, db_prefix="FMSL13_", ...) {
    ann <- read.csv(file=file)

    data.frame(sequence_plate= paste0(db_prefix, gsub("^0{1}", "", res[, 1])),
               sequence_row=,
               sequence_column=)

}


con <- dbConnect(SQLite(), "20150407.lines_sequences.sqlite")

create_database(con, db_structure)

plate_data <- list.files(path="importedData/", pattern="^plate_(.+)csv$",
                         full.names=TRUE)
lapply(plate_data, function(x) add_data(con, table="plate_data", file=x))
add_data(con, table="plate_tracking", file="importedData/tracking_info.csv")
add_data(con, table="dictionary_phyla_id", file="importedData/dictionary_phyla_id.csv")
dbWriteTable(conn=con, value=read.csv("20150407-all_uf.csv"), name="ufdb",
             row.names=FALSE, overwrite=TRUE, header=TRUE)
import_fasta(file="fromMatt/FMSLI13_1-6.fasta", con=con)

dbDisconnect(con)


####

## sli_st <- read_excel("~/Documents/2013-10.Lines/2013-10.Lines_intermediateCataloging_phase2.xls")
## fm13 <- read.csv(file = "importedData/plate_FMSL13_13.csv", stringsAsFactors = FALSE)

## tt <- left_join(fm13, sli_st[, c("Field_#", "UF_ID")], by = c("ufid_unique" =  "Field_#"))
## write.csv(tt, file = "/tmp/fixUFID.csv")




##########################################""

matt_view_1 <- function() {
    con <- dbConnect(SQLite(), "20150407.lines_sequences.sqlite")
    res <- dbSendQuery(conn=con, "
SELECT plate_data.plate, plate_data.row, plate_data.column, plate_data.phylum, plate_data.ufid_unique, ufdb.taxon
FROM plate_data
JOIN dictionary_phyla_id ON dictionary_phyla_id.phylum=plate_data.phylum
JOIN ufdb ON (ufdb.phyla_ID=dictionary_phyla_id.phyla_ID AND ufdb.uf_id = plate_data.ufid)
JOIN plate_tracking ON plate_tracking.plate=plate_data.plate;")
#WHERE plate_tracking.sent_on IS '2015-02-22';")
    dat <- dbFetch(res) %>%
        arrange(plate, row, column)
    dbClearResult(res)
    dbDisconnect(con)
    write.csv(dat, file="/tmp/plate_matt.csv")
}
