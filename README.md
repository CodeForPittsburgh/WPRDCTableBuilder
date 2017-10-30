# WPRDCTableBuilder
Table Builder ProtoType
/*
 * Mark Howe
 * 10/30/2017
 * Prototype that reads the WPRDC data set,
 * parses the JSON data
 * Finds the field names
 * Creates label and type arrays to process the records
 * Uses the label and results to create insert statements saved in insert.sql
 * Uses the label and reformatting code to build the create table
 * This is postgresql specific, and uses my policeblotter2 schema
 * Other databases, will have their own column definitions
 * This has been tested on the fire and city location data sets
 */
