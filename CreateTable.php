<?php

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
/*
 * Update log:
 * Mark Howe 10/31/2017
 * Added id for offense 30 days, arrest and city properties
 * Data errors found with missing lat/lng
 * Data errors found missing Age in offense and arrest
 * Added code to write errors to errorlog.sql for review
 * Added $tablename variable to control creat and insert table information
 * Added code to handle bool 1) convert to BOOLEAN and 2) add column value to TRUE or FALSE
 * from the city property file
 */
$names = array();
$labels = array();
$types = array();
$column_names = array();
$text_size = array();
$timezone = "America/New_York";
date_default_timezone_set($timezone);
$seconds = 15000;
set_time_limit($seconds);

global $handle, $error, $tablename;
$filename = 'data\wprdcquery.json';
$handle = fopen("data/insert.sql", "w");
$errorlog = fopen("data/errorlog.sql", "w");
$tablename = "offense"; //change this based on your data download name


$cityid = "fbb50b02-2879-47cd-abea-ae697ec05170"; // change this based on your data download
$arrestid = "e03a89dd-134a-4ee8-a2bd-62c40aeebc6f";
$offenseid ="1797ead8-8262-41cc-9099-cbc8a161924b";
$fireid = "8d76ac6b-5ae8-4428-82a4-043130d17b02";

$month = "10"; // used in the where statement
$year = "2017"; // used in the where statement

$id = $offenseid;

$url = buildURL($id, $month, $year);
print "Built url " . $url . PHP_EOL;

main($filename, $url);


print "Done" . "</BR>";

function buildURL($id, $month, $year) {
    $allurl = "https://data.wprdc.org/api/action/datastore_search_sql?sql=SELECT+%2A+from+%22" . $id . "%22";
    $url = "https://data.wprdc.org/api/action/datastore_search_sql?sql=SELECT+%2A+from+%22" . $id . "%22+WHERE+date_part%28%27year%27%2C+%22alarm_time%22%29+%3D+%27" . $year . "%27+and+date_part%28%27month%27%2C+%22alarm_time%22%29+%3D+%27" . $month . "%27";
    return $allurl;
}

// The is based on running in a web server
// lots of debug output at this time

function main($filename, $url) {
    global $column_names, $labels, $types;
    print $filename . "</BR>";
    getCurl($filename, $url);

    parsesqljson($filename);
    createColumnNames($labels, $types);
    buildSQL($column_names);
}

function createColumnNames($labels, $types) {
    global $column_names;
    for ($i = 0; $i < sizeof($labels); $i++) {

        print "ID " . $labels[$i] . "<BR>";
        print "TYPE " . $types[$i] . "<BR>";
        $pg_type = createTypes($types[$i], $i);
        $column_name = $labels[$i] . " " . $pg_type . ",";
        print $column_name . "<BR>";
        array_push($column_names, $column_name);
    }
}

// this is postgresql specific, other database would need changes
// also there might be other types that are available in other WPRDC datasets
function createTypes($type, $column_ptr) {
    global $text_size;
    switch ($type) {
        case "text":
            return "character(" . $text_size[$column_ptr] . ")";

        case "timestamp":
            return "timestamp";

        case "int4":
            return "integer";
        case "float8":
            return "numeric";
        case "bool":
            return "BOOLEAN DEFAULT FALSE";
        case "tsvector";
            return "character(" . $text_size[$column_ptr] . ")";
        case "_full_text ":
            return "character(" . $text_size[$column_ptr] . ")";
        default:
            return $type;
    }
}

// my schema is policeblotter2 SURPRISED!
function buildSQL($column_names) {
    global $tablename;
    $prefix = "CREATE TABLE \"PoliceBlotter2\"." . $tablename . "data2 (" . $tablename . "id serial NOT NULL,";
    $suffix = "CONSTRAINT " . $tablename . "2_pkey PRIMARY KEY (" . $tablename . "id))WITH (OIDS=FALSE);";
    $create_table = $prefix;

    for ($i = 0; $i < sizeof($column_names); $i++) {
        $create_table .= $column_names[$i];
    }
    $create_table .= $suffix;
    print $create_table . "<BR>";
}

function getCurl($filename, $url) {

    $wprdccurl = $url;
    $ch = curl_init($wprdccurl);
    $fp = fopen($filename, "w");

    curl_setopt($ch, CURLOPT_FILE, $fp);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_exec($ch);
    curl_close($ch);
    fclose($fp);
}

function parsesqljson($filename) {

    $fp = fopen($filename, "r") or exit("Unable to open file!");

    $pb = fgets($fp);
    fclose($fp);
    getJSON($pb);
}

function getJSON($pb) {
    $data = json_decode($pb, true);
    print sizeof($data) . "</BR>";
    $data0 = $data['result'];
    print "results: " . sizeof($data0) . "</BR>";
    getFields($data0);
    getRecords($data0);
}

function getRecords($data0) {
    global $labels;

    $data1 = $data0['records'];
    $maxcount = sizeof($data1);
    print "get records count " . $maxcount . "</BR>";
    for ($i = 0; $i < $maxcount; $i++) {

        setNames($labels, $data1, $i);
    }
}

function getFields($data0) {

    $data2 = $data0['fields'];
    $maxcount2 = sizeof($data2);
    print "fields count: " . $maxcount2 . "</BR>";
    for ($i = 0; $i < $maxcount2; $i++) {
        $field[0] = $data2[$i]['id'];
        $field[1] = $data2[$i]['type'];
        // I removed the _full_text column, I didn't see a need for duplicated information
        if ($field[0] === "_full_text") {
            
        } else {
            print $field[0] . " " . $field[1] . "<BR>";
            setLabels($field[0]);
            setTypes($field[1]);
        }
    }
}

function setNames($labels, $data1, $location) {
    global $names, $types;
    unset($names);
    $names = array();
    $labelsize = sizeof($labels);
    for ($i = 0; $i < $labelsize; $i++) {
        $name = pg_escape_string($data1[$location][$labels[$i]]);
        // this was needed for the city dataset, potential code changes
        // for other datasets
        if ($labels[$i] === "inactive" || $labels[$i] === "rentable") {
            if ($name) {
                $name = 'TRUE';
            } else {
                $name = 'FALSE';
            }
        }
        array_push($names, $name);
    }
    printFireResults($labels, $names, $types);
}

function setLabels($column_name) {
    global $labels, $text_size;

    array_push($labels, $column_name);
    array_push($text_size, 0);
}

function setTypes($column_name) {
    global $types;

    array_push($types, $column_name);
}

function printFireResults($labels, $names, $types) {
    $sizeofLabels = sizeof($labels);
    for ($i = 0; $i < $sizeofLabels; $i++) {
        print $labels[$i] . " " . $names[$i] . " +++" . strlen($names[$i]) . "+++</BR>";
        updateTextSize($i, strlen($names[$i]));
    }
    createSQL($labels, $names, $types);
    print "**************************************************" . "</BR>";
}

function updateTextSize($i, $textlength) {
    global $text_size;
    if ($text_size[$i] < $textlength) {
        $text_size[$i] = $textlength;
    }
}

function createSQL($labels, $names, $types) {
    global $handle, $errorlog, $tablename;
    $writeerror = FALSE;
    $namelength = count($names);
    print "size of names " . $namelength . "</BR>";
    // print_r($names);
    print "</BR>";

    $labellength = count($labels);
    print "size of names " . $labellength . "</BR>";
    // print_r($names);
    print "</BR>";

    $insert = "INSERT INTO \"PoliceBlotter2\"." . $tablename . "data2(";
    for ($i = 0; $i < $labellength - 1; $i++) {
        print $labels[$i] . "<BR>";

        $insert .= $labels[$i] . ",";
    }
    print $labels[$labellength - 1] . "<BR>";
    $insert .= $labels[$labellength - 1] . ")";


    $values = " values(";
    for ($i = 0; $i < $namelength - 1; $i++) {

        print "label and len " . $labels[$i] . " " . strlen($names[$i]) . "<BR>";
        // There might be other errors, but this is what I've found so far
        if ($labels[$i] === "alarm_time" && strlen($names[$i]) === 0) {
            $writeerror = TRUE;
        }

        if ($labels[$i] === "arrival_time" && strlen($names[$i]) === 0) {
            $writeerror = TRUE;
        }
        if ($labels[$i] === "latitude" && strlen($names[$i]) === 0) {
            $writeerror = TRUE;
        }
        if ($labels[$i] === "X" && strlen($names[$i]) === 0) {
            $writeerror = TRUE;
        }
        if ($labels[$i] === "longitude" && strlen($names[$i]) === 0) {
            $writeerror = TRUE;
        }
        if ($labels[$i] === "Y" && strlen($names[$i]) === 0) {
            $writeerror = TRUE;
        }
        if ($labels[$i] === "AGE" && strlen($names[$i]) === 0) {
            $writeerror = TRUE;
        }
        if ($types[$i] === "text" || $types[$i] === "timestamp") {
            $values .= "'" . $names[$i] . "',";
        } else {
            $values .= $names[$i] . ",";
        }
    }
    print "label and len " . $labels[$namelength - 1] . " " . strlen($names[$namelength - 1]) . "<BR>";
    if ($labels[$namelength - 1] === "longitude" && strlen($names[$namelength - 1]) === 0) {
        $writeerror = TRUE;
    }
    $values .= $names[$namelength - 1] . ")";


    $SQL = $insert . $values . ";\n";

    print "Insert SQL: " . $SQL . "</BR>";
    if ($writeerror) {
        fputs($errorlog, $SQL);
    } else {
        fputs($handle, $SQL);
    }
}
