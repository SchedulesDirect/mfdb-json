#!/usr/bin/php

<?php
/*
 * This file is a grabber which downloads data from Schedules Direct's JSON service.
 */

$isBeta = TRUE;
date_default_timezone_set("America/Chicago");
$date = new DateTime();
$todayDate = $date->format("Y-m-d");

$user = "mythtv";
$password = "mythtv";
$host = "localhost";
$db = "mythconverg";

$longoptions = array("beta::", "help::", "host::", "init::", "user::", "password::");

$options = getopt("h::", $longoptions);
foreach ($options as $k => $v)
{
    switch ($k)
    {
        case "beta":
            $isBeta == TRUE;
            break;
        case "help":
        case "h":
            print "The following options are available:\n";
            print "--beta\n";
            print "--help (this text)\n";
            print "--host=\t\texample: --host=192.168.10.10\n";
            print "--user=\t\tUsername to connect as\n";
            print "--password=\tPassword to access database.\n";
            exit;
        case "host":
            $host = $v;
            break;
        case "init":
            dbInit($dbh);
            break;
        case "user":
            $user = $v;
            break;
        case "password":
            $password = $v;
            break;
    }
}

print "Attempting to connect to database.\n";

try
{
    $dbh = new PDO("mysql:host=$host;dbname=$db;charset=utf8", $user, $password,
        array(PDO::ATTR_PERSISTENT => true));
    $dbh->exec("SET CHARACTER SET utf8");
    $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
} catch (PDOException $e)
{
    print "Exception with PDO: " . $e->getMessage() . "\n";
    exit;
}

if ($isBeta)
{
    # Test server. Things may be broken there.
    $baseurl = "http://23.21.174.111";
    print "Using beta server.\n";
    # API must match server version.
    $api = 20130512;
}

$stmt = $dbh->prepare("SELECT sourceid,name,userid,lineupid,password FROM videosource");
$stmt->execute();
$result = $stmt->fetchAll(PDO::FETCH_ASSOC);

foreach ($result[0] as $k => $v)
{
    print "k is $k\tv is $v\n";
    switch ($k)
    {
        case
        "userid":
            $username = $v;
            break;
        case
        "password":
            $password = sha1($v);
            break;
    }
}

print "Retrieving list of channels.\n";
$stmt = $dbh->prepare("SELECT DISTINCT(xmltvid) FROM channel WHERE visible=TRUE");
$stmt->execute();
$stationIDs = $stmt->fetchAll(PDO::FETCH_COLUMN);

print "Logging into Schedules Direct.\n";
$randHash = getRandhash($username, $password, $baseurl, $api);

if ($randHash != "ERROR")
{
    getStatus($randHash, $api);
    getSchedules($dbh, $randHash, $api, $stationIDs);
}

function getSchedules($dbh, $rh, $api, array $stationIDs)
{
    $programCache = array();
    $dbProgramCache = array();

    print "Sending schedule request.\n";
    $res = array();
    $res["action"] = "get";
    $res["object"] = "schedules";
    $res["randhash"] = $rh;
    $res["api"] = $api;
    $res["request"] = $stationIDs;

    $response = sendRequest(json_encode($res));

    $res = array();
    $res = json_decode($response, true);

    if ($res["response"] == "OK")
    {
        $tempdir = tempdir();

        $filename = $res["filename"];
        $url = $res["URL"];
        file_put_contents("$tempdir/$filename", file_get_contents($url));

        $zipArchive = new ZipArchive();
        $result = $zipArchive->open("$tempdir/$filename");
        if ($result === TRUE)
        {
            $zipArchive->extractTo("$tempdir");
            $zipArchive->close();
            foreach (glob("$tempdir/sched*") as $f)
            {
                $a = json_decode(file_get_contents($f), true);
                foreach ($a["programs"] as $v)
                {
                    $programCache[$v["programID"]] = $v["md5"];
                }
            }
        }
        else
        {
            print "FATAL: Could not open zip file.\n";
            exit;
        }
    }

    print "There are " . count($programCache) . " programIDs in the upcoming schedule.\n";
    print "Retrieving existing MD5 values.\n";

    $stmt = $dbh->prepare("SELECT programID,md5 FROM programMD5Cache");
    $stmt->execute();
    $dbProgramCache = $stmt->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_GROUP);

    $insertStack = array();
    $replaceStack = array();

    foreach ($programCache as $k => $v)
    {
        if (array_key_exists($k, $dbProgramCache))
        {
            /*
             * First we'll check if the key (the programID) exists in the database already, and if yes, does it have
             * the same md5 value as the one that we downloaded?
             */
            if ($dbProgramCache[$k] != $v)
            {
                $replaceStack[] = $k;
            }
        }
        else
        {
            /*
             * The programID wasn't in the database, so we'll need to get it.
             */
            $insertStack[] = $k;
        }
    }

    /*
     * Now we've got an array of programIDs that we need to download, either because we didn't have them,
     * or they have different md5's.
     */

    print "Need to download " . count($insertStack) . " new programs.\n";
    print "Need to download " . count($replaceStack) . "updated programs.\n";

    print "Sending program request.\n";
    $res = array();
    $res["action"] = "get";
    $res["object"] = "programs";
    $res["randhash"] = $rh;
    $res["api"] = $api;
    $res["request"] = array_merge($insertStack, $replaceStack);

    $response = sendRequest(json_encode($res));

    $res = array();
    $res = json_decode($response, true);
    if ($res["response"] == "OK")
    {
        $tempdir = tempdir();

        $filename = $res["filename"];
        $url = $res["URL"];
        file_put_contents("$tempdir/$filename", file_get_contents($url));

        $zipArchive = new ZipArchive();
        $result = $zipArchive->open("$tempdir/$filename");
        if ($result === TRUE)
        {
            $zipArchive->extractTo("$tempdir");
            $zipArchive->close();
        }
    }
}

function parseScheduleFile(array $sched)
{
    /*
     * This function takes an array and pulls out the programIDs and the md5
     */

    $pID = array();

    foreach ($sched["programs"] as $v)
    {
        $pID[$v["programID"]] = $v["md5"];
    }

    return $pID;
}


function getStatus($rh, $api)
{
    print "Status messages from Schedules Direct:\n";
    $res = array();
    $res["action"] = "get";
    $res["object"] = "status";
    $res["randhash"] = $rh;
    $res["api"] = $api;

    $response = sendRequest(json_encode($res));

    $res = array();
    $res = json_decode($response, true);

    $am = array();
    $he = array();

    // var_dump($res);


    foreach ($res as $k => $v)
    {
        switch ($k)
        {
            case "account":
                foreach ($v["messages"] as $a)
                {
                    $am[$a["msgID"]] = array("date" => $a["date"], "message" => $a["message"]);
                }
                $expires = $v["expires"];
                $maxHeadends = $v["maxHeadends"];
                $nextConnectTime = $v["nextSuggestedConnectTime"];
                break;
            case "headend":
                foreach ($v as $hk => $hv)
                {
                    $he[$hv["ID"]] = $hv["modified"];
                }
                break;
        }
    }

    // print "headends:\n\n";
    // var_dump($he);

    print "Used server: " . $res["serverID"] . "\n";
    print "Last data refresh: " . $res["lastDataUpdate"] . "\n";

    print "\n";

}

function getRandhash($username, $password, $baseurl, $api)
{
    $res = array();
    $res["action"] = "get";
    $res["object"] = "randhash";
    $res["request"] = array("username" => $username, "password" => $password);
    $res["api"] = $api;

    $response = sendRequest(json_encode($res));

    $res = array();
    $res = json_decode($response, true);

    if (json_last_error() != 0)
    {
        print "JSON decode error:\n";
        var_dump($response);
        exit;
    }

    if ($res["response"] == "OK")
    {
        return $res["randhash"];
    }

    print "Response from schedulesdirect: $response\n";

    return "ERROR";
}

function sendRequest($jsonText)
{
    $data = http_build_query(array("request" => $jsonText));

    $context = stream_context_create(array('http' =>
    array(
        'method' => 'POST',
        'header' => 'Content-type: application/x-www-form-urlencoded',
        'content' => $data
    )
    ));

    return rtrim(file_get_contents("http://23.21.174.111/handleRequest.php", false, $context));
}

function tempdir()
{
    $tempfile = tempnam(sys_get_temp_dir(), "mfdb");
    if (file_exists($tempfile))
    {
        unlink($tempfile);
    }
    mkdir($tempfile);
    if (is_dir($tempfile))
    {
        return $tempfile;
    }
}

function dbInit($dbh)
{
    $stmt = $dbh->prepare("CREATE TABLE programMD5Cache (`row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `programID` char(14) NOT NULL DEFAULT '',
  `md5` char(22) NOT NULL,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`row`),
  UNIQUE KEY `pid-MD5` (`prog_id`,`md5`)
  )
  ENGINE = InnoDB DEFAULT CHARSET=utf8
)");

    $stmt->execute();
}

?>