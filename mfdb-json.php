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

$longoptions = array("beta::", "help::", "host::", "user::", "password::");

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
$result = $stmt->fetchAll(PDO::FETCH_COLUMN);

print "Logging into Schedules Direct.\n";
$randHash = getRandhash($username, $password, $baseurl, $api);

if ($randHash != "ERROR")
{
    getStatus($randHash, $api);
}


function getStatus($rh, $api)
{
    print "Status messages from Schedules Direct:";
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
                                               'method'  => 'POST',
                                               'header'  => 'Content-type: application/x-www-form-urlencoded',
                                               'content' => $data
                                           )
    ));

    return rtrim(file_get_contents("http://23.21.174.111/handleRequest.php", false, $context));
}

?>