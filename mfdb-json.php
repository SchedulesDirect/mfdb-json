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

$longoptions = array("beta::","host::","user::","password::");

$options = getopt("", $longoptions);
foreach ($options as $k => $v)
{
    switch ($k)
    {
        case "beta":
            $isBeta == TRUE;
            break;
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

$videoSources = $dbh->prepare("SELECT * FROM videosource");
$videoSources->execute();
$result = $videoSources->fetchAll(PDO::FETCH_ASSOC);

foreach($result as $k=>$v)
{
    print "k is $k\n";
    print "v is $v\n";
}

?>