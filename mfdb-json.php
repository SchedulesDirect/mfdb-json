#!/usr/bin/php

<?php
/*
mfdb-json.php. * This file is a grabber which downloads data from Schedules Direct's JSON service.
Copyright (C) 2012-2014  Robert Kulagowski grabber@schedulesdirect.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/


/*
 * We need a bit of memory to process schedules, so request it right at the beginning.
 */

ini_set("memory_limit", "768M");

require_once "vendor/autoload.php";
require_once "functions.php";
use Guzzle\Http\Client;

$isBeta = TRUE;
$api = "";
$debug = FALSE;
$quiet = FALSE;
$forceDownload = FALSE;
$sdStatus = "";
$printTimeStamp = TRUE;
$maxProgramsToGet = 2000;
$errorWarning = FALSE;
$station = "";
$useScheduleFile = FALSE;
$useProgramFile = FALSE;
$useServiceAPI = FALSE;
$isMythTV = TRUE;
$tz = "UTC";
$usernameFromDB = "";
$passwordFromDB = "";
$stationIDs = array();
$dbWithoutMythtv = FALSE;
$force = FALSE;
$jsonProgramsToRetrieve = array();
$peopleCache = array();
$addToRetryQueue = array();

date_default_timezone_set($tz);
$date = new DateTime();
$todayDate = $date->format("Y-m-d");

$fh_log = fopen("$todayDate.log", "a");
$fh_error = fopen("$todayDate.debug.log", "a");

$baseurl = getBaseURL($isBeta);

$agentString = "mfdb-json.php developer grabber API:$api v$scriptVersion/$scriptDate";

$client = new Guzzle\Http\Client($baseurl);
$client->setUserAgent($agentString);

printMSG("Checking to see if we're running the latest client.");

$serverVersion = checkForClientUpdate($client);

if ($serverVersion == "ERROR")
{
    printMSG("Received error response from server. Exiting.");
    exit;
}

$helpText = <<< eol
The following options are available:
--beta
--help\t\t(this text)
--dbname=\tMySQL database name. (Default: mythconverg)
--dbuser=\tUsername for database access. (Default: mythtv)
--dbpassword=\tPassword for database access. (Default: mythtv)
--dbhost=\tMySQL database hostname. (Default: localhost)
--force\t\tForce download of schedules. (Default: FALSE)
--host=\t\tIP address of the MythTV backend. (Default: localhost)
--nomyth\tDon't execute any MythTV specific functions. (Default: FALSE)
    Must specify --schedule and/or --program
--max=\t\tMaximum number of programs to retrieve per request. (Default:$maxProgramsToGet)
--program\tDownload programs based on programIDs in sd.json.programs.conf file.
--quiet\t\tDon't print to screen; put all output into the logfile.
--station=\tDownload the schedule for a single stationID in your lineup.
--schedule\tDownload schedules based on stationIDs in sd.json.stations.conf file.
--timezone=\tSet the timezone for log file timestamps. See http://www.php.net/manual/en/timezones.php (Default:$tz)
--usedb\t\tUse a database to store data, even if you're not running MythTV. (Default: FALSE)
--version\tPrint version information and exit.
eol;

$longoptions = array("debug", "help", "host::", "dbname::", "dbuser::", "dbpassword::", "dbhost::",
                     "force", "nomyth", "max::", "program", "quiet", "station::", "schedule", "timezone::",
                     "usedb", "version", "x");
$options = getopt("h::", $longoptions);

foreach ($options as $k => $v)
{
    switch ($k)
    {
        case "debug":
            $debug = TRUE;
            break;
        case "help":
        case "h":
            printMSG("$agentString");
            printMSG("$helpText");
            exit;
            break;
        case "dbhost":
            $dbHost = $v;
            break;
        case "dbname":
            $dbName = $v;
            break;
        case "dbpassword":
            $dbPassword = $v;
            break;
        case "dbuser":
            $dbUser = $v;
            break;
        case "host":
            $host = $v;
            break;
        case "force":
            $forceDownload = TRUE;
            break;
        case "nomyth":
            $isMythTV = FALSE;
            break;
        case "max":
            $maxProgramsToGet = $v;
            break;
        case "program":
            $useProgramFile = TRUE;
            break;
        case "quiet":
            $quiet = TRUE;
            break;
        case "schedule":
            $useScheduleFile = TRUE;
            break;
        case "station":
            $station = $v;
            break;
        case "timezone":
            date_default_timezone_set($v);
            break;
        case "usedb":
            $dbWithoutMythtv = TRUE;
            break;
        case "version":
            print "$agentString\n\n";
            exit;
            break;
        case "x":
            $force = TRUE;
            break;
    }
}

if ($serverVersion != $scriptVersion)
{
    printMSG("***Version mismatch.***");
    printMSG("Server version: $serverVersion");
    printMSG("Our version: $scriptVersion");
    if (!$force)
    {
        printMSG("Exiting. Do you need to run 'git pull' to refresh?");
        printMSG("Restart script with --x to ignore mismatch.");
        exit;
    }
    else
    {
        printMSG("Continuing because of --x parameter.");
    }
}

if ($knownToBeBroken AND !$force)
{
    print "This version is known to be broken and force option not specified. Exiting.\n";
    exit;
}

printMSG("$agentString");

$dlSchedTempDir = tempdir("schedules");
printMSG("Temp directory for Schedules is $dlSchedTempDir");
$dlProgramTempDir = tempdir("programs");
printMSG("Temp directory for Programs is $dlProgramTempDir");

if ($isMythTV)
{
    if (!isset($dbHost) AND !isset($dbName) AND !isset($dbUser) and !isset($dbPassword))
    {
        list($dbHost, $dbName, $dbUser, $dbPassword) = getLoginFromFiles();
        if ($dbHost == "NONE")
        {
            $dbUser = "mythtv";
            $dbPassword = "mythtv";
            $dbHost = "localhost";
            $dbName = "mythconverg";
            $host = "localhost";
        }
    }
}

if ($isMythTV OR $dbWithoutMythtv)
{
    printMSG("Connecting to database.");
    try
    {
        $dbh = new PDO("mysql:host=$dbHost;dbname=$dbName;charset=utf8", $dbUser, $dbPassword,
            array(PDO::ATTR_PERSISTENT => true));
        $dbh->exec("SET CHARACTER SET utf8");
        $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    } catch (PDOException $e)
    {
        debugMSG("Exception with PDO: " . $e->getMessage());
        exit;
    }

    $useServiceAPI = checkForServiceAPI();

    $userLoginInformation = setting("SchedulesDirectLogin");

    if ($userLoginInformation !== FALSE)
    {
        $responseJSON = json_decode($userLoginInformation, TRUE);
        $usernameFromDB = $responseJSON["username"];
        $passwordFromDB = $responseJSON["password"];
    }
    else
    {
        printMSG("FATAL: Could not read Schedules Direct login information from settings table.");
        printMSG("Did you run the sd-utility.php program yet?");
        exit;
    }
}
else
{
    if (file_exists("sd.json.conf"))
    {
        $userLoginInformation = file("sd.json.conf");
        $responseJSON = json_decode($userLoginInformation[0], TRUE);
        $usernameFromDB = $responseJSON["username"];
        $passwordFromDB = $responseJSON["password"];
    }
    else
    {
        printMSG("FATAL: Could not read Schedules Direct login information from sd.json.conf file.");
        printMSG("Did you run the sd-utility.php program yet?");
        exit;
    }
}

$globalStartTime = time();
$globalStartDate = new DateTime();

if ($isMythTV)
{
    if ($station == "")
    {
        printMSG("Retrieving list of channels to download.");

        $stmt = $dbh->prepare("SELECT sourceID FROM videosource WHERE xmltvgrabber='schedulesdirect2'");
        $stmt->execute();
        $sources = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $stmt = $dbh->prepare("SELECT CAST(xmltvid AS UNSIGNED) FROM channel WHERE visible=TRUE
AND xmltvid != '' AND xmltvid > 0 GROUP BY xmltvid");
        $stmt->execute();
        $stationIDs = $stmt->fetchAll(PDO::FETCH_COLUMN);

        if (!count($stationIDs))
        {
            printMSG("Error: no channels retrieved from database. Check channel table.");
            exit;
        }
    }
    else
    {
        printMSG("Downloading data only for $station");
        $stationIDs[] = $station;
    }
}
else
{
    if (file_exists("sd.json.stations.conf") AND $useScheduleFile)
    {
        $stationIDs = file("sd.json.stations.conf");
    }
    elseif ($station != "")
    {
        $stationIDs[] = $station;
    }
    elseif (!$useProgramFile)
    {
        printMSG("Nothing to do: did not find file sd.json.stations.conf or no station to retrieve passed as parameter.");
        exit;
    }
}

printMSG("Logging into Schedules Direct.");
$token = getToken($usernameFromDB, sha1($passwordFromDB));

if ($token == "ERROR")
{
    printMSG("Got error when attempting to retrieve token from Schedules Direct.");
    if ($isMythTV)
    {
        printMSG("Check username / password in settings table.");
    }
    else
    {
        printMSG("Check username / password in sd.json.conf file.");
    }
    exit;
}

printMSG("Retrieving server status message.");

$response = updateStatus();

if ($response == "No new data on server." AND $forceDownload === FALSE)
{
    $statusMessage = "No new programs to retrieve.";
}

if ($token != "ERROR" AND $response != "ERROR")
{
    if (count($stationIDs))
    {
        $jsonProgramsToRetrieve = getSchedules($stationIDs);

        if ($jsonProgramsToRetrieve == "ERROR")
        {
            printMSG("Could not get schedules. Exiting.");
            exit;
        }

        while (count($addToRetryQueue))
        {
            /*
             * Recursive; hopefully we don't get a runaway.
             */
            $bar = array();
            printMSG("Retrying schedule fetch for the following:");
            $forceDownload = TRUE;
            foreach ($addToRetryQueue as $k => $v)
            {
                $bar[] = $k;
            }
            $foo = getSchedules($bar);
            $jsonProgramsToRetrieve = array_merge($jsonProgramsToRetrieve, $foo);
        }

        fetchPrograms($jsonProgramsToRetrieve);
    }
}
else
{
    debugMSG("Error connecting to Schedules Direct.");
    $statusMessage = "Error connecting to Schedules Direct.";
}

if ($isMythTV)
{
    if (count($jsonProgramsToRetrieve) OR $forceDownload === TRUE)
    {
        insertJSON($jsonProgramsToRetrieve);
        insertSchedule();
        $statusMessage = "Successful.";
    }
    else
    {
        $statusMessage = "No new programs to retrieve.";
    }
}
else
{
    if (file_exists("sd.json.programs.conf") AND $useProgramFile)
    {
        $jsonProgramsToRetrieve = file("sd.json.programs.conf");
        fetchPrograms($jsonProgramsToRetrieve);
        $statusMessage = "Successful.";
    }
    else
    {
        printMSG("Nothing to do: Not running MythTV and did not find sd.json.programs.conf");
        exit;
    }
}

printMSG("Status:$statusMessage");

$globalStartTime = date("Y-m-d H:i:s", $globalStartTime);
$globalEndTime = date("Y-m-d H:i:s");

printMSG("Global. Start Time:$globalStartTime");
printMSG("Global. End Time:  $globalEndTime");
$globalSinceStart = $globalStartDate->diff(new DateTime());
if ($globalSinceStart->h)
{
    printMSG($globalSinceStart->h . " hour ");
}
printMSG($globalSinceStart->i . " minutes " . $globalSinceStart->s . " seconds.");

if ($isMythTV)
{
    printMSG("Updating status.");

    $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunStart' AND hostname IS NULL");
    $stmt->execute(array("data" => $globalStartTime));

    $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunEnd' AND hostname IS NULL");
    $stmt->execute(array("data" => $globalEndTime));

    $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunStatus' AND hostname IS NULL");
    $stmt->execute(array("data" => $statusMessage));

    printMSG("Sending reschedule request to mythbackend.");

    exec("mythutil --resched");
}

printMSG("Done.");

if ($errorWarning)
{
    debugMSG("NOTE! Errors encountered during processing. Check logs.");
}

exit;

function getSchedules($stationIDsToFetch)
{
    global $client;
    global $dbh;
    global $token;
    global $dlSchedTempDir;
    global $quiet;
    global $debug;
    global $isMythTV;
    global $forceDownload;
    global $addToRetryQueue;

    $jsonProgramsToRetrieve = array();
    $requestArray = array();
    $bar = array();

    $dbProgramCache = array();
    $response = "";

    $downloadedStationIDs = array();
    $serverScheduleMD5 = array();

    if ($debug)
    {
        print "Station array:\n";
        var_dump($stationIDsToFetch);
    }

    if (count($stationIDsToFetch) == 0)
    {
        print "1. No schedules to fetch.\n";
        return("");
    }

    while (list(, $sid) = each($stationIDsToFetch))
    {
        $requestArray[] = array("stationID" => $sid, "days" => 13);
    }

    if (count($requestArray) == 0)
    {
        print "2. No schedules to fetch.\n"; // Should never hit this.
        return("");
    }

    if ($debug)
    {
        print "Request array:\n";
        var_dump($requestArray);
    }

    if (!$forceDownload)
    {
        printMSG("Determining if there are updated schedules.");

        $errorCount = 0;

        do
        {
            $response = NULL;
            try
            {
                $response = $client->post("schedules/md5",
                    array("token"           => $token,
                          "Accept-Encoding" => "deflate,gzip"),
                    json_encode($requestArray))->send();
            } catch (Guzzle\Http\Exception\BadResponseException $e)
            {
                $response = NULL;
                switch ($e->getCode())
                {
                    case 400:
                        return ("ERROR");
                        break;
                    case 504:
                        $errorCount++;
                        printMSG("Got timeout from gateway; retrying in 10 seconds.");
                        sleep(10); // Hammering away isn't going to make things better.
                        break;
                    default:
                        print "Unhandled BadResponseException in getSchedules MD5.\n";
                        print "Send the following to grabber@schedulesdirect.org\n";
                        var_dump($e);
                        break;
                }
            } catch (Exception $e)
            {
                print "Other exception in getSchedules.\n";
                var_dump($e);
                exit;
            }

            if (!is_null($response))
            {
                break;
            }
        } while ($errorCount < 10);

        if ($errorCount == 10)
        {
            printMSG("Fatal error trying to get MD5 for schedules.");

            return ("ERROR");
        }

        $schedulesDirectMD5s = $response->json();

        if ($debug)
        {
            print "Schedules Direct MD5's\n";
            var_dump($schedulesDirectMD5s);
        }

        $getLocalCache = $dbh->prepare("SELECT stationID,md5 FROM SDschedule");
        $getLocalCache->execute();
        $localMD5 = $getLocalCache->fetchAll(PDO::FETCH_KEY_PAIR);

        if ($debug)
        {
            print "Local MD5\n";
            var_dump($localMD5);
        }

        while (list($stationID, $data) = each($schedulesDirectMD5s))
        {
            if ($debug)
            {
                print "sid: $stationID\n";
                print "data:\n";
                var_dump($data);
            }

            if (isset($localMD5[$stationID]))
            {
                foreach ($data as $item)
                {
                    if ($debug)
                    {
                        print "item is\n";
                        var_dump($item);
                    }

                    if ($item["days"] == 13)
                    {
                        if ($localMD5[$stationID] != $item["md5"])
                        {
                            $bar[] = array("stationID" => $stationID, "days" => 13);
                            continue;
                        }
                    }
                }
            }
            else
            {
                $bar[] = array("stationID" => $stationID, "days" => 13);
            }
        }
    }
    else
    {
        $bar = $requestArray;
    }

    if ($debug)
    {
        print "bar is now\n";
        var_dump($bar);
    }

    if (count($bar) == 0)
    {
        printMSG("No updated schedules.");

        return ($jsonProgramsToRetrieve);
    }

    printMSG(count($bar) . " schedules to download.");

    $errorCount = 0;

    do
    {
        $response = NULL;
        try
        {
            $response = $client->post("schedules", array("token" => $token, "Accept-Encoding" => "deflate,gzip"),
                json_encode($bar))->send();
        } catch (Guzzle\Http\Exception\BadResponseException $e)
        {
            $response = NULL;
            switch ($e->getCode())
            {
                case 400:
                    return ("ERROR");
                    break;
                case 504:
                    $errorCount++;
                    printMSG("Got timeout from gateway; retrying in 10 seconds.");
                    sleep(10); // Hammering away isn't going to make things better.
                    break;
                default:
                    print "Unhandled BadResponseException in getSchedules.\n";
                    print "Send the following to grabber@schedulesdirect.org\n";
                    var_dump($e);
                    break;
            }
        } catch (Exception $e)
        {
            print "Other exception in getSchedules.\n";
            var_dump($e);
            exit;
        }

        if (!is_null($response))
        {
            break;
        }
    } while ($errorCount < 10);

    if ($errorCount == 10)
    {
        printMSG("Fatal error trying to get schedules.");

        return ("ERROR");
    }

    $schedules = $response->getBody();

    /*
     * Keep a copy for troubleshooting.
     */

    printMSG("Writing to $dlSchedTempDir/schedule.json");

    file_put_contents("$dlSchedTempDir/schedule.json", $schedules, FILE_APPEND);

    if (!$isMythTV)
    {
        return ("");
    }

    $updateLocalMD5 = $dbh->prepare("INSERT INTO SDschedule(stationID, md5) VALUES(:sid, :md5)
    ON DUPLICATE KEY UPDATE md5=:md5");

    $f = file("$dlSchedTempDir/schedule.json", FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

    foreach ($f as $json)
    {
        $item = json_decode($json, TRUE);

        if (isset($item["stationID"]))
        {
            $stationID = $item["stationID"];
        }
        else
        {
            printMSG("Fatal: No stationID? Send the following to grabber@schedulesdirect.org");
            printMSG($json);
            continue;
        }

        if (isset($item["code"]))
        {
            if ($item["code"] == 7000)
            {
                if (!isset($addToRetryQueue[$stationID]))
                {
                    $addToRetryQueue[$stationID] = 1;
                }
                else
                {
                    $addToRetryQueue[$stationID]++;
                }

                if ($addToRetryQueue[$stationID] == 10)
                {
                    unset($addToRetryQueue[$stationID]); // Permanent error.
                    printMSG("Permanent error attempting to fetch schedule for $stationID");
                }

                continue;
            }
        }

        $downloadedStationIDs[] = $stationID;

        printMSG("Parsing schedule for stationID:$stationID");

        if (!array_key_exists("programs", $item))
        {
            printMSG("WARNING: JSON does not contain any program elements.");
            printMSG("Send the following to grabber@schedulesdirect.org\n\n");
            var_dump($item);
            printMSG("$json\n\n");
            exit;
        }

        $md5 = $item["metadata"]["md5"];
        $updateLocalMD5->execute(array("sid" => $stationID, "md5" => $md5));

        foreach ($item["programs"] as $programData)
        {
            if (array_key_exists("md5", $programData))
            {
                $serverScheduleMD5[$programData["md5"]] = $programData["programID"];
            }
            else
            {
                $quiet = FALSE;
                printMSG("FATAL ERROR: no MD5 value for program.");
                printMSG("Send the following to grabber@schedulesdirect.org\n");
                printMSG("s:$stationID\n\njson:$json\n\nitem\n\n" . print_r($item, TRUE) . "\n\n");
                continue;
            }
        }
    }

    printMSG("There are " . count($serverScheduleMD5) . " programIDs in the upcoming schedule.");
    printMSG("Retrieving existing MD5 values from local cache.");

    /*
     * We're going to figure out which programIDs we need to download.
     */

    $stmt = $dbh->prepare("SELECT md5, programID FROM SDprogramCache");
    $stmt->execute();
    $dbProgramCache = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

    $jsonProgramsToRetrieve = array_diff_key($serverScheduleMD5, $dbProgramCache);

    if ($debug)
    {
        /*
         * One user is reporting that after a run, a second run immediately afterwards still has programs that need
         * to be downloaded. That shouldn't happen, so dump the array to the log file for analysis.
         */
        printMSG("dbProgramCache is");
        printMSG(print_r($dbProgramCache, TRUE));
        printMSG("serverScheduleMD5 is");
        printMSG(print_r($serverScheduleMD5, TRUE));
        printMSG("jsonProrgamstoRetrieve is");
        printMSG(print_r($jsonProgramsToRetrieve, TRUE));
    }

    return ($jsonProgramsToRetrieve);
}

function fetchPrograms($jsonProgramsToRetrieve)
{
    global $client;
    global $token;
    global $maxProgramsToGet;
    global $dlProgramTempDir;

    $toRetrieveTotal = count($jsonProgramsToRetrieve);

    /*
     * Now we've got an array of programIDs that we need to download in $toRetrieve,
     * either because we didn't have them, or they have different md5's.
     */

    if ($toRetrieveTotal == 0)
    {
        printMSG("No programs to fetch.");
    }
    else
    {
        printMSG("Need to download $toRetrieveTotal new or updated programs.");

        if ($toRetrieveTotal > 10000)
        {
            printMSG("Requesting more than 10000 programs. Please be patient.");
        }

        printMSG("Maximum number of programs we're downloading per call: $maxProgramsToGet");

        if (count($jsonProgramsToRetrieve))
        {
            $totalChunks = intval($toRetrieveTotal / $maxProgramsToGet);

            $counter = 0;

            for ($i = 0; $i <= $totalChunks; $i++)
            {
                printMSG("Retrieving chunk " . ($i + 1) . " of " . ($totalChunks + 1) . ".");
                $startOffset = $i * $maxProgramsToGet;
                $chunk = array_slice($jsonProgramsToRetrieve, $startOffset, $maxProgramsToGet);

                $counter += count($chunk);

                $schedulesDirectPrograms = $client->post("programs",
                    array("token"           => $token,
                          "Accept-Encoding" => "deflate,gzip"),
                    json_encode($chunk));
                $response = $schedulesDirectPrograms->send();

                $schedulesDirectPrograms = $response->getBody();

                file_put_contents("$dlProgramTempDir/programs." . substr("00$i", -2) . ".json", $schedulesDirectPrograms);
            }
        }
    }

    return ($jsonProgramsToRetrieve);
}

function insertJSON(array $jsonProgramsToRetrieve)
{
    global $dbh;
    global $dlProgramTempDir;
    global $debug;

    $insertJSON = $dbh->prepare("INSERT INTO SDprogramCache(programID,md5,json)
            VALUES (:programID,:md5,:json)
            ON DUPLICATE KEY UPDATE md5=:md5, json=:json");

    $insertPersonSD = $dbh->prepare("INSERT INTO SDpeople(personID,name) VALUES(:personID, :name)");
    $updatePersonSD = $dbh->prepare("UPDATE SDpeople SET name=:name WHERE personID=:personID");

    $insertPersonMyth = $dbh->prepare("INSERT INTO people(name) VALUES(:name)");

    $insertCreditSD = $dbh->prepare("INSERT INTO SDcredits(personID,programID,role)
    VALUES(:personID,:pid,:role)");

    $insertProgramGenresSD = $dbh->prepare("INSERT INTO SDprogramgenres(programID,relevance,genre)
    VALUES(:pid,:relevance,:genre) ON DUPLICATE KEY UPDATE genre=:genre");

    $getPeople = $dbh->prepare("SELECT name,person FROM people");
    $getPeople->execute();
    $peopleCacheMyth = $getPeople->fetchAll(PDO::FETCH_KEY_PAIR);

    $getPeople = $dbh->prepare("SELECT personID,name FROM SDpeople");
    $getPeople->execute();
    $peopleCacheSD = $getPeople->fetchAll(PDO::FETCH_KEY_PAIR);

    $getCredits = $dbh->prepare("SELECT CONCAT(personID,'-',programID,'-',role) FROM SDcredits");
    $getCredits->execute();
    $creditCache = $getCredits->fetchAll(PDO::FETCH_COLUMN);

    $creditCache = array_flip($creditCache);

    $counter = 0;
    $total = count($jsonProgramsToRetrieve);
    printMSG("Performing inserts of JSON data.");

    $dbh->beginTransaction();

    foreach (glob("$dlProgramTempDir/*.json") as $jsonFileToProcess)
    {
        $a = file($jsonFileToProcess);

        while (list(, $item) = each($a))
        {
            $counter++;
            if ($counter % 100 == 0)
            {
                printMSG("$counter / $total             \r");
                $dbh->commit();
                $dbh->beginTransaction();
            }

            if ($item == "")
            {
                continue;
            }

            $jsonProgram = json_decode($item, TRUE);

            if (json_last_error())
            {
                debugMSG("*** ERROR: JSON decode error $jsonFileToProcess");
                debugMSG(print_r($item, TRUE));
                continue;
            }

            if (isset($jsonProgram["code"]))
            {
                /*
                 * Probably not good. :(
                 */

                if ($jsonProgram["code"] == 6000)
                {
                    print "FATAL ERROR: server couldn't find programID?\n";
                    print "$item\n";
                    continue;
                }
            }

            if (isset($jsonProgram["programID"]))
            {
                $pid = $jsonProgram["programID"];
            }
            else
            {
                print "FATAL ERROR: No programID?\n";
                print "$item\n";
                continue;
            }

            if (isset($jsonProgram["md5"]))
            {
                $md5 = $jsonProgram["md5"];
            }
            else
            {
                print "FATAL ERROR: No md5?\n";
                print "$item\n";
                continue;
            }

            $insertJSON->execute(array("programID" => $pid, "md5" => $md5,
                                       "json"      => $item));

            $skipPersonID = FALSE;

            if (isset($jsonProgram["genres"]))
            {
                $relevance = 0; // We're going to assume that at some point we might do something with relevance.
                foreach ($jsonProgram["genres"] as $g)
                {
                    switch ($g)
                    {
                        case "Adults only":
                        case "Erotic":
                            $skipPersonID = TRUE;
                            break;
                    }
                    $insertProgramGenresSD->execute(array("pid"       => $pid,
                                                          "relevance" => ++$relevance, "genre" => $g));
                }
            }

            if ($skipPersonID === FALSE)
            {
                if (isset($jsonProgram["cast"]))
                {
                    foreach ($jsonProgram["cast"] as $credit)
                    {
                        if (!isset($credit["role"]))
                        {
                            printMSG("No role?");
                            printMSG("Send the following to grabber@schedulesdirect.org\n\n");
                            printMSG("Program: $pid. No role in cast.");
                            var_dump($jsonProgram["cast"]);
                            exit;
                        }
                        else
                        {
                            $role = $credit["role"];
                        }

                        if (!isset($credit["personId"]))
                        {
                            printMSG("$jsonFileToProcess:$pid does not have a personId.");
                            $debug = TRUE; // Set it to true
                            continue;
                        }
                        $personID = $credit["personId"];

                        $name = $credit["name"];

                        if (!array_key_exists($personID, $peopleCacheSD))
                        {
                            $insertPersonSD->execute(array("personID" => (int)$personID, "name" => $name));
                            $peopleCacheSD[$personID] = $name;
                        }

                        if ($peopleCacheSD[$personID] != $name)
                        {
                            $updatePersonSD->execute(array("personID" => (int)$personID, "name" => $name));
                        }

                        if (!isset($peopleCacheMyth[$name]))
                        {
                            $insertPersonMyth->execute(array("name" => $name));
                            $id = $dbh->lastInsertId();
                            $peopleCacheMyth[$name] = $id;
                        }

                        if (!isset($creditCache["$personID-$pid-$role"]))
                        {
                            $insertCreditSD->execute(array("personID" => (int)$personID, "pid" => $pid,
                                                           "role"     => $role));
                            $creditCache["$personID-$pid-$role"] = 1;
                        }
                    }
                }

                if (isset($jsonProgram["crew"]))
                {
                    foreach ($jsonProgram["crew"] as $credit)
                    {
                        if (!isset($credit["role"]))
                        {
                            printMSG("No role?");
                            printMSG("Send the following to grabber@schedulesdirect.org\n\n");
                            printMSG("Program: $pid. No role in crew.");
                            var_dump($jsonProgram["crew"]);
                            exit;
                        }
                        else
                        {
                            $role = $credit["role"];
                        }

                        if (!isset($credit["personId"]))
                        {
                            printMSG("$jsonFileToProcess:$pid does not have a personId.");
                            $debug = TRUE;
                            continue;
                        }
                        $personID = $credit["personId"];
                        $name = $credit["name"];

                        if (!array_key_exists($personID, $peopleCacheSD))
                        {
                            $insertPersonSD->execute(array("personID" => (int)$personID, "name" => $name));
                            $peopleCacheSD[$personID] = $name;
                        }

                        if ($peopleCacheSD[$personID] != $name)
                        {
                            $updatePersonSD->execute(array("personID" => (int)$personID, "name" => $name));
                        }

                        if (!isset($peopleCacheMyth[$name]))
                        {
                            $insertPersonMyth->execute(array("name" => $name));
                            $id = $dbh->lastInsertId();
                            $peopleCacheMyth[$name] = $id;
                        }

                        if (!isset($creditCache["$personID-$pid-$role"]))
                        {
                            $insertCreditSD->execute(array("personID" => (int)$personID, "pid" => $pid,
                                                           "role"     => $role));
                            $creditCache["$personID-$pid-$role"] = 1;
                        }
                    }
                }
            }

            if (isset($jsonProgram["genres"]))
            {
                foreach ($jsonProgram["genres"] as $relevance => $genre)
                {

                }
            }
        }

        if ($debug === FALSE)
        {
            unlink($jsonFileToProcess);
        }
    }

    if ($debug === FALSE)
    {
        rmdir("$dlProgramTempDir");
    }

    $dbh->commit();

    printMSG("Completed local database program updates.");
}

function insertSchedule()
{
    global $dbh;
    global $dlSchedTempDir;
    global $peopleCache;
    global $debug;
    global $errorWarning;

    $existingRoleTypesInMyth = array("actor", "director", "producer", "executive_producer", "writer", "guest_star",
                                     "host", "adapter", "presenter", "commentator", "guest");
    $existingRoleTypesInMyth = array_flip($existingRoleTypesInMyth);

    if (!count($peopleCache))
    {
        /*
         * People cache array is empty, so read it in.
         */
        $getPeople = $dbh->prepare("SELECT name,person FROM people");
        $getPeople->execute();
        $peopleCache = $getPeople->fetchAll(PDO::FETCH_KEY_PAIR);
    }

    $roleTable = array();

    printMSG("Inserting schedules.");

    $dbh->exec("DROP TABLE IF EXISTS t_program");
    $dbh->exec("CREATE TABLE t_program LIKE program");
    $dbh->exec("INSERT t_program SELECT * FROM program");

    $dbh->exec("DROP TABLE IF EXISTS t_credits");
    $dbh->exec("CREATE TABLE t_credits LIKE credits");

    $dbh->exec("DROP TABLE IF EXISTS t_programrating");
    $dbh->exec("CREATE TABLE t_programrating LIKE programrating");

    $stmt = $dbh->prepare("SELECT data FROM settings WHERE value='DBSchemaVer'");
    $stmt->execute();
    $dbSchema = $stmt->fetchColumn();

    if ($dbSchema > "1318")
    {
        /*
         * program table will have season and episode.
         */
        $insertSchedule = $dbh->prepare("INSERT INTO t_program(chanid,starttime,endtime,title,subtitle,description,
    category,category_type,airdate,stars,previouslyshown,stereo,subtitled,hdtv,closecaptioned,partnumber,parttotal,
    seriesid,originalairdate,showtype,colorcode,syndicatedepisodenumber,programid,generic,listingsource,first,last,
    audioprop,subtitletypes,videoprop,season,episode)
    VALUES(:chanid,:starttime,:endtime,:title,:subtitle,:description,:category,:category_type,:airdate,:stars,
    :previouslyshown,:stereo,:subtitled,:hdtv,:closecaptioned,:partnumber,:parttotal,
    :seriesid,:originalairdate,:showtype,:colorcode,:syndicatedepisodenumber,:programid,:generic,:listingsource,
    :first,:last,:audioprop,:subtitletypes,:videoprop,:season,:episode)");
    }
    else
    {
        /*
         * No season / episode insert.
         */
        $insertSchedule = $dbh->prepare("INSERT INTO t_program(chanid,starttime,endtime,title,subtitle,description,
    category,category_type,airdate,stars,previouslyshown,stereo,subtitled,hdtv,closecaptioned,partnumber,parttotal,
    seriesid,originalairdate,showtype,colorcode,syndicatedepisodenumber,programid,generic,listingsource,first,last,
    audioprop,subtitletypes,videoprop)
    VALUES(:chanid,:starttime,:endtime,:title,:subtitle,:description,:category,:category_type,:airdate,:stars,
    :previouslyshown,:stereo,:subtitled,:hdtv,:closecaptioned,:partnumber,:parttotal,
    :seriesid,:originalairdate,:showtype,:colorcode,:syndicatedepisodenumber,:programid,:generic,:listingsource,
    :first,:last,:audioprop,:subtitletypes,:videoprop)");
    }

    $insertCreditMyth = $dbh->prepare("INSERT INTO t_credits(person, chanid, starttime, role)
    VALUES(:person,:chanid,:starttime,:role)");

    $insertProgramRatingMyth = $dbh->prepare("INSERT INTO t_programrating(chanid, starttime, system, rating)
    VALUES(:chanid,:starttime,:system,:rating)");

    $getExistingChannels = $dbh->prepare("SELECT chanid,sourceid, CAST(xmltvid AS UNSIGNED) AS xmltvid FROM channel
WHERE visible = 1 AND xmltvid != '' AND xmltvid > 0 ORDER BY xmltvid");
    $getExistingChannels->execute();
    $existingChannels = $getExistingChannels->fetchAll(PDO::FETCH_ASSOC);

    $getProgramInformation = $dbh->prepare("SELECT json FROM SDprogramCache WHERE programID =:pid");

    $deleteExistingSchedule = $dbh->prepare("DELETE FROM t_program WHERE chanid = :chanid");

    $scheduleTemp = file("$dlSchedTempDir/schedule.json");

    /*
     * Move the schedule into an associative array so that we can process the items per stationID. We're going to
     * decode this once so that we're not doing it over and over again. May increase memory footprint though.
     */

    $scheduleJSON = array();
    while (list(, $item) = each($scheduleTemp))
    {
        $tempJSON = json_decode($item, TRUE);
        $stationID = $tempJSON["stationID"];
        $scheduleJSON[$stationID] = $tempJSON["programs"];
    }

    /*
     * Now that we're done, reset the array to empty to free up some memory.
     */
    $scheduleTemp = array();

    while (list(, $item) = each($existingChannels))
    {
        $chanID = $item["chanid"];
        $sourceID = $item["sourceid"];
        $stationID = $item["xmltvid"];

        if (!array_key_exists($stationID, $scheduleJSON))
        {
            continue; // If we don't have an updated schedule for the stationID, there's nothing to do.
        }

        printMSG("Inserting schedule for chanid:$chanID sourceid:$sourceID stationID:$stationID");

        $deleteExistingSchedule->execute(array("chanid" => $chanID));

        $dbh->beginTransaction();

        reset($scheduleJSON[$stationID]);
        while (list(, $schedule) = each($scheduleJSON[$stationID]))
        {
            /*
             * Pre-declare what we'll be using to quiet warning about unused variables.
             */

            $isNew = FALSE;
            $previouslyshown = FALSE;

            $title = "";
            $ratingSystem = "";
            $rating = "";
            $movieYear = "";
            $starRating = 0;
            $colorCode = "";
            $syndicatedEpisodeNumber = "";
            $showType = "";
            $oad = NULL;
            $audioprop = "";
            $subtitleTypes = "";
            $videoProperties = "";
            $partNumber = 0;
            $numberOfParts = 0;
            $season = 0;
            $episode = 0;

            /*
             * The various audio properties
             */
            $isClosedCaption = FALSE;
            $dvs = FALSE;
            $dolbyType = NULL;
            $dubbed = FALSE;
            $isStereo = FALSE;
            $isSubtitled = FALSE;
            $isSurround = FALSE;

            /*
             * Video properties
             */
            $is3d = FALSE;
            $isEnhancedResolution = FALSE; // Better than SD, not as good as HD
            $isHDTV = FALSE;
            $isLetterboxed = FALSE;
            $isSDTV = FALSE;

            /*
             * Optional booleans, some of which aren't used by MythTV
             */

            $cableInTheClassroom = FALSE; // Not used by MythTV
            $catchupProgram = FALSE; // Used in the UK; indicates that a program is available online
            $continuedProgram = FALSE; // Continued from a previous broadcast
            $isEducational = FALSE;
            $joinedInProgress = FALSE;
            $leftInProgress = FALSE;
            $isPremiere = FALSE; // First showing of a movie or TV series
            $programBreak = FALSE; // Program stops and will continue later; typically only found in UK
            $repeat = FALSE;
            $isSigned = FALSE; // supplemented with a person signing for the hearing impaired
            $subjectToBlackout = FALSE;
            $timeApproximate = FALSE;
            $isPremiereOrFinale = FALSE; // Season Premiere | Season Finale | Series Premiere | Series Finale
            $liveOrTapeDelay = NULL;

            /*
             * These are updated in another part of mfdb?
             */
            $isFirst = 0;
            $isLast = 0;

            $programID = $schedule["programID"];
            $getProgramInformation->execute(array("pid" => $programID));
            $pj = $getProgramInformation->fetchColumn();
            $programJSON = json_decode($pj, TRUE);

            if (json_last_error())
            {
                debugMSG("Error retrieving / decoding $programID from local database. Raw data was:");
                debugMSG(print_r($pj, TRUE));
                $errorWarning = TRUE;
                continue;
            }

            $md5 = $schedule["md5"];
            $air_datetime = $schedule["airDateTime"];
            $duration = $schedule["duration"];

            $programStartTimeMyth = str_replace("T", " ", $air_datetime);
            $programStartTimeMyth = rtrim($programStartTimeMyth, "Z");
            $programEndTimeMyth = gmdate("Y-m-d H:i:s", strtotime("$air_datetime + $duration seconds"));

            if (isset($schedule["audioProperties"]))
            {
                foreach ($schedule["audioProperties"] as $ap)
                {
                    if ($ap == "cc")
                    {
                        $isClosedCaption = TRUE;
                    }

                    if ($ap == "dvs")
                    {
                        $dvs = TRUE;
                    }

                    if ($ap == "Dolby")
                    {
                        $dolbyType = "dolby";
                    }

                    if ($ap == "DD")
                    {
                        $dolbyType = "Dolby Digital";
                    }

                    if ($ap == "DD 5.1")
                    {
                        $dolbyType = "Dolby Digital 5.1";
                    }

                    if ($ap == "dubbed")
                    {
                        $dubbed = TRUE;
                    }

                    if ($ap == "stereo")
                    {
                        $isStereo = TRUE;
                    }

                    if ($ap == "subtitled")
                    {
                        $isSubtitled = TRUE;
                    }

                    if ($ap == "surround")
                    {
                        $isSurround = TRUE;
                    }
                }
            }

            if (isset($schedule["videoProperties"]))
            {
                foreach ($schedule["videoProperties"] as $vp)
                {
                    if ($vp == "3d")
                    {
                        $is3d = TRUE;
                    }

                    if ($vp == "enhanced")
                    {
                        $isEnhancedResolution = TRUE;
                    }

                    if ($vp == "hdtv")
                    {
                        $isHDTV = TRUE;
                    }

                    if ($vp == "letterbox")
                    {
                        $isLetterboxed = TRUE;
                    }

                    if ($vp == "sdtv")
                    {
                        $isSDTV = TRUE; //Not used in MythTV
                    }
                }
            }

            if (isset($schedule["isPremiereOrFinale"]))
            {
                switch ($schedule["isPremiereOrFinale"])
                {
                    case "Series Premiere":
                    case "Season Premiere":
                        $isFirst = TRUE;
                        break;
                    case "Series Finale":
                    case "Season Finale":
                        $isLast = TRUE;
                        break;
                }
            }
            else
            {
                $isFirst = FALSE;
                $isLast = FALSE;
            }

            if (isset($schedule["contentRating"]))
            {
                foreach ($schedule["contentRating"] as $r)
                {
                    if ($r["body"] == "USA Parental Rating")
                    {
                        $ratingSystem = "V-CHIP";
                        $rating = $r["code"];
                    }
                    if ($r["body"] == "Motion Picture Association of America")
                    {
                        $ratingSystem = "MPAA";
                        $rating = $r["code"];
                    }
                }
            }

            /*
             * Yes, there may be two different places that ratings exist. But the schedule rating should be used if
             * it exists, because a program may have different version, some edited for language, etc,
             * and that's indicated in the schedule.
             */
            else
            {
                if (isset($programJSON["contentRating"]))
                {
                    foreach ($programJSON["contentRating"] as $r)
                    {
                        if ($r["body"] == "USA Parental Rating")
                        {
                            $ratingSystem = "V-CHIP";
                            $rating = $r["code"];
                        }
                        if ($r["body"] == "Motion Picture Association of America")
                        {
                            $ratingSystem = "MPAA";
                            $rating = $r["code"];
                        }
                    }
                }
            }

            /*
             * Boolean types
             */

            if (isset($schedule["new"]))
            {
                $isNew = TRUE;
                $previouslyshown = FALSE;
            }
            else
            {
                $isNew = FALSE;
                $previouslyshown = TRUE;
            }

            /*
             * Shouldn't be "new" and "repeat"
             */

            if (isset($schedule["repeat"]))
            {
                if ($isNew)
                {
                    debugMSG("*** WARNING sid:$stationID pid:$programID has 'new' and 'repeat' set. Open SD ticket:");
                    debugMSG(print_r($schedule, TRUE));
                    $errorWarning = TRUE;
                }
                else
                {
                    $isNew = FALSE;
                    $previouslyshown = TRUE;
                }
            }

            if (isset($schedule["cableInTheClassroom"]))
            {
                $cableInTheClassroom = TRUE;
            }

            if (isset($schedule["catchup"]))
            {
                $catchupProgram = TRUE;
            }

            if (isset($schedule["continued"]))
            {
                $continuedProgram = TRUE;
            }

            if (isset($schedule["educational"]))
            {
                $isEducational = TRUE;
            }

            if (isset($schedule["joinedInProgress"]))
            {
                $joinedInProgress = TRUE;
            }

            if (isset($schedule["leftInProgress"]))
            {
                $leftInProgress = TRUE;
            }

            if (isset($schedule["premiere"]))
            {
                $isPremiere = TRUE;
            }

            if (isset($schedule["programBreak"]))
            {
                $programBreak = TRUE;
            }

            if (isset($schedule["signed"]))
            {
                $isSigned = TRUE;
            }

            if (isset($schedule["subjectToBlackout"]))
            {
                $subjectToBlackout = TRUE;
            }

            if (isset($schedule["timeApproximate"]))
            {
                $timeApproximate = TRUE;
            }

            if (isset($schedule["liveTapeDelay"]))
            {
                $liveOrTapeDelay = $schedule["liveTapeDelay"];
            }

            $title = $programJSON["titles"][0]["title120"];

            if ($title == NULL OR $title == "")
            {
                debugMSG("FATAL ERROR: Empty title? $programID");
                continue;
            }

            if (isset($programJSON["episodeTitle150"]))
            {
                $subTitle = $programJSON["episodeTitle150"];
            }
            else
            {
                $subTitle = "";
            }

            if (isset($programJSON["descriptions"]["description1000"]))
            {
                $description = $programJSON["descriptions"]["description1000"][0]["description"];
            }
            else
            {
                $description = "";
            }

            if (isset($programJSON["genres"]))
            {
                $category = $programJSON["genres"][0];
            }
            else
            {
                $category = "";
            }

            if (isset($programJSON["metadata"]))
            {
                foreach ($programJSON["metadata"] as $md)
                {
                    if (isset($md["Tribune"]))
                    {
                        $season = $md["Tribune"]["season"];
                        $episode = $md["Tribune"]["episode"];
                    }
                }
            }

            $isGeneric = FALSE;
            $seriesID = "";
            $type = strtolower(substr($programID, 0, 2));
            switch ($type)
            {
                case "sh":
                    $categoryType = "series";
                    $isGeneric = TRUE;
                    $seriesID = "EP" . substr($programID, 2, 8);
                    break;
                case "ep":
                    $categoryType = "tvshow";
                    $seriesID = substr($programID, 0, 10);
                    break;
                case "mv":
                    $categoryType = "movie";
                    break;
                case "sp":
                    $categoryType = "sports";
                    break;
                default:
                    debugMSG("FATAL ERROR: $programID has unknown type.");
                    continue;
                    break;
            }

            if ($type == "mv" AND isset($programJSON["movie"]))
            {
                if (isset($programJSON["movie"]["year"]))
                {
                    $movieYear = $programJSON["movie"]["year"];
                }

                /*
                 * MythTV uses a system where 4 stars would be a "1.0".
                 */

                if (isset($programJSON["movie"]["qualityRating"]))
                {
                    $starRating = $programJSON["movie"]["qualityRating"][0]["rating"] * 0.25;
                }
            }

            if (isset($programJSON["colorCode"]))
            {
                $colorCode = $programJSON["colorCode"];
            }

            if (isset($programJSON["syndicatedEpisodeNumber"]))
            {
                $syndicatedEpisodeNumber = $programJSON["syndicatedEpisodeNumber"];
            }

            if ($isStereo)
            {
                $audioprop = "STEREO";
            }

            if ($dolbyType)
            {
                $audioprop = "DOLBY";
            }

            if ($isSurround)
            {
                $audioprop = "SURROUND";
            }

            if (isset($programJSON["showType"]))
            {
                $showType = $programJSON["showType"];
            }

            if (isset($programJSON["originalAirDate"]))
            {
                $oad = $programJSON["originalAirDate"];
            }

            if ($isLetterboxed)
            {
                $videoProperties = "WIDESCREEN";
            }

            if ($isHDTV)
            {
                $videoProperties = "HDTV";
            }

            if ($isSigned)
            {
                $subtitleTypes = "SIGNED";
            }

            if ($dbSchema > "1318")
            {
                try
                {
                    $insertSchedule->execute(array(
                        "chanid"                  => $chanID,
                        "starttime"               => $programStartTimeMyth,
                        "endtime"                 => $programEndTimeMyth,
                        "title"                   => $title,
                        "subtitle"                => $subTitle,
                        "description"             => $description,
                        "category"                => $category,
                        "category_type"           => $categoryType,
                        "airdate"                 => $movieYear,
                        "stars"                   => $starRating,
                        "previouslyshown"         => $previouslyshown,
                        "stereo"                  => $isStereo,
                        "subtitled"               => $isSubtitled,
                        "hdtv"                    => $isHDTV,
                        "closecaptioned"          => $isClosedCaption,
                        "partnumber"              => $partNumber,
                        "parttotal"               => $numberOfParts,
                        "seriesid"                => $seriesID,
                        "originalairdate"         => $oad,
                        "showtype"                => $showType,
                        "colorcode"               => $colorCode,
                        "syndicatedepisodenumber" => $syndicatedEpisodeNumber,
                        "programid"               => $programID,
                        "generic"                 => $isGeneric,
                        "listingsource"           => $sourceID,
                        "first"                   => $isFirst,
                        "last"                    => $isLast,
                        "audioprop"               => $audioprop,
                        "subtitletypes"           => $subtitleTypes,
                        "videoprop"               => $videoProperties,
                        "season"                  => $season,
                        "episode"                 => $episode
                    ));
                } catch (PDOException $e)
                {
                    print "Exception: " . $e->getMessage();
                    $debug = TRUE;
                    var_dump($programJSON);
                }
            }
            else
            {
                try
                {
                    $insertSchedule->execute(array(
                        "chanid"                  => $chanID,
                        "starttime"               => $programStartTimeMyth,
                        "endtime"                 => $programEndTimeMyth,
                        "title"                   => $title,
                        "subtitle"                => $subTitle,
                        "description"             => $description,
                        "category"                => $category,
                        "category_type"           => $categoryType,
                        "airdate"                 => $movieYear,
                        "stars"                   => $starRating,
                        "previouslyshown"         => $previouslyshown,
                        "stereo"                  => $isStereo,
                        "subtitled"               => $isSubtitled,
                        "hdtv"                    => $isHDTV,
                        "closecaptioned"          => $isClosedCaption,
                        "partnumber"              => $partNumber,
                        "parttotal"               => $numberOfParts,
                        "seriesid"                => $seriesID,
                        "originalairdate"         => $oad,
                        "showtype"                => $showType,
                        "colorcode"               => $colorCode,
                        "syndicatedepisodenumber" => $syndicatedEpisodeNumber,
                        "programid"               => $programID,
                        "generic"                 => $isGeneric,
                        "listingsource"           => $sourceID,
                        "first"                   => $isFirst,
                        "last"                    => $isLast,
                        "audioprop"               => $audioprop,
                        "subtitletypes"           => $subtitleTypes,
                        "videoprop"               => $videoProperties
                    ));
                } catch (PDOException $e)
                {
                    print "Exception: " . $e->getMessage();
                    $debug = TRUE;
                    var_dump($programJSON);
                }
            }

            if ($ratingSystem != "")
            {
                try
                {
                    $insertProgramRatingMyth->execute(array("chanid"    => $chanID,
                                                            "starttime" => $programStartTimeMyth,
                                                            "system"    => $ratingSystem, "rating" => $rating));
                } catch (PDOException $e)
                {
                    print "Exception: " . $e->getMessage();
                    $debug = TRUE;
                }
            }
        }
        $dbh->commit();
    }

    /* These don't seem to be in the On data:
    if (isset($v["dubbedLanguage"]))
    {
        $dubbedLanguage = $v["dubbedLanguage"];
    }
    else
    {
        $dubbedLanguage = NULL;
    }


    if (isset($v["subtitledLanguage"]))
    {
        $subtitledLanguage = $v["subtitledLanguage"];
    }
    else
    {
        $subtitledLanguage = NULL;
    }

    if (isset($v["partNumber"]))
            {
                $partNumber = $v["partNumber"];
            }
            else
            {
                $partNumber = 0;
            }

            if (isset($v["numberOfParts"]))
            {
                $numberOfParts = $v["numberOfParts"];
            }
            else
            {
                $numberOfParts = 0;
            }

    if (isset($v["sapLanguage"]))
            {
                $sapLanguage = $v["sapLanguage"];
            }
            else
            {
                $sapLanguage = NULL;
            }

if (isset($v["sap"]))
            {
                $sap = TRUE;
            }
            else
            {
                $sap = FALSE;
            }


            if (isset($v["programLanguage"]))
            {
                $programLanguage = $v["programLanguage"];
            }
            else
            {
                $programLanguage = NULL;
            }


            if (isset($v["hasDialogRating"]))
            {
                $dialogRating = TRUE;
            }
            else
            {
                $dialogRating = FALSE;
            }

            if (isset($v["hasLanguageRating"]))
            {
                $languageRating = TRUE;
            }
            else
            {
                $languageRating = FALSE;
            }

            if (isset($v["hasSexRating"]))
            {
                $sexRating = TRUE;
            }
            else
            {
                $sexRating = FALSE;
            }

            if (isset($v["hasViolenceRating"]))
            {
                $violenceRating = TRUE;
            }
            else
            {
                $violenceRating = FALSE;
            }

            if (isset($v["hasFantasyViolenceRating"]))
            {
                $fvRating = TRUE;
            }
            else
            {
                $fvRating = FALSE;
            }


*/

    /*
     * TODO: update the credits table using the existing role types in MythTV.
     */


    /*
     * If users start to complain about errors on the insert, it's probably due to a new role type.
     */

    if ($debug AND count($roleTable))
    {
        debugMSG("Role table:");
        debugMSG(print_r($roleTable, TRUE));
    }

    $dbh->exec("DROP TABLE program");
    $dbh->exec("RENAME TABLE t_program TO program");

    $dbh->exec("DROP TABLE credits");
    $dbh->exec("RENAME TABLE t_credits TO credits");

    $dbh->exec("DROP TABLE programrating");
    $dbh->exec("RENAME TABLE t_programrating TO programrating");
}

function tempdir($type)
{
    if ($type == "programs")
    {
        $tempfile = tempnam(sys_get_temp_dir(), "mfdb_programs_");
    }

    if ($type == "schedules")
    {
        $tempfile = tempnam(sys_get_temp_dir(), "mfdb_schedules_");
    }

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

function updateStatus()
{
    global $dbh;
    global $client;
    global $host;
    global $useServiceAPI;
    global $isMythTV;

    $res = getStatus();

    if ($isMythTV)
    {
        $updateLocalMessageTable = $dbh->prepare("INSERT INTO SDMessages(id,date,message,type)
    VALUES(:id,:date,:message,:type) ON DUPLICATE KEY UPDATE message=:message,date=:date,type=:type");
    }

    if ($res["code"] == 0)
    {
        $expires = $res["account"]["expires"];
        $maxLineups = $res["account"]["maxLineups"];
        $nextConnectTime = $res["account"]["nextSuggestedConnectTime"];

        foreach ($res["account"]["messages"] as $a)
        {
            $msgID = $a["msgID"];
            $msgDate = $a["date"];
            $message = $a["message"];
            printMSG("MessageID:$msgID : $msgDate : $message");
            if ($isMythTV)
            {
                $updateLocalMessageTable->execute(array("id"      => $msgID, "date" => $msgDate,
                                                        "message" => $message,
                                                        "type"    => "U"));
            }
        }
    }
    else
    {
        debugMSG("Received error response from server!");
        debugMSG("ServerID: {$res["serverID"]}");
        debugMSG("Message: {$res["message"]}");
        debugMSG("FATAL ERROR. Terminating execution.");

        return ("ERROR");
    }

    printMSG("Server: {$res["serverID"]}");
    printMSG("Last data refresh: {$res["lastDataUpdate"]}");
    printMSG("Account expires: $expires");
    printMSG("Max number of lineups for your account: $maxLineups");
    printMSG("Next suggested connect time: $nextConnectTime");

    if ($useServiceAPI)
    {
        printMSG("Updating settings via the Services API.");
        $request = $client->post("http://$host:6544/Myth/PutSetting", array(),
            array("Key" => "MythFillSuggestedRunTime", "Value" => $nextConnectTime))->send();
        $request = $client->post("http://$host:6544/Myth/PutSetting", array(),
            array("Key" => "DataDirectMessage", "Value" => "Your subscription expires on $expires."))->send();
    }
    else
    {
        if ($isMythTV)
        {
            $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value = 'MythFillSuggestedRunTime' AND hostname IS NULL");
            $stmt->execute(array("data" => $nextConnectTime));

            $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='DataDirectMessage' AND hostname IS NULL");
            $stmt->execute(array("data" => "Your subscription expires on $expires."));

            $stmt = $dbh->prepare("SELECT data FROM settings WHERE value='SchedulesDirectLastUpdate'");
            $result = $stmt->fetchColumn();
            $getLastUpdate = $result[0];

            if ($res["lastDataUpdate"] == $getLastUpdate)
            {
                return ("No new data on server.");
            }
            else
            {
                printMSG("Updating settings using MySQL.");
                $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='SchedulesDirectLastUpdate'
        AND hostname IS NULL");
                $stmt->execute(array("data" => $res["lastDataUpdate"]));
            }
        }
    }

    if ($isMythTV)
    {
        exec("mythutil --clearcache"); // Force a clearcache to make sure that everyone is in sync.
    }

    return ("");
}

?>
