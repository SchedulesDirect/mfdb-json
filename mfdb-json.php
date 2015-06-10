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

ini_set("memory_limit", "-1");

require_once "vendor/autoload.php";
require_once "functions.php";
use Guzzle\Http\Client;

$isBeta = TRUE;
$api = "";
$debug = FALSE;
$quiet = FALSE;
$forceDownload = FALSE;
$forceRun = FALSE;
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
$skipVersionCheck = FALSE;
$jsonProgramsToRetrieve = array();
$addToRetryQueue = array();
$permanentDownloadFailure = array();
$scheduleJSON = array();
$dbHostSD = "localhost";
$daysToRetrieve = 30;

$baseurl = getBaseURL($isBeta);

$agentString = "mfdb-json.php developer grabber API:$api v$scriptVersion/$scriptDate";

$client = new Guzzle\Http\Client($baseurl);
$client->setUserAgent($agentString);

$helpText = <<< eol
The following options are available:
--beta
--help\t\t(this text)
--days\t\tNumber of days to retrieve. (Default: 30)
--dbname=\tMySQL database name for MythTV. (Default: mythconverg)
--dbuser=\tUsername for database access for MythTV. (Default: mythtv)
--dbpassword=\tPassword for database access for MythTV. (Default: mythtv)
--dbhost=\tMySQL database hostname for MythTV. (Default: localhost)
--dbhostsd=\tMySQL database hostname for SchedulesDirect JSON data. (Default: localhost)
--forcedownload\tForce download of schedules. (Default: FALSE)
--forcerun\tContinue to run even if we're known to be broken. (Default: FALSE)
--host=\t\tIP address of the MythTV backend. (Default: localhost)
--nomyth\tDon't execute any MythTV specific functions. (Default: FALSE)
    Must specify --schedule and/or --program
--max=\t\tMaximum number of programs to retrieve per request. (Default:$maxProgramsToGet)
--program\tDownload programs based on programIDs in sd.json.programs.conf file.
--quiet\t\tDon't print to screen; put all output into the logfile.
--skipversion\tForce the program to run even if there's a version mismatch between the client and the server.
--station=\tDownload the schedule for a single stationID in your lineup.
--schedule\tDownload schedules based on stationIDs in sd.json.stations.conf file.
--timezone=\tSet the timezone for log file timestamps. See http://www.php.net/manual/en/timezones.php (Default:$tz)
--usedb\t\tUse a database to store data, even if you're not running MythTV. (Default: FALSE)
--version\tPrint version information and exit.

eol;

$longoptions = array("debug", "help", "host::", "days::", "dbname::", "dbuser::", "dbpassword::", "dbhost::",
                     "forcedownload", "forcerun", "nomyth", "max::", "program", "quiet", "station::", "schedule",
                     "skipversion", "timezone::",
                     "usedb", "version");
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
        case "days":
            $daysToRetrieve = $v;
            break;
        case "dbhost":
            $dbHost = $v;
            break;
        case "dbhostsd":
            $dbHostSD = $v;
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
        case "forcedownload":
            $forceDownload = TRUE;
            break;
        case "forcerun":
            $forceRun = TRUE;
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
        case "skipversion":
            $skipVersionCheck = TRUE;
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
    }
}

if ($knownToBeBroken === TRUE AND $forceRun === FALSE)
{
    print "This version is known to be broken and --forcerun option not specified. Exiting.\n";
    exit;
}

if ($skipVersionCheck === FALSE)
{
    printMSG("Checking to see if we're running the latest client.");

    list($hadError, $serverVersion) = checkForClientUpdate($client);

    if ($hadError !== FALSE)
    {
        if ($hadError != 1005)
        {
            printMSG("Received error response from server. Exiting.");
            exit;
        }
        else
        {
            printMSG("Server doesn't recognize our client. Continuing.");
        }
    }
    else
    {
        if ($serverVersion != $scriptVersion)
        {
            printMSG("***Version mismatch.***");
            printMSG("Server version: $serverVersion");
            printMSG("Our version: $scriptVersion");
            if ($skipVersionCheck === FALSE)
            {
                printMSG("Exiting. Do you need to run 'git pull' to refresh?");
                printMSG("Restart script with --skipversion to ignore mismatch.");
                exit;
            }
            else
            {
                printMSG("Continuing because of --forcerun parameter.");
            }
        }
    }
}

printMSG("$agentString");

$dlSchedTempDir = tempdir("schedules");
printMSG("Temp directory for Schedules is $dlSchedTempDir");
$dlProgramTempDir = tempdir("programs");
printMSG("Temp directory for Programs is $dlProgramTempDir");

if ($isMythTV === TRUE)
{
    if
    (
        (isset($dbHost) === FALSE) AND
        (isset($dbName) === FALSE) AND
        (isset($dbUser) === FALSE) AND
        (isset($dbPassword) === FALSE)
    )
    {
        list($dbHost, $dbName, $dbUser, $dbPassword) = getLoginFromFiles();
    }
    if ($dbHost == "NONE")
    {
        $dbUser = "mythtv";
        $dbPassword = "mythtv";
        $dbHost = "localhost";
        $dbName = "mythconverg";
        $host = "localhost";
    }
}

if (isset($dbHost) === FALSE)
{
    $dbHost = "localhost";
}

if (isset($dbName) === FALSE)
{
    $dbName = "mythconverg";
}

if (isset($dbUser) === FALSE)
{
    $dbUser = "mythtv";
}

if (isset($dbPassword) === FALSE)
{
    $dbPassword = "mythtv";
}

if (isset($host) === FALSE)
{
    $host = "localhost";
}

if (($isMythTV === TRUE) OR ($dbWithoutMythtv === TRUE))
{
    printMSG("Connecting to Schedules Direct database.");
    try
    {
        $dbhSD = new PDO("mysql:host=$dbHostSD;dbname=schedulesdirect;charset=utf8", "sd", "sd");
        $dbhSD->exec("SET CHARACTER SET utf8");
        $dbhSD->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    } catch (PDOException $e)
    {
        if ($e->getCode() == 2002)
        {
            printMSG("Could not connect to database:\n" . $e->getMessage());
            exit;
        }

        if ($e->getCode() == 1049)
        {
            printMSG("Initial database not created for Schedules Direct tables.");
            printMSG("Please run\nmysql -uroot -p < sd.sql");
            printMSG("Then run sd-utility.php to complete the initialization.");
            printMSG("Make sure you use function '4' to refresh the local lineup cache.");
            printMSG("Please check the updated README.md for more information.");
            exit;
        }
        else
        {
            printMSG("Got error connecting to database.");
            printMSG("Code: " . $e->getCode() . "");
            printMSG("Message: " . $e->getMessage() . "");
            exit;
        }
    }

    if ($isMythTV)
    {
        printMSG("Connecting to MythTV database.");
        try
        {
            $dbh = new PDO("mysql:host=$dbHost;dbname=$dbName;charset=utf8", $dbUser, $dbPassword);
            $dbh->exec("SET CHARACTER SET utf8");
            $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e)
        {
            if ($e->getCode() == 2002)
            {
                printMSG("Could not connect to database:\n" . $e->getMessage() . "");
                printMSG("If you're running the grabber as standalone, use --nomyth");
                exit;
            }
            else
            {
                printMSG("Got error connecting to database.");
                printMSG("Code: " . $e->getCode() . "");
                printMSG("Message: " . $e->getMessage() . "");
                exit;
            }
        }

        $useServiceAPI = checkForServiceAPI();

        $userLoginInformation = settingSD("SchedulesDirectLogin");

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

        /*
         * MythTV uses an auto-increment field for the person number, SD doesn't. We'll need to cross-reference later.
         */
        $getPeople = $dbh->prepare("SELECT name,person FROM people");
        $getPeople->execute();
        $peopleCacheMyth = $getPeople->fetchAll(PDO::FETCH_KEY_PAIR);
    }
    else
    {
        if (file_exists("sd.json.conf") === TRUE)
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
}
$globalStartTime = time();
$globalStartDate = new DateTime();

if ($isMythTV === TRUE)
{
    if ($station == "")
    {
        printMSG("Retrieving list of channels to download.");

        $stmt = $dbh->prepare("SELECT sourceID FROM videosource WHERE xmltvgrabber='schedulesdirect2'");
        $stmt->execute();
        $sources = $stmt->fetchAll(PDO::FETCH_COLUMN);

        if (count($sources) == 0)
        {
            printMSG("Error: no videosources configured for Schedules Direct JSON service. Check videosource table.");
            exit;
        }

        foreach ($sources as $sourceid)
        {
            $stmt = $dbh->prepare("SELECT xmltvid FROM channel WHERE visible=TRUE AND xmltvid != '' AND xmltvid > 0 AND
        sourceid=:sourceid");
            $stmt->execute(array("sourceid" => $sourceid));
            $stationIDs = array_merge($stationIDs, $stmt->fetchAll(PDO::FETCH_COLUMN));
        }

        /*
         * Double flip does a unique then makes the keys back to values
         */
        $stationIDs = array_flip(array_flip($stationIDs));

        if (count($stationIDs) == 0)
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
    if ((file_exists("sd.json.stations.conf") === TRUE) AND ($useScheduleFile === TRUE))
    {
        $stationIDs = file("sd.json.stations.conf");
    }
    elseif ($station != "")
    {
        $stationIDs[] = $station;
    }
    elseif ($useProgramFile === FALSE)
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
    if (($isMythTV === TRUE) OR ($dbWithoutMythtv === TRUE))
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
    if (count($stationIDs) != 0)
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
                if (isset($permanentDownloadFailure[$k]) === FALSE)
                {
                    $bar[] = $k;
                    printMSG("StationID: $k");
                }
            }
            $addToRetryQueue = array(); // Otherwise we may never exit.
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

if ($isMythTV === TRUE)
{
    if ((count($jsonProgramsToRetrieve) != 0) OR ($forceDownload === TRUE))
    {
        updateLocalProgramCache($jsonProgramsToRetrieve);
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
    if ((file_exists("sd.json.programs.conf") === TRUE) AND ($useProgramFile === TRUE))
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

if ($isMythTV === TRUE)
{
    printMSG("Updating status.");

    $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunStart' AND hostname IS NULL");
    $stmt->execute(array("data" => $globalStartTime));

    $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunEnd' AND hostname IS NULL");
    $stmt->execute(array("data" => $globalEndTime));

    $stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunStatus' AND hostname IS NULL");
    $stmt->execute(array("data" => $statusMessage));

    printMSG("Sending reschedule request to mythbackend.");

    if (`which mythutil`)
    {
        exec("mythutil --resched");
    }
}

printMSG("Done.");

if ($errorWarning === TRUE)
{
    debugMSG("NOTE! Errors encountered during processing. Check logs.");
}

exit;

function getSchedules($stationIDsToFetch)
{
    global $client;
    global $dbh;
    global $dbhSD;
    global $token;
    global $dlSchedTempDir;
    global $quiet;
    global $debug;
    global $forceDownload;
    global $addToRetryQueue;
    global $permanentDownloadFailure;
    global $scheduleJSON;
    global $daysToRetrieve;
    global $todayDate;

    $arrayProgramsToRetrieveFromSchedulesDirect = array();
    $requestArray = array();
    $dateArray = array();
    $bar = array();
    $schedulesToFetch = array();

    $dbProgramCache = array();
    $response = "";

    $downloadedStationIDs = array();
    $serverScheduleMD5 = array();

    if ($debug === TRUE)
    {
        print "Station array:\n";
        var_dump($stationIDsToFetch);
    }

    if (count($stationIDsToFetch) == 0)
    {
        print "1. No schedules to fetch.\n";

        return ("");
    }

    //$stationIDsToFetch = array("20454", "10021");

    foreach (range(0, ($daysToRetrieve - 1)) as $j)
    {
        $dateArray[] = date("Y-m-d", strtotime("$todayDate + $j days"));
    }

    while (list(, $sid) = each($stationIDsToFetch))
    {
        $requestArray[] = array("stationID" => "$sid", "date" => $dateArray);
    }

    if (count($requestArray) == 0)
    {
        print "2. No schedules to fetch.\n"; // Should never hit this.
        return ("");
    }

    if ($debug === TRUE)
    {
        print "Request array:\n";
        var_dump($requestArray);
    }

    if ($forceDownload === FALSE)
    {
        printMSG("Determining if there are updated schedules.");

        $errorCount = 0;
        $timeout = 30;
        $response = NULL;

        do
        {
            try
            {
                $request = $client->post("schedules/md5",
                    array("token"           => $token,
                          "Accept-Encoding" => "deflate,gzip"),
                    json_encode($requestArray),
                    array("timeout" => $timeout));
                $response = $request->send();

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
                        print "Code: " . $e->getCode() . "\n";
                        print "Message: " . $e->getMessage() . "\n";
                        var_dump($e);
                        break;
                }
            } catch (Guzzle\Http\Exception\CurlException $e)
            {
                if (strpos($e->getMessage(), "Operation timed out") > 0)
                {
                    $errorCount++;
                    printMSG("No response from server; retrying. Code is " . $e->getCode());
                    $timeout *= 2;
                    sleep(10); // Hammering away isn't going to make things better.
                    break;
                }
            } catch (Exception $e)
            {
                print "Other exception in getSchedules.\n";
                print "Code: " . $e->getCode() . "\n";
                print "Message: " . $e->getMessage() . "\n";
                var_dump($e);
                exit;
            }

            if (is_null($response) === FALSE)
            {
                break;
            }
        } while ($errorCount < 5);

        if ($errorCount == 5)
        {
            printMSG("Fatal error trying to get MD5 for schedules.");

            return ("ERROR");
        }

        $schedulesDirectMD5s = $response->json();

        if ($debug === TRUE)
        {
            print "Schedules Direct MD5's\n";
            var_dump($schedulesDirectMD5s);
        }

        $getLocalCacheMD5 = $dbhSD->prepare("SELECT md5 FROM schedules WHERE stationID=:sid AND date=:date");

        while (list($stationID, $data) = each($schedulesDirectMD5s))
        {
            $bar = array();
            $needToFetch[$stationID] = FALSE;

            foreach ($data as $date => $arrayFoo)
            {
                if ($arrayFoo["code"] != 0)
                {
                    printMSG("Got error response for sid:$stationID, date:$date array:" . print_r($arrayFoo, TRUE));
                    continue;
                }

                if ($debug === TRUE)
                {
                    print "sid: $stationID\n";
                    print "data:\n";
                    var_dump($data);
                }

                $getLocalCacheMD5->execute(array("sid" => $stationID, "date" => $date));
                $localMD5 = $getLocalCacheMD5->fetchColumn();

                if ($debug === TRUE)
                {
                    print "Local MD5\n";
                    var_dump($localMD5);
                }

                if ($localMD5 != $arrayFoo["md5"])
                {
                    /*
                     * We need to get that particular day.
                     */
                    $bar[] = $date;
                    $needToFetch[$stationID] = TRUE;
                }
            }

            if ($needToFetch[$stationID] === TRUE)
            {
                $schedulesToFetch[] = array("stationID" => "$stationID", "date" => $bar);
            }
        }
    }
    else
    {
        /*
         * We need to generate a real request array here.
         */
        $bar = $requestArray;
    }

    if ($debug === TRUE)
    {
        print "schedulesToFetch is now\n";
        var_dump($schedulesToFetch);
    }

    if (count($schedulesToFetch) == 0)
    {
        printMSG("No updated schedules.");

        return ($arrayProgramsToRetrieveFromSchedulesDirect);
    }

    printMSG(count($schedulesToFetch) . " schedules to download.");

    $errorCount = 0;

    do
    {
        $response = NULL;
        try
        {
            $response = $client->post("schedules",
                array("token"           => $token,
                      "Accept-Encoding" => "deflate,gzip"),
                json_encode($schedulesToFetch), array("timeout" => 120))->send();
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
                    print "Code: " . $e->getCode() . "\n";
                    print "Message: " . $e->getMessage() . "\n";
                    var_dump($e);
                    break;
            }
        } catch (Exception $e)
        {
            print "Other exception in getSchedules.\n";
            print "Code: " . $e->getCode() . "\n";
            print "Message: " . $e->getMessage() . "\n";
            var_dump($e);
            exit;
        }

        if (is_null($response) === FALSE)
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

    file_put_contents("$dlSchedTempDir/schedule.json", $schedules);

    $updateLocalMD5 = $dbhSD->prepare("INSERT INTO schedules(stationID, date, md5) VALUES(:sid, :date, :md5)
    ON DUPLICATE KEY UPDATE md5=:md5");

    $jsonSchedule = file_get_contents("$dlSchedTempDir/schedule.json");

    $item = json_decode($jsonSchedule, TRUE);

    if (json_last_error() === TRUE)
    {
        printMSG("Couldn't decode $dlSchedTempDir/schedule.json from JSON.");
        printMSG(json_last_error_msg());
        exit;
    }

    printMSG("Parsing schedules.");

    foreach ($item as $v)
    {
        if (isset($v["stationID"]) === TRUE)
        {
            $stationID = $v["stationID"];
        }
        else
        {
            printMSG("Fatal: No stationID? Send the following to grabber@schedulesdirect.org");
            printMSG($item);
            continue;
        }

        if (isset($v["code"]) === TRUE)
        {
            switch ($v["code"])
            {
                case 7000:
                    if (isset($addToRetryQueue[$stationID]) === TRUE)
                    {
                        $permanentDownloadFailure[$stationID] = 1;
                        unset($addToRetryQueue[$stationID]); // Permanent error.
                    }
                    printMSG("Permanent error attempting to fetch schedule for $stationID");
                    continue;
                    break;
                case 7100:
                    if (isset($addToRetryQueue[$stationID]) === FALSE)
                    {
                        $addToRetryQueue[$stationID] = 1;
                    }
                    else
                    {
                        $addToRetryQueue[$stationID]++;
                    }

                    printMSG("Adding $stationID to retry queue. Count is {$addToRetryQueue[$stationID]}");

                    if ($addToRetryQueue[$stationID] == 10)
                    {
                        unset($addToRetryQueue[$stationID]); // Permanent error.
                        $permanentDownloadFailure[$stationID] = 1;
                        printMSG("Permanent error attempting to fetch schedule for $stationID");
                    }

                    sleep(10); // We're going to sleep so that the server has the chance to generate the schedule.
                    continue;
                    break;
                default:
                    printMSG("getSchedules error: $item");
                    continue;
                    break;
            }
        }

        $downloadedStationIDs[] = $stationID;
        $date = $v["metadata"]["startDate"];
        $md5 = $v["metadata"]["md5"];

        if ($debug === TRUE)
        {
            printMSG("Parsing schedule stationID:$stationID for $date");
        }

        if (isset($v["metadata"]["isDeleted"]) === TRUE)
        {
            printMSG("WARNING: $stationID has been marked as deleted.");
            $updateVisibleToFalse = $dbh->prepare("UPDATE channel SET visible = FALSE WHERE xmltvid=:sid");
            $updateVisibleToFalse->execute(array("sid" => $stationID));
            continue;
        }

        if (isset($v["programs"]) === FALSE)
        {
            printMSG("WARNING: JSON does not contain any program elements.");
            printMSG("Send the following to grabber@schedulesdirect.org\n\n");
            var_dump($item);
            exit;
        }

        $updateLocalMD5->execute(array("sid" => $stationID, "md5" => $md5, "date" => $date));

        foreach ($v["programs"] as $programData)
        {
            if (isset($programData["md5"]) === TRUE)
            {
                $serverScheduleMD5[$programData["md5"]] = $programData["programID"];
                $scheduleJSON[$stationID][] = $programData;
            }
            else
            {
                $quiet = FALSE;
                printMSG("FATAL ERROR: no MD5 value for program.");
                printMSG("Send the following to grabber@schedulesdirect.org\n");
                printMSG("s:$stationID\n\n\n\nitem\n\n" . print_r($programData, TRUE) . "\n\n");
                continue;
            }
        }
    }

    printMSG("There are " . count($serverScheduleMD5) . " programIDs in the upcoming schedule.");
    printMSG("Retrieving existing MD5 values from local cache.");

    /*
     * We're going to figure out which programIDs we need to download.
     */

    $stmt = $dbhSD->prepare("SELECT md5, programID FROM programs");
    $stmt->execute();
    $dbProgramCache = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

    $arrayProgramsToRetrieveFromSchedulesDirect = array_diff_key($serverScheduleMD5, $dbProgramCache);

    if ($debug === TRUE)
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
        printMSG(print_r($arrayProgramsToRetrieveFromSchedulesDirect, TRUE));
    }

    return ($arrayProgramsToRetrieveFromSchedulesDirect);
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

        $totalChunks = intval($toRetrieveTotal / $maxProgramsToGet);

        $counter = 0;
        $retrieveList = array();

        for ($i = 0; $i <= $totalChunks; $i++)
        {
            $startOffset = $i * $maxProgramsToGet;
            $chunk = array_slice($jsonProgramsToRetrieve, $startOffset, $maxProgramsToGet);
            $retrieveList[] = json_encode($chunk);

            $counter += count($chunk);
        }

        $failedChunk = array();

        foreach ($retrieveList as $index => $chunk)
        {
            $retryCounter = 0;
            $hadError = FALSE;
            $failedChunk[$index] = TRUE;

            printMSG("Retrieving chunk " . ($index + 1) . " of " . count($retrieveList) . ".");
            do
            {
                $schedulesDirectPrograms = $client->post("programs",
                    array("token"           => $token,
                          "Accept-Encoding" => "deflate,gzip"), $chunk);
                $response = $schedulesDirectPrograms->send();

                try
                {
                    $schedulesDirectPrograms = $response->getBody();
                } catch (Guzzle\Http\Exception\ServerErrorResponseException $e)
                {
                    $errorReq = $e->getRequest();
                    $errorResp = $e->getResponse();
                    $errorMessage = $e->getMessage();
                    exceptionErrorDump($errorReq, $errorResp, $errorMessage);
                    $retryCounter++;
                    $hadError = TRUE;
                    debugMSG("Had error retrieving chunk $index: retrying.");
                    sleep(30);
                } catch (Guzzle\Http\Exception\CurlException $e)
                {
                    $errorReq = $e->getRequest();
                    $errorResp = $e->getResponse();
                    $errorMessage = $e->getMessage();
                    exceptionErrorDump($errorReq, $errorResp, $errorMessage);
                    $retryCounter++;
                    $hadError = TRUE;
                    debugMSG("Had error retrieving chunk $index: retrying.");
                    sleep(30);
                } catch (Guzzle\Http\Exception\RequestException $e)
                {
                    $errorReq = $e->getRequest();
                    $errorResp = $e->getResponse();
                    $errorMessage = $e->getMessage();
                    exceptionErrorDump($errorReq, $errorResp, $errorMessage);
                    $retryCounter++;
                    $hadError = TRUE;
                    debugMSG("Had error retrieving chunk $index: retrying.");
                    sleep(30);
                }

                if ($hadError === FALSE)
                {
                    file_put_contents("$dlProgramTempDir/programs." . substr("00$index", -2) . ".json",
                        $schedulesDirectPrograms);
                    unset($failedChunk[$index]);
                    break;
                }
            } while ($retryCounter < 5);
        }

        if (count($failedChunk) != 0)
        {
            printMSG("Failed to retrieve data after multiple retries.");
        }
    }

    return ($jsonProgramsToRetrieve);
}

function updateLocalProgramCache(array $jsonProgramsToRetrieve)
{
    global $dbhSD;
    global $dlProgramTempDir;
    global $debug;

    $insertJSON = $dbhSD->prepare("INSERT INTO programs(programID,md5,json)
            VALUES (:programID,:md5,:json)
            ON DUPLICATE KEY UPDATE md5=:md5, json=:json");

    $insertPersonSD = $dbhSD->prepare("INSERT INTO people(personID,name) VALUES(:personID, :name)");
    $updatePersonSD = $dbhSD->prepare("UPDATE people SET name=:name WHERE personID=:personID");

    $insertCreditSD = $dbhSD->prepare("INSERT INTO credits(personID,programID,role)
    VALUES(:personID,:pid,:role)");

    $insertProgramGenresSD = $dbhSD->prepare("INSERT INTO programGenres(programID,relevance,genre)
    VALUES(:pid,:relevance,:genre) ON DUPLICATE KEY UPDATE genre=:genre");

    $getPeople = $dbhSD->prepare("SELECT personID,name FROM people");
    $getPeople->execute();
    $peopleCacheSD = $getPeople->fetchAll(PDO::FETCH_KEY_PAIR);

    $getCredits = $dbhSD->prepare("SELECT CONCAT(personID,'-',programID,'-',role) FROM credits");
    $getCredits->execute();
    $creditCache = $getCredits->fetchAll(PDO::FETCH_COLUMN);

    $creditCache = array_flip($creditCache);

    $counter = 0;
    $total = count($jsonProgramsToRetrieve);
    printMSG("Performing inserts of JSON data.");

    foreach (glob("$dlProgramTempDir/*.json") as $jsonFileToProcess)
    {
        $rawProgramJSON = file_get_contents($jsonFileToProcess);
        $jsonPrograms = json_decode($rawProgramJSON, TRUE);

        if (json_last_error() === TRUE)
        {
            debugMSG("*** ERROR: JSON decode error $jsonFileToProcess");
            debugMSG(print_r($jsonPrograms, TRUE));
            continue;
        }

        debugMSG("Processing $jsonFileToProcess");

        $dbhSD->beginTransaction();

        foreach ($jsonPrograms as $v)
        {
            $counter++;
            if ($counter % 100 == 0)
            {
                printMSG("$counter / $total             \r");
                $dbhSD->commit();
                $dbhSD->beginTransaction();
            }
            if (isset($v["code"]) === TRUE)
            {
                /*
                 * Probably not good. :(
                 */

                if ($v["code"] == 6000)
                {
                    print "FATAL ERROR: server couldn't find programID?\n";
                    print "$jsonPrograms\n";
                    continue;
                }
            }

            if (isset($v["programID"]) === TRUE)
            {
                $pid = $v["programID"];
            }
            else
            {
                print "FATAL ERROR: No programID?\n";
                var_dump($v);
                continue;
            }

            if (isset($v["md5"]) === TRUE)
            {
                $md5 = $v["md5"];
            }
            else
            {
                print "FATAL ERROR: No md5?\n";
                print "$jsonPrograms\n";
                continue;
            }

            $insertJSON->execute(array("programID" => $pid, "md5" => $md5,
                                       "json"      => json_encode($v)));
        }

        $dbhSD->commit();
    }

    if ($debug === FALSE)
    {
        // unlink($jsonFileToProcess); don't delete for now.
    }

    if ($debug === FALSE)
    {
        // rmdir("$dlProgramTempDir"); don't delete for now.
    }

    printMSG("Completed local database program updates.");
}

function insertSchedule()
{
    global $dbh;
    global $dbhSD;
    global $peopleCacheMyth;
    global $debug;
    global $errorWarning;
    global $scheduleJSON;

    $insertPersonMyth = $dbh->prepare("INSERT INTO people(name) VALUES(:name)");

    $SchedulesDirectRoleToMythTv = array("Actor"                               => "actor",
                                         "Voice"                               => "actor",
                                         "Director"                            => "director",
                                         "Executive Producer"                  => "executive_producer",
                                         "Co-Producer"                         => "producer",
                                         "Associate Producer"                  => "producer",
                                         "Producer"                            => "producer",
                                         "Line Producer"                       => "producer",
                                         "Co-Executive Producer"               => "executive_producer",
                                         "Co-Associate Producer"               => "producer",
                                         "Assistant Producer"                  => "producer",
                                         "Supervising Producer"                => "producer",
                                         "Executive Music Producer"            => "producer",
                                         "Visual Effects Producer"             => "producer",
                                         "Special Effects Makeup Producer"     => "producer",
                                         "Makeup Effects Producer"             => "producer",
                                         "Consulting Producer"                 => "producer",
                                         "Score Producer"                      => "producer",
                                         "Executive Producer, English Version" => "producer",
                                         "Co-Writer"                           => "writer",
                                         "Screenwriter"                        => "writer",
                                         "Writer"                              => "writer",
                                         "Guest Star"                          => "guest_star",
                                         "Anchor"                              => "host",
                                         "Host"                                => "host",
                                         "Presenter"                           => "presenter",
                                         "Guest"                               => "guest"
    );

    $roleTable = array();

    printMSG("Inserting schedules.");

    $dbh->exec("DROP TABLE IF EXISTS t_program");
    $dbh->exec("CREATE TABLE t_program LIKE program");
    $dbh->exec("INSERT t_program SELECT * FROM program");

    $dbh->exec("DROP TABLE IF EXISTS t_credits");
    $dbh->exec("CREATE TABLE t_credits LIKE credits");

    $dbh->exec("DROP TABLE IF EXISTS t_programrating");
    $dbh->exec("CREATE TABLE t_programrating LIKE programrating");

    $dbh->exec("DROP TABLE IF EXISTS t_programgenres");
    $dbh->exec("CREATE TABLE t_programgenres LIKE programgenres");

    $dbSchema = setting("DBSchemaVer");

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

    $insertCreditMyth = $dbh->prepare("INSERT IGNORE INTO t_credits(person, chanid, starttime, role)
    VALUES(:person,:chanid,:starttime,:role)");

    $insertProgramRatingMyth = $dbh->prepare("INSERT INTO t_programrating(chanid, starttime, system, rating)
    VALUES(:chanid,:starttime,:system,:rating)");

    $insertProgramGenreMyth = $dbh->prepare("INSERT INTO t_programgenres(chanid, starttime, relevance, genre)
    VALUES(:chanid,:starttime,:relevance,:genre)");

    $getExistingChannels = $dbh->prepare("SELECT chanid,sourceid, CAST(xmltvid AS UNSIGNED) AS xmltvid FROM channel
WHERE visible = 1 AND xmltvid != '' AND xmltvid > 0 ORDER BY xmltvid");
    $getExistingChannels->execute();
    $existingChannels = $getExistingChannels->fetchAll(PDO::FETCH_ASSOC);

    $getProgramInformation = $dbhSD->prepare("SELECT json FROM programs WHERE programID =:pid");

    $deleteExistingSchedule = $dbh->prepare("DELETE FROM t_program WHERE chanid = :chanid");

    while (list(, $item) = each($existingChannels))
    {
        $chanID = $item["chanid"];
        $sourceID = $item["sourceid"];
        $stationID = $item["xmltvid"];

        if (isset($scheduleJSON[$stationID]) === FALSE)
        {
            continue; // If we don't have an updated schedule for the stationID, there's nothing to do.
        }

        printMSG("Inserting schedule for chanid:$chanID sourceid:$sourceID stationID:$stationID");

        $deleteExistingSchedule->execute(array("chanid" => $chanID));

        $dbh->beginTransaction();

        reset($scheduleJSON[$stationID]);
        while (list(, $scheduleElement) = each($scheduleJSON[$stationID]))
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

            $programID = $scheduleElement["programID"];
            $getProgramInformation->execute(array("pid" => $programID));
            $pj = $getProgramInformation->fetchColumn();

            if ($pj == "")
            {
                debugMSG("Error retrieving $programID from local database.");
                $errorWarning = TRUE;
                continue;
            }

            $programJSON = json_decode($pj, TRUE);

            if (json_last_error() === TRUE)
            {
                debugMSG("Error decoding $programID from local database. Raw data was:");
                debugMSG(print_r($pj, TRUE));
                $errorWarning = TRUE;
                continue;
            }

            $md5 = $scheduleElement["md5"];
            $air_datetime = $scheduleElement["airDateTime"];
            $duration = $scheduleElement["duration"];

            $programStartTimeMyth = str_replace("T", " ", $air_datetime);
            $programStartTimeMyth = rtrim($programStartTimeMyth, "Z");
            $programEndTimeMyth = gmdate("Y-m-d H:i:s", strtotime("$air_datetime + $duration seconds"));

            $skipPersonID = FALSE;

            if (isset($programJSON["genres"]) === TRUE)
            {
                foreach ($programJSON["genres"] as $g)
                {
                    if (in_array($g, array("Adults only", "Erotic")))
                    {
                        $skipPersonID = TRUE; // Adult content typically does not have personID.
                    }
                }
            }

            foreach (array("cast", "crew") as $processing)
            {
                if (isset($programJSON[$processing]) === TRUE)
                {
                    foreach ($programJSON[$processing] as $credit)
                    {
                        if (isset($credit["role"]) === FALSE)
                        {
                            printMSG("No role?");
                            printMSG("Send the following to grabber@schedulesdirect.org\n\n");
                            printMSG("Program: $programID. No role in cast.");
                            var_dump($programJSON[$processing]);
                            exit;
                        }

                        $name = $credit["name"];

                        if (isset($credit["personId"]) === FALSE)
                        {
                            if ($skipPersonID === FALSE)
                            {
                                printMSG("$programID does not have a personId.");
                            }
                        }

                        if (isset($peopleCacheMyth[$name]) === FALSE)
                        {
                            $insertPersonMyth->execute(array("name" => $name));
                            $personID = $dbh->lastInsertId();
                            $peopleCacheMyth[$name] = $personID;
                        }
                        else
                        {
                            $personID = $peopleCacheMyth[$name];
                        }

                        if (isset($SchedulesDirectRoleToMythTv[$credit["role"]]) === TRUE)
                        {
                            $mythTVRole = $SchedulesDirectRoleToMythTv[$credit["role"]];
                            $insertCreditMyth->execute(array("person"    => $personID,
                                                             "chanid"    => $chanID,
                                                             "starttime" => $programStartTimeMyth,
                                                             "role"      => $mythTVRole));
                        }
                    }
                }
            }

            if (isset($scheduleElement["audioProperties"]) === TRUE)
            {
                foreach ($scheduleElement["audioProperties"] as $ap)
                {
                    switch ($ap)
                    {
                        case "cc":
                            $isClosedCaption = TRUE;
                            break;
                        case "dvs":
                            $dvs = TRUE;
                            break;
                        case "Dolby":
                            $dolbyType = "dolby";
                            break;
                        case "DD":
                            $dolbyType = "Dolby Digital";
                            break;
                        case "DD 5.1":
                            $dolbyType = "Dolby Digital 5.1";
                            break;
                        case "dubbed":
                            $dubbed = TRUE;
                            break;
                        case "stereo":
                            $isStereo = TRUE;
                            break;
                        case "subtitled":
                            $isSubtitled = TRUE;
                            break;
                        case "surround":
                            $isSurround = TRUE;
                            break;
                    }
                }
            }

            if (isset($scheduleElement["videoProperties"]) === TRUE)
            {
                foreach ($scheduleElement["videoProperties"] as $vp)
                {
                    switch ($vp)
                    {
                        case "3d":
                            $is3d = TRUE;
                            break;
                        case "enhanced":
                            $isEnhancedResolution = TRUE;
                            break;
                        case "hdtv":
                            $isHDTV = TRUE;
                            break;
                        case "letterbox":
                            $isLetterboxed = TRUE;
                            break;
                        case "sdtv":
                            $isSDTV = TRUE; // Not used in MythTV
                            break;

                    }
                }
            }

            if (isset($scheduleElement["isPremiereOrFinale"]) === TRUE)
            {
                switch ($scheduleElement["isPremiereOrFinale"])
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

            if (isset($scheduleElement["contentRating"]) === TRUE)
            {
                foreach ($scheduleElement["contentRating"] as $r)
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
                if (isset($programJSON["contentRating"]) === TRUE)
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

            if (isset($scheduleElement["new"]) === TRUE)
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

            if (isset($scheduleElement["repeat"]) === TRUE)
            {
                if ($isNew === TRUE)
                {
                    debugMSG("*** WARNING sid:$stationID pid:$programID has 'new' and 'repeat' set. Open SD ticket:");
                    debugMSG(print_r($scheduleElement, TRUE));
                    $errorWarning = TRUE;
                }
                else
                {
                    $isNew = FALSE;
                    $previouslyshown = TRUE;
                }
            }

            if (isset($scheduleElement["cableInTheClassroom"]) === TRUE)
            {
                $cableInTheClassroom = TRUE;
            }

            if (isset($scheduleElement["catchup"]) === TRUE)
            {
                $catchupProgram = TRUE;
            }

            if (isset($scheduleElement["continued"]) === TRUE)
            {
                $continuedProgram = TRUE;
            }

            if (isset($scheduleElement["educational"]) === TRUE)
            {
                $isEducational = TRUE;
            }

            if (isset($scheduleElement["joinedInProgress"]) === TRUE)
            {
                $joinedInProgress = TRUE;
            }

            if (isset($scheduleElement["leftInProgress"]) === TRUE)
            {
                $leftInProgress = TRUE;
            }

            if (isset($scheduleElement["premiere"]) === TRUE)
            {
                $isPremiere = TRUE;
            }

            if (isset($scheduleElement["programBreak"]) === TRUE)
            {
                $programBreak = TRUE;
            }

            if (isset($scheduleElement["signed"]) === TRUE)
            {
                $isSigned = TRUE;
            }

            if (isset($scheduleElement["subjectToBlackout"]) === TRUE)
            {
                $subjectToBlackout = TRUE;
            }

            if (isset($scheduleElement["timeApproximate"]) === TRUE)
            {
                $timeApproximate = TRUE;
            }

            if (isset($scheduleElement["liveTapeDelay"]) === TRUE)
            {
                $liveOrTapeDelay = $scheduleElement["liveTapeDelay"];
            }

            $title = $programJSON["titles"][0]["title120"];

            if ($title == NULL OR $title == "")
            {
                debugMSG("FATAL ERROR: Empty title? $programID");
                continue;
            }

            if (isset($programJSON["episodeTitle150"]) === TRUE)
            {
                $subTitle = $programJSON["episodeTitle150"];
            }
            else
            {
                $subTitle = "";
            }

            if (isset($programJSON["descriptions"]["description1000"]) === TRUE)
            {
                $description = $programJSON["descriptions"]["description1000"][0]["description"];
            }
            else
            {
                $description = "";
            }

            if (isset($programJSON["genres"]) === TRUE)
            {
                $category = $programJSON["genres"][0];
            }
            else
            {
                $category = "";
            }

            if (isset($programJSON["metadata"]) === TRUE)
            {
                foreach ($programJSON["metadata"] as $md)
                {
                    if (isset($md["Gracenote"]) === TRUE)
                    {
                        $season = $md["Gracenote"]["season"];
                        $episode = $md["Gracenote"]["episode"];
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

            if ($type == "mv" AND (isset($programJSON["movie"]) === TRUE))
            {
                if (isset($programJSON["movie"]["year"]) === TRUE)
                {
                    $movieYear = $programJSON["movie"]["year"];
                }

                /*
                 * MythTV uses a system where 4 stars would be a "1.0".
                 */

                if (isset($programJSON["movie"]["qualityRating"]) === TRUE)
                {
                    $starRating = $programJSON["movie"]["qualityRating"][0]["rating"] * 0.25;
                }
            }

            if (isset($programJSON["colorCode"]) === TRUE)
            {
                $colorCode = $programJSON["colorCode"];
            }

            if (isset($programJSON["syndicatedEpisodeNumber"]) === TRUE)
            {
                $syndicatedEpisodeNumber = $programJSON["syndicatedEpisodeNumber"];
            }

            if ($isStereo === TRUE)
            {
                $audioprop = "STEREO";
            }

            if ($dolbyType === TRUE)
            {
                $audioprop = "DOLBY";
            }

            if ($isSurround === TRUE)
            {
                $audioprop = "SURROUND";
            }

            if (isset($programJSON["showType"]) === TRUE)
            {
                $showType = $programJSON["showType"];
            }

            if (isset($programJSON["originalAirDate"]) === TRUE)
            {
                $oad = $programJSON["originalAirDate"];
            }

            if ($isLetterboxed === TRUE)
            {
                $videoProperties = "WIDESCREEN";
            }

            if ($isHDTV === TRUE)
            {
                $videoProperties = "HDTV";
            }

            if ($isSigned === TRUE)
            {
                $subtitleTypes = "SIGNED";
            }

            if (isset($scheduleElement["multipart"]) === TRUE)
            {
                $partNumber = $scheduleElement["multipart"]["partNumber"];
                $numberOfParts = $scheduleElement["multipart"]["totalParts"];
            }

            if (isset($programJSON["genres"]) === TRUE)
            {
                $relevance = 0;
                foreach ($programJSON["genres"] as $genre)
                {
                    $insertProgramGenreMyth->execute(array("chanid"    => $chanID,
                                                           "starttime" => $programStartTimeMyth,
                                                           "relevance" => $relevance,
                                                           "genre"     => $genre));
                    $relevance++;
                }
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
                                                            "system"    => $ratingSystem,
                                                            "rating"    => $rating));
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
     * If users start to complain about errors on the insert, it's probably due to a new role type.


    if ($debug AND count($roleTable))
    {
        debugMSG("Role table:");
        debugMSG(print_r($roleTable, TRUE));
    }
    */

    $dbh->exec("DROP TABLE program");
    $dbh->exec("RENAME TABLE t_program TO program");

    $dbh->exec("DROP TABLE credits");
    $dbh->exec("RENAME TABLE t_credits TO credits");

    $dbh->exec("DROP TABLE programrating");
    $dbh->exec("RENAME TABLE t_programrating TO programrating");

    $dbh->exec("DROP TABLE programgenres");
    $dbh->exec("RENAME TABLE t_programgenres TO programgenres");
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

    if (file_exists($tempfile) === TRUE)
    {
        unlink($tempfile);
    }
    mkdir($tempfile);
    if (is_dir($tempfile) === TRUE)
    {
        return $tempfile;
    }
}

function updateStatus()
{
    global $dbhSD;
    global $client;
    global $host;
    global $useServiceAPI;
    global $isMythTV;

    $res = getStatus();

    if ($isMythTV === TRUE)
    {
        $updateLocalMessageTable = $dbhSD->prepare("INSERT INTO messages(id,date,message,type)
    VALUES(:id,:date,:message,:type) ON DUPLICATE KEY UPDATE message=:message,date=:date,type=:type");
    }

    if ($res["code"] == 0)
    {
        $expires = $res["account"]["expires"];
        $maxLineups = $res["account"]["maxLineups"];

        foreach ($res["account"]["messages"] as $a)
        {
            $msgID = $a["msgID"];
            $msgDate = $a["date"];
            $message = $a["message"];
            printMSG("MessageID:$msgID : $msgDate : $message");
            if ($isMythTV === TRUE)
            {
                $updateLocalMessageTable->execute(array("id"      => $msgID, "date" => $msgDate,
                                                        "message" => $message,
                                                        "type"    => "U"));
            }
        }

        foreach ($res["systemStatus"] as $a)
        {
            $date = $a["date"];
            $status = $a["status"];
            $message = $a["message"];

            printMSG(":$date : $status : $message");
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

    $nextConnectTime = date("Y-m-d\TH:i:s\Z", strtotime("tomorrow"));
    printMSG("Server: {$res["serverID"]}");
    printMSG("Last data refresh: {$res["lastDataUpdate"]}");
    printMSG("Account expires: $expires");
    printMSG("Max number of lineups for your account: $maxLineups");
    //printMSG("Next suggested connect time: $nextConnectTime");

    if ($useServiceAPI === TRUE)
    {
        printMSG("Updating settings via the Services API.");
        $request = $client->post("http://$host:6544/Myth/PutSetting", array(),
            array("Key" => "MythFillSuggestedRunTime", "Value" => $nextConnectTime))->send();
        $request = $client->post("http://$host:6544/Myth/PutSetting", array(),
            array("Key" => "DataDirectMessage", "Value" => "Your subscription expires on $expires."))->send();
    }
    else
    {
        if ($isMythTV === TRUE)
        {
            setting("MythFillSuggestedRunTime", $nextConnectTime);
            setting("DataDirectMessage", "Your subscription expires on $expires.");

            $getLastUpdate = settingSD("SchedulesDirectLastUpdate");

            if ($res["lastDataUpdate"] == $getLastUpdate)
            {
                return ("No new data on server.");
            }
            else
            {
                printMSG("Updating settings using MySQL.");
                settingSD("SchedulesDirectLastUpdate", $res["lastDataUpdate"]);
            }
        }
    }

    if ($isMythTV === TRUE)
    {
        if (`which mythutil`)
        {
            exec("mythutil --clearcache"); // Force a clearcache to make sure that everyone is in sync.
        }

        return ("");
    }
}

?>
