#!/usr/bin/php

<?php
/*
 * This file is a grabber which downloads data from Schedules Direct's JSON service.
 * Robert Kulagowski
 * grabber@schedulesdirect.org
 *
 */

/*
 * We need a bit of memory to process schedules, so request it right at the beginning.
 */

ini_set("memory_limit", "768M");

require_once "vendor/autoload.php";
require_once "functions.php";
use Guzzle\Http\Client;

$isBeta = TRUE;
$debug = FALSE;
$quiet = FALSE;
$forceDownload = FALSE;
$sdStatus = "";
$printTimeStamp = TRUE;
$scriptVersion = "0.01";
$scriptDate = "2014-08-07";
$maxProgramsToGet = 2000;
$errorWarning = FALSE;
$station = "";
$useServiceAPI = FALSE;
$isMythTV = TRUE;
$tz = "UTC";

$agentString = "mfdb-json.php developer grabber v$scriptVersion/$scriptDate";

date_default_timezone_set($tz);
$date = new DateTime();
$todayDate = $date->format("Y-m-d");

$fh_log = fopen("$todayDate.log", "a");
$fh_error = fopen("$todayDate.debug.log", "a");

$jsonProgramsToRetrieve = array();
$peopleCache = array();

$dbUser = "mythtv";
$dbPassword = "mythtv";
$dbHost = "localhost";
$dbName = "mythconverg";
$host = "localhost";

$helpText = <<< eol
The following options are available:
--beta
--help\t\t(this text)
--dbname=\tMySQL database name. (Default: $dbName)
--dbuser=\tUsername for database access. (Default: $dbUser)
--dbpassword=\tPassword for database access. (Default: $dbPassword)
--dbhost=\tMySQL database hostname. (Default: $dbHost)
--force\t\tForce download of schedules. (Default: FALSE)
--host=\t\tIP address of the MythTV backend. (Default: $host)
--nomyth\tDon't execute any MythTV specific functions. (Default: FALSE)
--max=\t\tMaximum number of programs to retrieve per request. (Default:$maxProgramsToGet)
--quiet\t\tDon't print to screen; put all output into the logfile.
--station=\tDownload the schedule for a single stationID in your lineup.
--timezone=\tSet the timezone for log file timestamps. See http://www.php.net/manual/en/timezones.php (Default:$tz)
eol;
/*'*/

$longoptions = array("beta", "debug", "help", "host::", "dbname::", "dbuser::", "dbpassword::", "dbhost::",
                     "force", "test", "nomyth", "max::", "quiet", "station::", "timezone::");
$options = getopt("h::", $longoptions);

foreach ($options as $k => $v)
{
    switch ($k)
    {
        case "beta":
            $isBeta = TRUE;
            break;
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
        case "test":
            $test = TRUE;
            break;
        case "nomyth":
            $isMythTV = FALSE;
            break;
        case "max":
            $maxProgramsToGet = $v;
            break;
        case "quiet":
            $quiet = TRUE;
            break;
        case "station":
            $station = $v;
            break;
        case "timezone":
            date_default_timezone_set($v);
            break;
    }
}

printMSG("$agentString");

$dlSchedTempDir = tempdir();
printMSG("Temp directory for Schedules is $dlSchedTempDir");
$dlProgramTempDir = tempdir();
printMSG("Temp directory for Programs is $dlProgramTempDir");

if ($isMythTV)
{
    printMSG("Connecting to MythTV database.");
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
}

if ($isBeta)
{
    # Test server. Things may be broken there.
    $baseurl = "http://ec2-54-86-226-234.compute-1.amazonaws.com/20140530/";
    printMSG("Using beta server.");
    # API must match server version.
    $api = 20140530;
}
else
{
    $baseurl = "https://json.schedulesdirect.org/20131021/";
    printMSG("Using production server.");
    $api = 20131021;
}

$client = new Guzzle\Http\Client($baseurl);
$client->setUserAgent($agentString);

$useServiceAPI = checkForServiceAPI();

if ($isMythTV)
{
    $userLoginInformation = getSchedulesDirectLoginFromDB();
    $responseJSON = json_decode($userLoginInformation, TRUE);
    $sdUsername = $responseJSON["username"];
    $sdPassword = sha1($responseJSON["password"]);

    if ($sdUsername == "")
    {
        printMSG("FATAL: Could not read Schedules Direct login information from settings table.");
        printMSG("Did you run the sd-utility.php program yet?");
        exit;
    }
}

$globalStartTime = time();
$globalStartDate = new DateTime();

if ($station == "" AND $isMythTV)
{
    printMSG("Retrieving list of channels to download.");
    $stmt = $dbh->prepare("SELECT CAST(xmltvid AS UNSIGNED) FROM channel WHERE visible=TRUE
AND xmltvid != '' AND xmltvid > 0 GROUP BY xmltvid");
    $stmt->execute();
    $stationIDs = $stmt->fetchAll(PDO::FETCH_COLUMN);
}

if ($station != "" AND $isMythTV)
{
    printMSG("Downloading data only for $station");
    $stmt = $dbh->prepare("SELECT CAST(xmltvid AS UNSIGNED) FROM channel WHERE xmltvid=:station");
    $stmt->execute(array("station" => $station));
    $stationIDs = $stmt->fetchAll(PDO::FETCH_COLUMN);
}

if (!$isMythTV)
{
    /*
     * Stub. Read in a configuration file that contains username, password and stationIDs.
     */

    printMSG("Opening sd.conf");
    exit;
}

printMSG("Logging into Schedules Direct.");
$token = getToken($sdUsername, $sdPassword);

if ($token == "ERROR")
{
    printMSG("Got error when attempting to retrieve token from Schedules Direct.");
    printMSG("Check username / password in settings table.");
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
    $jsonProgramsToRetrieve = getSchedules($stationIDs);
}
else
{
    debugMSG("Error connecting to Schedules Direct.");
    $statusMessage = "Error connecting to Schedules Direct.";
}

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

printMSG("Status:$statusMessage");

$globalStartTime = date("Y-m-d H:i:s", $globalStartTime);
$globalEndTime = date("Y-m-d H:i:s");

printMSG("Global. Start Time:$globalStartTime");
printMSG("Global. End Time:$globalEndTime");
$globalSinceStart = $globalStartDate->diff(new DateTime());
if ($globalSinceStart->h)
{
    printMSG($globalSinceStart->h . " hour ");
}
printMSG($globalSinceStart->i . " minutes " . $globalSinceStart->s . " seconds.");

printMSG("Updating status.");

$stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunStart' AND hostname IS NULL");
$stmt->execute(array("data" => $globalStartTime));

$stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunEnd' AND hostname IS NULL");
$stmt->execute(array("data" => $globalEndTime));

$stmt = $dbh->prepare("UPDATE settings SET data=:data WHERE value='mythfilldatabaseLastRunStatus' AND hostname IS NULL");
$stmt->execute(array("data" => $statusMessage));

printMSG("Sending reschedule request to mythbackend.");

exec("mythutil --resched");

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
    global $dlProgramTempDir;
    global $dlSchedTempDir;
    global $maxProgramsToGet;
    global $quiet;
    global $debug;

    $dbProgramCache = array();
    $response = "";

    $downloadedStationIDs = array();
    $serverScheduleMD5 = array();

    printMSG("Sending schedule request.");

    // $body["request"] = $stationIDs;

    try
    {
        //$response = $client->post("schedules", array("token" => $token, "Accept-Encoding" => "deflate,gzip"),
//            json_encode($body))->send();

        $response = $client->post("schedules", array("token" => $token, "Accept-Encoding" => "deflate,gzip"),
            json_encode($stationIDsToFetch))->send();



    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        if ($e->getCode() == 400)
        {
            return ("ERROR");
        }
    }

    $resBody = $response->getBody();

    /*
     * Keep a copy for troubleshooting.
     */

    printMSG("Writing to $dlSchedTempDir/schedule.json");

    file_put_contents("$dlSchedTempDir/schedule.json", $resBody);

    $f = file("$dlSchedTempDir/schedule.json", FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

    foreach ($f as $json)
    {
        $item = json_decode($json, TRUE);
        $stationID = $item["stationID"];
        $downloadedStationIDs[] = $stationID;

        printMSG("Parsing schedule for stationID:$stationID");

        foreach ($item["programs"] as $programData)
        {
            if (array_key_exists("md5", $programData))
            {
                $serverScheduleMD5[$programData["md5"]] = $programData["programID"];
            }
            else
            {
                $quiet = FALSE;
                printMSG("FATAL ERROR: no MD5 value for program. Open ticket with Schedules Direct.");
                printMSG("s:$stationID\n\njson:$json\n\nitem\n\n" . print_r($item, TRUE) . "\n\n");
                exit;
            }
        }
    }

    printMSG("There are " . count($serverScheduleMD5) . " programIDs in the upcoming schedule.");
    printMSG("Retrieving existing MD5 values.");

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
        $quiet = TRUE;
        printMSG("dbProgramCache is");
        printMSG(print_r($dbProgramCache, TRUE));
        printMSG("serverScheduleMD5 is");
        printMSG(print_r($serverScheduleMD5, TRUE));
        printMSG("jsonProrgamstoRetrieve is");
        printMSG(print_r($jsonProgramsToRetrieve, TRUE));
        $quiet = FALSE;
    }

    $toRetrieveTotal = count($jsonProgramsToRetrieve);

    /*
     * Now we've got an array of programIDs that we need to download in $toRetrieve,
     * either because we didn't have them, or they have different md5's.
     */

    printMSG("Need to download $toRetrieveTotal new or updated programs.");

    if ($toRetrieveTotal > 10000)
    {
        printMSG("Requesting more than 10000 programs. Please be patient.");
    }

    printMSG("Maximum programs we're downloading per call: $maxProgramsToGet");

    if (count($jsonProgramsToRetrieve))
    {
        $totalChunks = intval($toRetrieveTotal / $maxProgramsToGet);

        $counter = 0;

        for ($i = 0; $i <= $totalChunks; $i++)
        {
            printMSG("Retrieving chunk " . ($i + 1) . " of " . ($totalChunks + 1) . ".");
            $startOffset = $i * $maxProgramsToGet;
            $chunk = array_slice($jsonProgramsToRetrieve, $startOffset, $maxProgramsToGet);

            $body["request"] = $chunk;

            $counter += count($chunk);

            $request = $client->post("programs", array("token" => $token, "Accept-Encoding" => "deflate,gzip"),
                json_encode($body));
            $response = $request->send();

            $resBody = $response->getBody();

            file_put_contents("$dlProgramTempDir/programs." . substr("00$i", -2) . ".json", $resBody);
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

            $jsonProgram = json_decode($item, TRUE);

            if (json_last_error())
            {
                debugMSG("*** ERROR: JSON decode error $jsonFileToProcess");
                debugMSG(print_r($item, TRUE));
                continue;
            }

            $pid = $jsonProgram["programID"];
            $md5 = $jsonProgram["md5"];

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
                        $role = $credit["role"];
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
                        $role = $credit["role"];
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

    $dbh->exec("DROP TABLE IF EXISTS t_SDschedule");
    $dbh->exec("CREATE TABLE t_SDschedule LIKE SDschedule");

    $dbh->exec("DROP TABLE IF EXISTS t_program");
    $dbh->exec("CREATE TABLE t_program LIKE program");

    $dbh->exec("DROP TABLE IF EXISTS t_credits");
    $dbh->exec("CREATE TABLE t_credits LIKE credits");

    $dbh->exec("DROP TABLE IF EXISTS t_programrating");
    $dbh->exec("CREATE TABLE t_programrating LIKE programrating");

    $stmt = $dbh->prepare("SELECT data FROM settings WHERE value='DBSchemaVer'");
    $stmt->execute();
    $dbSchema = $stmt->fetchColumn();

    $insertScheduleSD = $dbh->prepare("INSERT IGNORE INTO t_SDschedule(stationID,programID,md5,air_datetime,duration,
    previouslyshown,closecaptioned,partnumber,parttotal,first,last,dvs,new,educational,hdtv,3d,letterbox,stereo,
    dolby,dubbed,dubLanguage,subtitled,subtitleLanguage,sap,sapLanguage,programLanguage,tvRatingSystem,tvRating,
    dialogRating,languageRating,sexualContentRating,violenceRating,fvRating)
    VALUES(:stationID,:programID,:md5,:air_datetime,:duration,
    :previouslyshown,:closecaptioned,:partnumber,:parttotal,:first,:last,:dvs,:new,:educational,:hdtv,:3d,
    :letterbox,:stereo,:dolby,:dubbed,:dubLanguage,:subtitled,:subtitleLanguage,:sap,:sapLanguage,:programLanguage,
    :ratingSystem,:tvRating,:dialogRating,:languageRating,:sexualContentRating,:violenceRating,:fvRating)");

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

    $jsonSchedule = array();

    foreach (glob("$dlSchedTempDir/*.json") as $jsonFileToProcess)
    {
        $scheduleTemp = file($jsonFileToProcess);
    }

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
     * Now that we're done, reset the array to empty.
     */
    $scheduleTemp = array();

    while (list(, $item) = each($existingChannels))
    {
        $chanID = $item["chanid"];
        $sourceID = $item["sourceid"];
        $stationID = $item["xmltvid"];
        printMSG("Inserting schedule for chanid:$chanID sourceid:$sourceID xmltvid:$stationID");

        $dbh->beginTransaction();

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

            $title = $programJSON["titles"]["title120"];

            if ($title == NULL OR $title == "")
            {
                debugMSG("FATAL ERROR: Empty title? $programID");
                exit;
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
                    exit;
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

            /* Bypass the SD-specific insert for now; might be easier to just parse the JSON from the programCache.
                        try
                        {
                            $insertScheduleSD->execute(array(
                                "stationID"           => $stationID,
                                "programID"           => $programID,
                                "md5"                 => $md5,
                                "air_datetime"        => $air_datetime,
                                "duration"            => $duration,
                                "previouslyshown"     => $previouslyshown,
                                "closecaptioned"      => $isClosedCaption,
                                "partnumber"          => $partNumber,
                                "parttotal"           => $numberOfParts,
                                "first"               => $isFirst,
                                "last"                => $isLast,
                                "dvs"                 => $dvs,
                                "new"                 => $isNew,
                                "educational"         => $isEducational,
                                "hdtv"                => $isHDTV,
                                "3d"                  => $is3d,
                                "letterbox"           => $isLetterboxed,
                                "stereo"              => $isStereo,
                                "dolby"               => $dolbyType,
                                "dubbed"              => $dubbed,
                                "dubLanguage"         => $dubbedLanguage,
                                "subtitled"           => $isSubtitled,
                                "subtitleLanguage"    => $subtitledLanguage,
                                "sap"                 => $sap,
                                "sapLanguage"         => $sapLanguage,
                                "programLanguage"     => $programLanguage,
                                "ratingSystem"        => $ratingSystem,
                                "tvRating"            => $rating,
                                "dialogRating"        => $dialogRating,
                                "languageRating"      => $languageRating,
                                "sexualContentRating" => $sexRating,
                                "violenceRating"      => $violenceRating,
                                "fvRating"            => $fvRating));
                        } catch (PDOException $e)
                        {
                            print "Exception: " . $e->getMessage();
                            $debug = TRUE;
                            var_dump($programJSON);
                        }
            */
            if (isset($programJSON["castAndCrew"]))
            {
                foreach ($programJSON["castAndCrew"] as $credit)
                {
                    $role = strtolower($credit["role"]);
                    /*
                     * MythTV has hardcoded maps of roles because it uses a set during the create table.
                     */
                    switch ($role)
                    {
                        case "executive producer":
                            $role = "executive_producer";
                            break;
                        case "guest star":
                            $role = "guest_star";
                            break;
                        case "musical guest":
                            $role = "musical_guest";
                            break;
                    }

                    $roleTable[$role] = 1;

                    try
                    {
                        $insertCreditMyth->execute(array("person"    => $peopleCache[$credit["name"]],
                                                         "chanid"    => $chanID,
                                                         "starttime" => $programStartTimeMyth,
                                                         "role"      => $role));
                    } catch (PDOException $e)
                    {
                        print "Exception: " . $e->getMessage();
                        $debug = TRUE;
                        var_dump($programJSON);
                    }
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
     * If users start to complain about errors on the insert, it's probably due to a new role type.
     */

    if ($debug AND count($roleTable))
    {
        debugMSG("Role table:");
        debugMSG(print_r($roleTable, TRUE));
    }

    printMSG("Done inserting schedules.");
    $dbh->exec("DROP TABLE SDschedule");
    $dbh->exec("RENAME TABLE t_SDschedule TO SDschedule");

    $dbh->exec("DROP TABLE program");
    $dbh->exec("RENAME TABLE t_program TO program");

    $dbh->exec("DROP TABLE credits");
    $dbh->exec("RENAME TABLE t_credits TO credits");

    $dbh->exec("DROP TABLE programrating");
    $dbh->exec("RENAME TABLE t_programrating TO programrating");
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

function updateStatus()
{
    global $dbh;
    global $client;
    global $host;
    global $useServiceAPI;

    $res = getStatus();

    $updateLocalMessageTable = $dbh->prepare("INSERT INTO SDMessages(id,date,message,type)
    VALUES(:id,:date,:message,:type) ON DUPLICATE KEY UPDATE message=:message,date=:date,type=:type");

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
            $updateLocalMessageTable->execute(array("id"   => $msgID, "date" => $msgDate, "message" => $message,
                                                    "type" => "U"));
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

    exec("mythutil --clearcache"); // Force a clearcache to make sure that everyone is in sync.

    return ("");
}

?>
