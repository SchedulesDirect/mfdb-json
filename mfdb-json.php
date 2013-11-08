#!/usr/bin/php

<?php
/*
 * This file is a grabber which downloads data from Schedules Direct's JSON service.
 */

$isBeta = TRUE;
$debug = TRUE;
$quiet = FALSE;
$printTS = TRUE;

date_default_timezone_set("UTC");
$date = new DateTime();
$todayDate = $date->format("Y-m-d");
$fh_log = fopen("$todayDate.log", "a");

$dlSchedTempDir = tempdir();
printMSG("Temp directory for Schedules is $dlSchedTempDir\n");
$dlProgramTempDir = tempdir();
printMSG("Temp directory for Programs is $dlProgramTempDir\n");
$jsonProgramstoRetrieve = array();

$dbuser = "mythtv";
$dbpassword = "mythtv";
$dbhost = "localhost";
$db = "mythconverg";

$longoptions = array("beta::", "debug::", "help::", "dbhost::", "dbpassword::", "setup::", "dbuser::");

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
            print "The following options are available:\n";
            print "--beta\n";
            print "--help\t(this text)\n";
            print "--dbhost=\t\texample: --host=192.168.10.10\tDefault:$dbhost\n";
            print "--dbuser=\t\tUsername to connect to MythTV database\tDefault:$dbuser\n";
            print "--dbpassword=\tPassword to access MythtTV database\tDefault:$dbpassword\n";
            exit;
        case "host":
            $dbhost = $v;
            break;
        case "password":
            $dbpassword = $v;
            break;
        case "user":
            $dbuser = $v;
            break;
    }
}

printMSG("Connecting to database.\n");
try
{
    $dbh = new PDO("mysql:host=$dbhost;dbname=$db;charset=utf8", $dbuser, $dbpassword,
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
    printMSG("Using beta server.\n");
    # API must match server version.
    $api = 20131021;
}
else
{
    $baseurl = "https://data2.schedulesdirect.org";
    printMSG("Using production server.\n");
    $api = 20130709;
}

$stmt = $dbh->prepare("SELECT userid,password FROM videosource LIMIT 1");
$stmt->execute();
$result = $stmt->fetchAll(PDO::FETCH_ASSOC);

$sdUsername = $result[0]["userid"];
$sdPassword = sha1($result[0]["password"]);

$globalStartTime = time();
$globalStartDate = new DateTime();

printMSG("Retrieving list of channels to download.\n");
$stmt = $dbh->prepare("SELECT DISTINCT(xmltvid) FROM channel WHERE visible=TRUE");
$stmt->execute();
$stationIDs = $stmt->fetchAll(PDO::FETCH_COLUMN);

printMSG("Logging into Schedules Direct.\n");
$randHash = getRandhash($sdUsername, $sdPassword);

if ($randHash != "ERROR")
{
    $success = getSchedules($stationIDs);
}
else
{
    print "Error connecting to Schedules Direct.\n";
    exit;
}

if ($success)
{
    insertJSON();
    insertSchedule();
}

printMSG("Global. Start Time:" . date("Y-m-d H:i:s", $globalStartTime) . "\n");
printMSG("Global. End Time:" . date("Y-m-d H:i:s") . "\n");
$globalSinceStart = $globalStartDate->diff(new DateTime());
if ($globalSinceStart->h)
{
    printMSG($globalSinceStart->h . " hour ");
}
printMSG($globalSinceStart->i . " minutes " . $globalSinceStart->s . " seconds.\n");

printMSG("Done.\n");

function getSchedules(array $stationIDs)
{
    global $dbh;
    global $api;
    global $randHash;
    global $dlProgramTempDir;
    global $dlSchedTempDir;

    $dbProgramCache = array();

    $downloadedStationIDs = array();
    $serverScheduleMD5 = array();

    printMSG("Sending schedule request.\n");
    $res = array();
    $res["action"] = "get";
    $res["object"] = "schedules";
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = $stationIDs;

    $response = sendRequest(json_encode($res));

    $res = array();
    $res = json_decode($response, TRUE);

    if ($res["response"] == "OK")
    {
        $fileName = $res["filename"];
        $url = $res["URL"];
        file_put_contents("$dlSchedTempDir/$fileName", file_get_contents($url));

        $zipArchive = new ZipArchive();
        $result = $zipArchive->open("$dlSchedTempDir/$fileName");
        if ($result === TRUE)
        {
            $zipArchive->extractTo("$dlSchedTempDir");
            $zipArchive->close();

            foreach (glob("$dlSchedTempDir/sched_*.json.txt") as $f)
            {
                $a = json_decode(file_get_contents($f), TRUE);
                $stationID = $a["stationID"];
                $downloadedStationIDs[] = $stationID;

                foreach ($a["programs"] as $v)
                {
                    $serverScheduleMD5[$v["md5"]] = $v["programID"];
                }
            }
        }
        else
        {
            printMSG("FATAL: Could not open zip file.\n");
            exit;
        }
    }

    printMSG("There are " . count($serverScheduleMD5) . " programIDs in the upcoming schedule.\n");
    printMSG("Retrieving existing MD5 values.\n");

    /*
     * We're going to figure out which programIDs we need to download.
     */

    $stmt = $dbh->prepare("SELECT md5, programID FROM SDprogramCache");
    $stmt->execute();
    $dbProgramCache = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

    $jsonProgramstoRetrieve = array_diff_key($serverScheduleMD5, $dbProgramCache);

    /*
     * Now we've got an array of programIDs that we need to download in $toRetrieve,
     * either because we didn't have them, or they have different md5's.
     */

    printMSG("Need to download " . count($jsonProgramstoRetrieve) . " new or updated programs.\n");

    if (count($jsonProgramstoRetrieve) > 10000)
    {
        printMSG("Requesting more than 10000 programs. Please be patient.\n");
    }

    if (count($jsonProgramstoRetrieve))
    {
        printMSG("Requesting new and updated programs.\n");
        $res = array();
        $res["action"] = "get";
        $res["object"] = "programs";
        $res["randhash"] = $randHash;
        $res["api"] = $api;
        $res["request"] = $jsonProgramstoRetrieve;

        $response = sendRequest(json_encode($res));

        $res = array();
        $res = json_decode($response, TRUE);

        if ($res["response"] == "OK")
        {
            printMSG("Downloading program file.\n");

            $fileName = $res["filename"];
            $url = $res["URL"];
            file_put_contents("$dlProgramTempDir/$fileName", file_get_contents($url));

            $zipArchive = new ZipArchive();
            $result = $zipArchive->open("$dlProgramTempDir/$fileName");
            if ($result === TRUE)
            {
                $zipArchive->extractTo("$dlProgramTempDir");
                $zipArchive->close();
            }
            else
            {
                printMSG("FATAL: Could not open .zip file while extracting programIDs.\n");
                exit;
            }
        }
        else
        {
            printMSG("Error getting programs:\n" . var_dump($res));
            exit;
        }
    }

    return (TRUE);
}

function insertJSON()
{
    global $dbh;
    global $jsonProgramstoRetrieve;
    global $dlProgramTempDir;
    global $debug;

    $counter = 0;
    printMSG("Performing inserts of JSON data.\n");

    $insertJSON = $dbh->prepare("INSERT INTO SDprogramCache(programID,md5,json)
            VALUES (:programID,:md5,:json)
            ON DUPLICATE KEY UPDATE md5=:md5, json=:json");

    $insertPersonSD = $dbh->prepare("INSERT INTO peopleSD(personID,name) VALUES(:personID, :name)
        ON DUPLICATE KEY UPDATE name=:name");

    $insertCreditSD = $dbh->prepare("INSERT INTO creditsSD(personID,programID,role)
    VALUES(:personID,:pid,:role)");

    $insertProgramGenresSD = $dbh->prepare("INSERT INTO programgenresSD(programID,relevance,genre)
    VALUES(:pid,:relevance,:genre) ON DUPLICATE KEY UPDATE genre=:genre");

    $peopleCache = array();
    $getPeople = $dbh->prepare("SELECT name,personID FROM peopleSD");
    $getPeople->execute();
    $peopleCache = $getPeople->fetchAll(PDO::FETCH_KEY_PAIR);

    $total = count($jsonProgramstoRetrieve);

    foreach ($jsonProgramstoRetrieve as $md5 => $pid)
    {
        $counter++;
        if ($counter % 1000)
        {
            printMSG("$counter / $total             \r");
        }

        $fileJSON = file_get_contents("$dlProgramTempDir/$pid.json.txt");

        if ($fileJSON === FALSE)
        {
            printMSG("*** ERROR: Could not open file $dlProgramTempDir/$pid.json.txt\n");
            continue;
        }

        $insertJSON->execute(array("programID" => $pid, "md5" => $md5,
                                   "json"      => $fileJSON));

        $jsonProgram = json_decode($fileJSON, TRUE);

        if (json_last_error())
        {
            printMSG("*** ERROR: JSON decode error $dlProgramTempDir/$pid.json.txt\n");
            printMSG("$fileJSON\n");
            continue;
        }

        if (isset($jsonProgram["castAndCrew"]))
        {
            foreach ($jsonProgram["castAndCrew"] as $credit)
            {
                $role = $credit["role"];
                $personID = $credit["personID"];
                $name = $credit["name"];

                $insertPersonSD->execute(array("personID" => (int)$personID, "name" => $name));

                $insertCreditSD->execute(array("personID" => (int)$personID, "pid" => $pid,
                                               "role"     => $role));
            }
        }

        if (isset($jsonProgram["genres"]))
        {
            foreach ($jsonProgram["genres"] as $relevance => $genre)
            {
                $insertProgramGenresSD->execute(array("pid"       => $pid,
                                                      "relevance" => $relevance, "genre" => $genre));
            }
        }

        if ($debug == FALSE)
        {
            unlink("$dlProgramTempDir/$pid.json.txt");
        }

    }

    if ($debug == FALSE)
    {
        unlink("$dlProgramTempDir/serverID.txt");
        rmdir("$dlProgramTempDir");
    }

    printMSG("Completed local database program updates.\n");
}

function insertSchedule()
{
    global $dbh;
    global $dlSchedTempDir;

    printMSG("Inserting schedules.\n");

    $counter = 0;

    $stmt = $dbh->exec("DROP TABLE IF EXISTS t_scheduleSD");
    $stmt = $dbh->exec("CREATE TABLE t_scheduleSD LIKE scheduleSD");

    $stmt = $dbh->exec("DROP TABLE IF EXISTS t_program");
    $stmt = $dbh->exec("CREATE TABLE t_program LIKE program");

    $insertScheduleSD = $dbh->prepare("INSERT INTO t_scheduleSD(stationID,programID,md5,air_datetime,duration,
    previouslyshown,closecaptioned,partnumber,parttotal,first,last,dvs,new,educational,hdtv,3d,letterbox,stereo,
    dolby,dubbed,dubLanguage,subtitled,subtitleLanguage,sap,sapLanguage,programLanguage,tvRatingSystem,tvRating,
    dialogRating,languageRating,sexualContentRating,violenceRating,fvRating)
    VALUES(:stationID,:programID,:md5,:air_datetime,:duration,
    :previouslyshown,:closecaptioned,:partnumber,:parttotal,:first,:last,:dvs,:new,:educational,:hdtv,:3d,
    :letterbox,:stereo,:dolby,:dubbed,:dubLanguage,:subtitled,:subtitleLanguage,:sap,:sapLanguage,:programLanguage,
    :ratingSystem,:tvRating,:dialogRating,:languageRating,:sexualContentRating,:violenceRating,:fvRating)");

    $insertSchedule = $dbh->prepare("INSERT INTO t_program(chanid,starttime,endtime,title,subtitle,description,
    category,category_type,airdate,stars,previouslyshown,stereo,subtitled,hdtv,closecaptioned,partnumber,parttotal,
    seriesid,originalairdate,showtype,colorcode,syndicatedepisodenumber,programid,generic,listingsource,first,last,
    audioprop,subtitletypes,videoprop)

    VALUES(:chanid,:starttime,:endtime,:title,:subtitle,:description,:category,:category_type,:airdate,:stars,
    :previouslyshown,:stereo,:subtitled,:hdtv,:closecaptioned,:partnumber,:parttotal,
    :seriesid,:originalairdate,:showtype,:colorcode,:syndicatedepisodenumber,:programid,:generic,:listingsource,
    :first,:last,:audioprop,:subtitletypes,:videoprop)");

    $getExistingChannels = $dbh->prepare("SELECT chanid,sourceid,xmltvid FROM channel WHERE visible=1");
    $getExistingChannels->execute();

    $existingChannels = $getExistingChannels->fetchAll(PDO::FETCH_ASSOC);

    $getProgramInformation = $dbh->prepare("SELECT json FROM SDprogramCache WHERE programID=:pid");

    foreach ($existingChannels as $channel)
    {
        $chanID = $channel["chanid"];
        $sourceID = $channel["sourceid"];
        $stationID = $channel["xmltvid"];

        printMSG("c:$chanID sou:$sourceID sid:$stationID\n");

        $a = json_decode(file_get_contents("$dlSchedTempDir/sched_$stationID.json.txt"), TRUE);

        $dbh->beginTransaction();

        foreach ($a["programs"] as $v)
        {
            /*
             * A few things need to be set to non-null, so declare them here. Also, quiets some warnings.
             */
            $ratingSystem = "";
            $rating = "";
            $movieYear = "";
            $starRating = 0;
            $colorCode = "";
            $syndicatedEpisodeNumber = "";
            $showType = "";
            $oad = NULL;
            $audioprop = "";

            /*
             * These are updated in another part of mfdb?
             */
            $isFirst = 0;
            $isLast = 0;


            $programID = $v["programID"];
            $getProgramInformation->execute(array("pid" => $programID));
            $programJSON = json_decode($getProgramInformation->fetchColumn(), TRUE);

            if (json_last_error())
            {
                printMSG("Error retrieving / decoding $programID from local database.\n");
                continue;
            }

            $md5 = $v["md5"];
            $air_datetime = $v["airDateTime"];
            $duration = $v["duration"];

            $programStartTimeMyth = str_replace("T", " ", $air_datetime);
            $programStartTimeMyth = rtrim($programStartTimeMyth, "Z");
            $programEndTimeMyth = gmdate("Y-m-d H:i:s", strtotime("$air_datetime + $duration seconds"));

            if (isset($v["new"]))
            {
                $isNew = TRUE;
                $previouslyshown = FALSE;
            }
            else
            {
                $isNew = FALSE;
                $previouslyshown = TRUE;
            }

            if (isset($v["cc"]))
            {
                $isClosedCaption = TRUE;
            }
            else
            {
                $isClosedCaption = FALSE;
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

            if (isset($v["isPremiereOrFinale"]))
            {
                switch ($v["isPremiereOrFinale"])
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

            if (isset($v["dvs"]))
            {
                $dvs = TRUE;
            }
            else
            {
                $dvs = FALSE;
            }

            if (isset($v["educational"]))
            {
                $isEducational = TRUE;
            }
            else
            {
                $isEducational = FALSE;
            }

            if (isset($v["hdtv"]))
            {
                $isHDTV = TRUE;
            }
            else
            {
                $isHDTV = FALSE;
            }

            if (isset($v["is3d"]))
            {
                $is3d = TRUE;
            }
            else
            {
                $is3d = FALSE;
            }

            if (isset($v["letterbox"]))
            {
                $isLetterboxed = TRUE;
            }
            else
            {
                $isLetterboxed = FALSE;
            }

            if (isset($v["stereo"]))
            {
                $isStereo = TRUE;
            }
            else
            {
                $isStereo = FALSE;
            }

            if (isset($v["dolby"]))
            {
                $dolbyType = $v["dolby"];
            }
            else
            {
                $dolbyType = NULL;
            }

            if (isset($v["dubbed"]))
            {
                $dubbed = TRUE;
            }
            else
            {
                $dubbed = FALSE;
            }

            if (isset($v["dubbedLanguage"]))
            {
                $dubbedLanguage = $v["dubbedLanguage"];
            }
            else
            {
                $dubbedLanguage = NULL;
            }

            if ($dubbed AND $dubbedLanguage == NULL)
            {
                $quiet = TRUE;
                printMSG("*** Warning: $programID has dub but no dubbed language set.\n");
                $quiet = FALSE;
            }

            if (isset($v["subtitled"]))
            {
                $isSubtitled = TRUE;
            }
            else
            {
                $isSubtitled = FALSE;
            }

            if (isset($v["subtitledLanguage"]))
            {
                $subtitledLanguage = $v["subtitledLanguage"];
            }
            else
            {
                $subtitledLanguage = NULL;
            }
            if ($isSubtitled AND $subtitledLanguage == NULL)
            {
                $quiet = TRUE;
                printMSG("*** Warning: $programID has subtitle but no subtitled language set.\n");
                $quiet = FALSE;
            }

            if (isset($v["sap"]))
            {
                $sap = TRUE;
            }
            else
            {
                $sap = FALSE;
            }

            if (isset($v["sapLanguage"]))
            {
                $sapLanguage = $v["sapLanguage"];
            }
            else
            {
                $sapLanguage = NULL;
            }

            if ($sap AND $sapLanguage == NULL)
            {
                $quiet = TRUE;
                printMSG("*** Warning: $programID has SAP but no SAP language set.\n");
                $quiet = FALSE;
            }

            if (isset($v["programLanguage"]))
            {
                $programLanguage = $v["programLanguage"];
            }
            else
            {
                $programLanguage = NULL;
            }

            if (isset($v["tvRating"]))
            {
                $ratingSystem = "V-CHIP";
                $rating = $v["tvRating"];
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

            $title = $programJSON["titles"]["title120"];

            if (isset($programJSON["episodeTitle150"]))
            {
                $subTitle = $programJSON["episodeTitle150"];
            }
            else
            {
                $subTitle = "";
            }

            if (isset($programJSON["descriptions"]["description255"]))
            {
                $description = $programJSON["descriptions"]["description255"];
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

            $isGeneric = FALSE;
            $seriesID = "";
            $type = strtolower(substr($programID, 0, 2));
            switch ($type)
            {
                case "sh":
                    $categoryType = "series";
                    $isGeneric = TRUE;
                    $seriesID = "EP" .substr($programJSON, 2, 8);
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
                    printMSG("FATAL ERROR: $programID has unknown type.\n");
                    exit;
                    break;
            }

            if ($type == "mv" AND isset($programJSON["movie"]))
            {
                if (isset($programJSON["movie"]["year"]))
                {
                    $movieYear = $programJSON["movie"]["year"];
                }

                if (isset($programJSON["movie"]["starRating"]))
                {
                    $starRating = substr_count($programJSON["movie"]["starRating"], "*") +
                        (.5 * substr_count($programJSON["movie"]["starRating"], "+"));
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

            if (isset($programJSON["showType"]))
            {
                $showType = $programJSON["showType"];
            }

            if (isset($programJSON["originalAirDate"]))
            {
                $oad = $programJSON["originalAirDate"];
            }

            $subtitleTypes = "";
            $videoProperties = "";

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

            $insertSchedule = $dbh->prepare("INSERT INTO t_program(chanid,starttime,endtime,title,subtitle,description,
    category,category_type,airdate,stars,previouslyshown,stereo,subtitled,hdtv,closecaptioned,partnumber,parttotal,
    seriesid,originalairdate,showtype,colorcode,syndicatedepisodenumber,programid,generic,listingsource,first,last,
    audioprop,subtitletypes,videoprop)

    VALUES(:chanid,:starttime,:endtime,:title,:subtitle,:description,:category,:category_type,:airdate,:stars,
    :previouslyshown,:stereo,:subtitled,:hdtv,:closecaptioned,:partnumber,:parttotal,
    :seriesid,:originalairdate,:showtype,:colorcode,:syndicatedepisodenumber,:programid,:generic,:listingsource,
    :first,:last,:audioprop,:subtitletypes,:videoprop)");

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
        }

        $dbh->commit();
    }

    printMSG("\nDone inserting schedules.\n");
    $stmt = $dbh->exec("DROP TABLE scheduleSD");
    $stmt = $dbh->exec("RENAME TABLE t_scheduleSD TO scheduleSD");

    $stmt = $dbh->exec("DROP TABLE program");
    $stmt = $dbh->exec("RENAME TABLE t_program TO program");
}

function getRandhash($username, $password)
{
    global $api;
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
        printMSG("JSON decode error:\n");
        var_dump($response);
        exit;
    }

    if ($res["response"] == "OK")
    {
        return $res["randhash"];
    }

    printMSG("Response from schedulesdirect: $response\n");

    return "ERROR";
}

function sendRequest($jsonText)
{
    /*
     * Retrieving 42k program objects took 8 minutes. Once everything is in a steady state, you're not going to be
     * having that many objects that need to get pulled. Set timeout for 15 minutes.
     */

    global $baseurl;

    $data = http_build_query(array("request" => $jsonText));

    $context = stream_context_create(array('http' =>
                                               array(
                                                   'method'  => 'POST',
                                                   'header'  => 'Content-type: application/x-www-form-urlencoded',
                                                   'timeout' => 900,
                                                   'content' => $data
                                               )
    ));

    return rtrim(file_get_contents("$baseurl/handleRequest.php", false, $context));
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

function printMSG($str)
{
    global $fh_log;
    global $quiet;
    global $printTS;

    if ($printTS)
    {
        $str = date("H:i:s") . ":$str";
    }

    if (!$quiet)
    {
        print "$str";
    }

    $str = str_replace("\r", "\n", $str);
    fwrite($fh_log, $str);
}

?>