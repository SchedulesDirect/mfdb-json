#!/usr/bin/php

<?php
/*
 * This file is a grabber which downloads data from Schedules Direct's JSON service.
 */

$isBeta = FALSE;
$debug = TRUE;
$quiet = FALSE;

date_default_timezone_set("UTC");
$date = new DateTime();
$todayDate = $date->format("Y-m-d");
$fh_log = fopen("$todayDate.log", "a");

$user = "mythtv";
$password = "mythtv";
$host = "localhost";
$db = "mythconverg";

$longoptions = array("beta::", "debug::", "help::", "host::", "password::", "setup::", "user::");

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
            print "--host=\t\texample: --host=192.168.10.10\n";
            print "--user=\t\tUsername to connect as\n";
            print "--password=\tPassword to access database.\n";
            exit;
        case "host":
            $host = $v;
            break;
        case "password":
            $password = $v;
            break;
        case "user":
            $user = $v;
            break;
    }
}

printMSG("Attempting to connect to database.\n");
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
    printMSG("Using beta server.\n");
    # API must match server version.
    $api = 20130709;
}
else
{
    $baseurl = "https://data2.schedulesdirect.org";
    printMSG("Using production server.\n");
    $api = 20130512;
}

$stmt = $dbh->prepare("SELECT sourceid,name,userid,lineupid,password FROM videosource");
$stmt->execute();
$result = $stmt->fetchAll(PDO::FETCH_ASSOC);

foreach ($result[0] as $k => $v)
{
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

$globalStartTime = time();
$globalStartDate = new DateTime();

printMSG("Retrieving list of channels to download.\n");
$stmt = $dbh->prepare("SELECT DISTINCT(xmltvid) FROM channel WHERE visible=TRUE");
$stmt->execute();
$stationIDs = $stmt->fetchAll(PDO::FETCH_COLUMN);

printMSG("Logging into Schedules Direct.\n");
$randHash = getRandhash($username, $password);

if ($randHash != "ERROR")
{
    printStatus(getStatus());
    getSchedules($stationIDs, $debug);
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

function getSchedules(array $stationIDs, $debug)
{
    global $dbh;
    global $api;
    global $randHash;

    $dbProgramCache = array();
    $schedTempDir = tempdir();
    $downloadedStationIDs = array();

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

    /*
     * First we're going to load all the programIDs and md5's from the schedule files we just downloaded into
     * an array called programCache
     */

    if ($res["response"] == "OK")
    {
        $fileName = $res["filename"];
        $url = $res["URL"];
        file_put_contents("$schedTempDir/$fileName", file_get_contents($url));

        $zipArchive = new ZipArchive();
        $result = $zipArchive->open("$schedTempDir/$fileName");
        if ($result === TRUE)
        {
            $zipArchive->extractTo("$schedTempDir");
            $zipArchive->close();

            foreach (glob("$schedTempDir/sched_*.json.txt") as $f)
            {
                // print "***DEBUG: Reading schedule $f\n";
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

    $stmt = $dbh->prepare("SELECT programID,md5 FROM SDprogramCache");
    $stmt->execute();
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    foreach ($result as $v)
    {
        $dbProgramCache[$v["md5"]] = $v["programID"];
    }

    $toRetrieve = array();
    $toRetrieve = array_diff_key($serverScheduleMD5, $dbProgramCache);

    /*
     * Now we've got an array of programIDs that we need to download in $toRetrieve,
     * either because we didn't have them, or they have different md5's.
     */

    printMSG("Need to download " . count($toRetrieve) . " new or updated programs.\n");

    if (count($toRetrieve) > 10000)
    {
        printMSG("Requesting more than 10000 programs. Please be patient.\n");
    }

    if (count($toRetrieve))
    {
        printMSG("Requesting new and updated programs.\n");
        $res = array();
        $res["action"] = "get";
        $res["object"] = "programs";
        $res["randhash"] = $randHash;
        $res["api"] = $api;
        $res["request"] = $toRetrieve;

        $response = sendRequest(json_encode($res));

        $res = array();
        $res = json_decode($response, TRUE);

        if ($res["response"] == "OK")
        {
            printMSG("Starting program cache insert.\n");
            $tempDir = tempdir();

            $fileName = $res["filename"];
            $url = $res["URL"];
            file_put_contents("$tempDir/$fileName", file_get_contents($url));

            $zipArchive = new ZipArchive();
            $result = $zipArchive->open("$tempDir/$fileName");
            if ($result === TRUE)
            {
                $zipArchive->extractTo("$tempDir");
                $zipArchive->close();
            }
            else
            {
                printMSG("FATAL: Could not open .zip file while extracting programIDs.\n");
                exit;
            }

            $counter = 0;
            printMSG("Performing inserts.\n");

            $insertJSON = $dbh->prepare("INSERT INTO SDprogramCache(programID,md5,json)
            VALUES (:programID,:md5,:json)
            ON DUPLICATE KEY UPDATE md5=:md5, json=:json");
            $total = count($toRetrieve);

            $insertPerson = $dbh->prepare("INSERT INTO peopleSD(personID,name) VALUES(:personID, :name)
        ON DUPLICATE KEY UPDATE name=:name");

            $insertCredit = $dbh->prepare("INSERT INTO creditsSD(personID,programID,role)
    VALUES(:personID,:pid,:role)");

            $insertProgramGenres = $dbh->prepare("INSERT INTO programgenresSD(programID,relevance,genre)
    VALUES(:pid,:relevance,:genre) ON DUPLICATE KEY UPDATE genre=:genre");

            $peopleCache = array();
            $getPeople = $dbh->prepare("SELECT name,personID FROM peopleSD");
            $getPeople->execute();


            while ($row = $getPeople->fetch())
            {
                $peopleCache[$row[0]] = $row[1];
            }

            foreach ($toRetrieve as $md5 => $pid)
            {
                $counter++;
                if ($counter % 1000)
                {
                    printMSG("$counter / $total             \r");
                }

                $fileJSON = file_get_contents("$tempDir/$pid.json.txt");

                if ($fileJSON === FALSE)
                {
                    printMSG("*** ERROR: Could not open file $tempDir/$pid.json.txt\n");
                    continue;
                }

                $insertJSON->execute(array("programID" => $pid, "md5" => $md5,
                                           "json"      => $fileJSON));

                $jsonProgram = json_decode($fileJSON, TRUE);

                if (json_last_error())
                {
                    printMSG("*** ERROR: JSON decode error $tempDir/$pid.json.txt\n");
                    printMSG("$fileJSON\n");
                    continue;
                }

                if (isset($jsonProgram["castAndCrew"]))
                {
                    foreach ($jsonProgram["castAndCrew"] as $credit)
                    {
                        list ($role, $name) = explode(":", $credit);
                        $role = strtolower($role);

                        if (isset($peopleCache[$name]))
                        {
                            $personID = $peopleCache[$name];
                        }
                        else
                        {
                            $personID = mt_rand(1000, 10000000);
                            $peopleCache[$name] = $personID;
                        }

                        $insertPerson->execute(array("personID" => $personID, "name" => $name));

                        $insertCredit->execute(array("personID" => $personID, "pid" => $pid,
                                                     "role"     => $role));
                    }
                }

                if (isset($jsonProgram["genres"]))
                {
                    foreach ($jsonProgram["genres"] as $relevance => $genre)
                    {
                        $insertProgramGenres->execute(array("pid"       => $pid,
                                                            "relevance" => $relevance, "genre" => $genre));
                    }
                }

                if ($debug == FALSE)
                {
                    unlink("$tempDir/$pid.json.txt");
                }
            }

            if ($debug == FALSE)
            {
                unlink("$tempDir/serverID.txt");
                rmdir("$tempDir");
            }
        }

        printMSG("Completed local database program updates.\n");
    }

    printMSG("Inserting schedules.\n");

    $counter = 0;
    $total = count($downloadedStationIDs);

    printMSG("Starting insert loop. $total station schedules to insert.\n");

    $stmt = $dbh->exec("DROP TABLE IF EXISTS s_scheduleSD");
    $stmt = $dbh->exec("CREATE TABLE s_scheduleSD LIKE scheduleSD");

    $insertSchedule = $dbh->prepare("INSERT INTO s_scheduleSD(stationID,programID,md5,air_datetime,duration,
    previouslyshown,closecaptioned,partnumber,parttotal,first,last,dvs,new,educational,hdtv,3d,letterbox,stereo,
    dolby,dubbed,dubLanguage,subtitled,subtitleLanguage,sap,sapLanguage,programLanguage,tvRating,dialogRating,languageRating,
    sexualContentRating,violenceRating,fvRating) 
    
    VALUES(:stationID,:programID,:md5,:air_datetime,:duration,
    :previouslyshown,:closecaptioned,:partnumber,:parttotal,:first,:last,:dvs,:new,:educational,:hdtv,:3d,
    :letterbox,:stereo,:dolby,:dubbed,:dubLanguage,:subtitled,:subtitleLanguage,:sap,:sapLanguage,:programLanguage,
    :tvRating,:dialogRating,:languageRating,:sexualContentRating,:violenceRating,:fvRating)");

    foreach ($downloadedStationIDs as $stationID)
    {
        $a = json_decode(file_get_contents("$schedTempDir/sched_$stationID.json.txt"), TRUE);

        // printMSG("Reading $stationID\n");

        $counter++;
        if ($counter % 100 == 0)
        {
            printMSG("Inserted $counter of $total stationIDs.                                         \r");
        }

        $dbh->beginTransaction();

        foreach ($a["programs"] as $v)
        {
            $programID = $v["programID"];
            $md5 = $v["md5"];
            $air_datetime = $v["airDateTime"];
            $duration = $v["duration"];

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
                $dolby = $v["dolby"];
            }
            else
            {
                $dolby = NULL;
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
                printMSG("*** Warning: $programID has dub but no dubbed language set.\n");
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
                printMSG("*** Warning: $programID has subtitle but no subtitled language set.\n");
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
                printMSG("*** Warning: $programID has SAP but no SAP language set.\n");
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

            $insertSchedule->execute(array(
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
                "dolby"               => $dolby,
                "dubbed"              => $dubbed,
                "dubLanguage"         => $dubbedLanguage,
                "subtitled"           => $isSubtitled,
                "subtitleLanguage"    => $subtitledLanguage,
                "sap"                 => $sap,
                "sapLanguage"         => $sapLanguage,
                "programLanguage"     => $programLanguage,
                "tvRating"            => $rating,
                "dialogRating"        => $dialogRating,
                "languageRating"      => $languageRating,
                "sexualContentRating" => $sexRating,
                "violenceRating"      => $violenceRating,
                "fvRating"            => $fvRating));
        }

        $dbh->commit();
    }

    printMSG("Done inserting schedules.\n");
    $stmt = $dbh->exec("DROP TABLE scheduleSD");
    $stmt = $dbh->exec("RENAME TABLE s_scheduleSD TO scheduleSD");
}

function getStatus()
{
    global $api;
    global $randHash;

    $res = array();
    $res["action"] = "get";
    $res["object"] = "status";
    $res["randhash"] = $randHash;
    $res["api"] = $api;

    return sendRequest(json_encode($res));
}

function printStatus($json)
{
    global $dbh;

    printMSG("Status messages from Schedules Direct:\n");

    $res = array();
    $res = json_decode($json, true);

    $am = array();
    $he = array();

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
                foreach ($v as $hv)
                {
                    $he[$hv["ID"]] = $hv["modified"];
                }
                break;
            case "code":
                if ($v == 401)
                {
                    /*
                     * Error notification - we're going to have to abort because the server didn't like what we sent.
                     */
                    printMSG("Received error response from server!\n");
                    printMSG("ServerID: " . $res["serverID"] . "\n");
                    printMSG("Message: " . $res["message"] . "\n");
                    printMSG("\nFATAL ERROR. Terminating execution.\n");
                    exit;
                }
        }
    }

    printMSG("Server: " . $res["serverID"] . "\n");
    printMSG("Last data refresh: " . $res["lastDataUpdate"] . "\n");
    printMSG("Account expires: $expires\n");
    printMSG("Max number of headends for your account: $maxHeadends\n");
    printMSG("Next suggested connect time: $nextConnectTime\n");

    if (count($he))
    {
        $stmt = $dbh->prepare("SELECT modified FROM SDlineupCache WHERE headend=:he");
        printMSG("The following headends are in your account:\n");

        $retrieveLineups = array();
        foreach ($he as $id => $modified)
        {
            printMSG("ID: $id\t\t");
            if (strlen($id) < 4)
            {
                // We want the tabs to align.
                printMSG("\t");
            }
            printMSG("SD Modified: $modified\n");
            $stmt->execute(array("he" => $id));
            $result = $stmt->fetchAll(PDO::FETCH_COLUMN);

            if ((count($result) == 0) OR ($result[0] < $modified))
            {
                $retrieveLineups[] = $id;
            }
        }

        if (count($retrieveLineups))
        {
            processLineups($retrieveLineups);
        }
    }
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
        printMSG("tempdir is $tempfile\n");

        return $tempfile;
    }
}

function printMSG($str)
{
    global $fh_log;
    global $quiet;

    $str = date("H:i:s") . ":$str";

    if (!$quiet)
    {
        print "$str";
    }

    $str = str_replace("\r", "\n", $str);
    fwrite($fh_log, $str);
}

?>