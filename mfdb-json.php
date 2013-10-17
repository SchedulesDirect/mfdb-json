#!/usr/bin/php

<?php
/*
 * This file is a grabber which downloads data from Schedules Direct's JSON service.
 */

$isBeta = FALSE;
$debug = TRUE;
$doSetup = FALSE;
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
            print "--help (this text)\n";
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
        case "setup":
            $doSetup = TRUE;
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

if ($doSetup)
{
    setup();
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

printMSG("Retrieving list of channels.\n");
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

    printMSG("\n\nDone inserting schedules.\n");
    $stmt = $dbh->exec("DROP TABLE scheduleSD");
    $stmt = $dbh->exec("RENAME TABLE s_scheduleSD TO scheduleSD");
}

function setup()
{
    global $dbh;
    $done = FALSE;

    while ($done == FALSE)
    {
        $stmt = $dbh->prepare("SELECT sourceid,name,userid,lineupid,password FROM videosource");
        $stmt->execute();
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (count($result))
        {
            printMSG("Existing sources:\n");
            foreach ($result as $v)
            {
                printMSG("sourceid: " . $v["sourceid"] . "\n");
                printMSG("name: " . $v["name"] . "\n");
                printMSG("userid: " . $v["userid"] . "\n");
                $username = $v["userid"];
                printMSG("lineupid: " . $v["lineupid"] . "\n");
                printMSG("password: " . $v["password"] . "\n\n");
                $password = $v["password"];
            }
        }
        else
        {
            $username = readline("Schedules Direct username:");
            $password = readline("Schedules Direct password:");
        }

        printMSG("Checking existing lineups at Schedules Direct.\n");
        $randHash = getRandhash($username, sha1($password));

        if ($randHash != "ERROR")
        {
            $res = array();
            $res = json_decode(getStatus(), true);
            $he = array();

            foreach ($res as $k => $v)
            {
                if ($k == "headend")
                {
                    foreach ($v as $hv)
                    {
                        $he[$hv["ID"]] = 1;
                        printMSG("Headend: " . $hv["ID"] . "\n");
                    }
                }
            }

            if (count($he))
            {
                printMSG("A to add a new sourceid in MythTV\n");
                printMSG("L to Link an existing sourceid to an existing headend at SD\n");
                printMSG("Q to Quit\n");
                $response = strtoupper(readline(">"));

                switch ($response)
                {
                    case "A":
                        printMSG("Adding new sourceid\n\n");
                        $newName = readline("Source name:>");
                        $stmt = $dbh->prepare("INSERT INTO videosource(name,userid,password)
                        VALUES(:name,:userid,:password)");
                        $stmt->execute(array("name"     => $newName, "userid" => $username,
                                             "password" => $password));
                        break;
                    case "L":
                        printMSG("Linking Schedules Direct headend to sourceid\n\n");
                        $sid = readline("Source id:>");
                        $he = readline("Headend:>");
                        $stmt = $dbh->prepare("UPDATE videosource SET lineupid=:he WHERE sourceid=:sid");
                        $stmt->execute(array("he" => $he, "sid" => $sid));
                        /*
                         * Download the lineups
                         */
                        /*
                         * Create the channel table.
                         */
                        break;
                    case "Q":
                    default:
                        $done = TRUE;
                        break;
                }
            }
            else
            {
                /*
                 * User has no headends defined in their SD account.
                 */
                addHeadendsToSchedulesDirect($randHash);
            }
        }

    }
}

function addHeadendsToSchedulesDirect()
{
    global $randHash;
    global $api;

    printMSG("\n\nNo headends are configured in your Schedules Direct account.\n");
    printMSG("Enter your 5-digit zip code for U.S.\n");
    printMSG("Enter your 6-character postal code for Canada.\n");
    printMSG("Two-character ISO3166 code for international.\n");

    $response = readline(">");

    $res = array();
    $res["action"] = "get";
    $res["object"] = "headends";
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = "PC:$response";

    $res = json_decode(sendRequest(json_encode($res)), true);

    foreach ($res["data"] as $v)
    {
        printMSG("headend: " . $v["headend"] . "\nname: " . $v["name"] . "(" . $v["location"] . ")\n\n");
    }

    $he = readline("Headend to add>");
    if ($he == "")
    {
        return;
    }

    $res = array();
    $res["action"] = "add";
    $res["object"] = "headends";
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = $he;

    $res = json_decode(sendRequest(json_encode($res)), true);

    if ($res["response"] == "OK")
    {
        printMSG("Successfully added headend.\n");
    }
    else
    {
        printMSG("\n\n-----\nERROR:Received error response from server:\n");
        printMSG($res["message"] . "\n\n-----\n");
        printMSG("Press ENTER to continue.\n");
        $a = fgets(STDIN);
    }
}

function getLineup(array $he)
{
    global $randHash;
    global $api;

    printMSG("Retrieving lineup from Schedules Direct.\n");

    $res = array();
    $res["action"] = "get";
    $res["object"] = "lineups";
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = $he;

    return sendRequest(json_encode($res), true);
}

function commitToDb(array $stack, $base, $chunk, $useTransaction, $verbose)
{
    global $dbh;

    /*
     * If the "chunk" is too big, then things get slow, and you run into other issues, like max size of the packet
     * that mysql will swallow. Better safe than sorry, and once things are running there aren't massive numbers of
     * added program IDs.
     */

    $numRows = count($stack);

    if ($numRows == 0)
    {
        return;
    }

    $str = "";
    $counter = 0;
    $loop = 0;
    $numLoops = intval($numRows / $chunk);

    if ($useTransaction)
    {
        $dbh->beginTransaction();
    }

    foreach ($stack as $value)
    {
        $counter++;
        $str .= $value;

        if ($counter % $chunk == 0)
        {
            $loop++;
            $str = rtrim($str, ","); // Get rid of the last comma.
            printMSG("Loop: $loop of $numLoops\r");

            try
            {
                $count = $dbh->exec("$base$str");
            } catch (Exception $e)
            {
                printMSG("Exception: " . $e->getMessage());
            }

            if ($count === FALSE)
            {
                print_r($dbh->errorInfo(), TRUE);
                printMSG("line:\n\n$base$str\n");
                exit;
            }
            $str = "";
            if ($useTransaction)
            {
                $dbh->commit();
                $dbh->beginTransaction();
            }
        }
    }

    printMSG("\n");

    // Remainder
    $str = rtrim($str, ","); // Get rid of the last comma.

    $count = $dbh->exec("$base$str");
    if ($count === FALSE)
    {
        print_r($dbh->errorInfo(), true);
    }

    if ($verbose)
    {
        printMSG("Done inserting.\n");
    }
    if ($useTransaction)
    {
        $dbh->commit();
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

function processLineups(array $retrieveLineups)
{
    global $dbh;

    /*
     * If we're here, that means that either the lineup has been updated, or it didn't exist at all.
     * The overall group of lineups in a headend have a modified date based on the "max" of the modified dates
     * of the lineups in the headend. But we may not be using that particular lineup, so dig deeper...
     */

    printMSG("Checking for updated lineups from Schedules Direct.\n");

    $res = array();
    $res = json_decode(getLineup($retrieveLineups), true);

    if ($res["response"] != "OK")
    {
        printMSG("\n\n-----\nERROR: Bad response from Schedules Direct.\n");
        printMSG($res["message"] . "\n\n-----\n");
        exit;
    }

    $tempDir = tempdir();
    $fileName = "$tempDir/lineups.json.zip";
    file_put_contents($fileName, file_get_contents($res["URL"]));

    $zipArchive = new ZipArchive();
    $result = $zipArchive->open("$fileName");
    if ($result === TRUE)
    {
        $zipArchive->extractTo("$tempDir");
        $zipArchive->close();
    }
    else
    {
        printMSG("FATAL: Could not open lineups zip file.\n");
        printMSG("tempdir is $tempDir\n");
        exit;
    }

    /*
     * First, store a copy of the data that we just downloaded into the cache for later.
     */
    $stmt = $dbh->prepare("REPLACE INTO SDlineupCache(headend,json) VALUES(:he,:json)");
    foreach (glob("$tempDir/*.json.txt") as $f)
    {
        $json = file_get_contents($f);
        $a = json_decode($json, true);
        $he = $a["headend"];
        $stmt->execute(array("he" => $he, "json" => $json));
    }

    /*
     * Get list of lineups that the user has and only worry about those.
     */
    $stmt = $dbh->prepare("SELECT sourceid,lineupid,modified FROM videosource");
    $stmt->execute();
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
    $lineup = array();

    foreach ($result as $v)
    {
        $device = "";
        $lineupid = $v["lineupid"];
        $modified = $v["modified"];

        if (strpos($lineupid, ":"))
        {
            list($headend, $device) = explode(":", $lineupid);
            if ($headend == "PC")
            {
                $headend = $lineupid;
                $device = "Antenna";
            }
        }
        else
        {
            $headend = $lineupid;
            $device = "Analog";
        }
        $lineup[$v["sourceid"]] = array("headend" => $headend, "device" => $device, "modified" => $modified);
        // printMSG("headend:$headend device:$device modified:$modified\n");
    }

    /*
     * Now we have to determine if the lineup that the user is actually using has been updated.
     */

    $stmt = $dbh->prepare("SELECT json FROM SDlineupCache WHERE headend=:he");
    foreach ($lineup as $lineupid => $v)
    {
        $headend = $v["headend"];
        $device = $v["device"];
        $modified = $v["modified"];
        $stmt->execute(array("he" => $headend));
        $json = json_decode($stmt->fetchAll(PDO::FETCH_COLUMN)[0], true);

        foreach ($json["metadata"] as $v1)
        {
            if ($v1["device"] == $device)
            {
                $jsonModified = $v1["modified"];
                $transport = $v1["transport"];
                // Eventually we won't print once coding is done.
                printMSG("$headend:$device local modified date:" . $lineup[$lineupid]["modified"] . "\n");
                printMSG("server modified date:$jsonModified\n");
                if ($jsonModified != $lineup[$lineupid]["modified"])
                {
                    printMSG("Use new lineup?\n");
                    $updateDB = strtoupper(readline(">"));
                    if ($updateDB == "Y")
                    {
                        updateChannelTable($lineupid, $headend, $device, $transport, $json);
                        $stmt = $dbh->prepare("UPDATE videosource SET modified=:modified WHERE sourceid=:sourceid");
                        $stmt->execute(array("modified" => $jsonModified, "sourceid" => $lineupid));
                    }
                }
            }
        }


    }
}

function updateChannelTable($sourceid, $he, $dev, $transport, array $json)
{
    global $dbh;

    printMSG("Updating channel table for sourceid:$sourceid\n");
    $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sourceid");
    $stmt->execute(array("sourceid" => $sourceid));

    foreach ($json[$dev]["map"] as $mapArray)
    {
        $stationID = $mapArray["stationID"];

        if ($transport == "Antenna")
        {
            $freqid = $mapArray["uhfVhf"];
            if (isset($mapArray["atscMajor"]))
            {
                $atscMajor = $mapArray["atscMajor"];
                $atscMinor = $mapArray["atscMinor"];
            }
            else
            {
                $atscMajor = 0;
                $atscMinor = 0;
            }
        }
        else
        {
            $channum = $mapArray["channel"];
        }
        /*
         * If we start to do things like "IP" then we'll be inserting URLs, but this is fine for now.
         */

        if ($transport == "Cable")
        {
            $stmt = $dbh->prepare(
                "INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid)
                 VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid)");

            $stmt->execute(array("chanid" => (int)($sourceid * 1000) + (int)$channum, "channum" => $channum,
                                 "freqid" => $channum, "sourceid" => $sourceid, "xmltvid" => $stationID));
        }
        else
        {
            $stmt = $dbh->prepare(
                "INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid,atsc_major_chan,atsc_minor_chan)
                VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid,:atsc_major_chan,:atsc_minor_chan)");
            $stmt->execute(array("chanid"          => (int)($sourceid * 1000) + (int)$freqid, "channum" => $freqid,
                                 "freqid"          => $freqid, "sourceid" => $sourceid, "xmltvid" => $stationID,
                                 "atsc_major_chan" => $atscMajor, "atsc_minor_chan" => $atscMinor));
        }
    }
    /*
     * Now that we have basic information in the database, we can start filling in other things, like callsigns, etc.
     */

    $stmt = $dbh->prepare("UPDATE channel SET name=:name, callsign=:callsign WHERE xmltvid=:stationID");
    foreach ($json["stations"] as $stationArray)
    {
        $stationID = $stationArray["stationID"];
        $name = $stationArray["name"];
        $callsign = $stationArray["callsign"];
        $stmt->execute(array("name" => $name, "callsign" => $callsign, "stationID" => $stationID));
    }

    if (isset($json["QAM"]))
    {
        printMSG("Adding QAM data.\n");
        $dtvMultiplex = array();

        $channelInsert =
            $dbh->prepare("UPDATE channel SET tvformat='ATSC',visible='1',mplexid=:mplexid,serviceid=:qamprogram
        WHERE xmltvid=:stationID");

        $qamModified = $json["QAM"]["metadata"]["modified"];
        printMSG("qam modified:$qamModified\n");

        foreach ($json["QAM"]["map"] as $v)
        {
            $stationID = $v["stationID"];
            $qamType = $v["qamType"];
            $qamProgram = $v["qamProgram"];
            $qamFreq = $v["qamFreq"];
            $channel = $v["channel"];
            if (isset($v["virtualChannel"]))
            {
                $virtualChannel = $v["virtualChannel"];
            }
            else
            {
                $virtualChannel = "";
            }

            // printMSG("$stationID $qamType $qamFreq $qamProgram $channel\n");

            /*
             * Because multiple programs  may end up on a single frequency, we only want to insert once, but we want
             * to track the mplexid assigned when we do the insert, because that might be used more than once.
             */

            if (!isset($dtvMultiplex[$qamFreq]))
            {
                $insertDTVMultiplex = $dbh->prepare
                    ("INSERT INTO dtv_multiplex
                (sourceid,frequency,symbolrate,polarity,modulation,visible,constellation,hierarchy,mod_sys,rolloff,sistandard)
                VALUES
                (:sourceid,:freq,0,'v','qam_256',1,'qam_256','a','UNDEFINED','0.35','atsc')");
                $insertDTVMultiplex->execute(array("sourceid" => $sourceid, "freq" => $qamFreq));
                $dtvMultiplex[$qamFreq] = $dbh->lastInsertId();
            }

            $channelInsert->execute(array("mplexid"   => $dtvMultiplex[$qamFreq], "qamprogram" => $qamProgram,
                                          "stationID" => $stationID));
        }
    }

    printMSG("***DEBUG: Exiting updateChannelTable.\n");
    /*
     * Set the startchan to a non-bogus value.
     */

    $stmt = $dbh->prepare("SELECT channum FROM channel WHERE sourceid=:sourceid ORDER BY CAST(channum AS SIGNED) LIMIT 1");
    $stmt->execute(array("sourceid" => $sourceid));
    $result = $stmt->fetchAll(PDO::FETCH_COLUMN);
    $startChan = $result[0];
    $setStartChannel = $dbh->prepare("UPDATE cardinput SET startchan=:startChan WHERE sourceid=:sourceid");
    $setStartChannel->execute(array("sourceid" => $sourceid, "startChan" => $startChan));
    print "***DEBUG: Exiting updateChannelTable.\n";
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

function holder()
{
    $getProgramDetails->execute(array("pid" => $programID));
    $tempJsonProgram = $getProgramDetails->fetchAll(PDO::FETCH_COLUMN);

    $jsonProgram = json_decode($tempJsonProgram[0], TRUE);

    if (isset($jsonProgram["titles"]["title120"]))
    {
        $title = $jsonProgram["titles"]["title120"];
    }
    else
    {
        print "MAJOR ERROR: no title. station:$stationID program $programID";
        var_dump($tempJsonProgram);
        print "\n\n\n\n\n\n";
        var_dump($jsonProgram);
        exit;
    }

    if (isset($jsonProgram["episodeTitle150"]))
    {
        $subtitle = $jsonProgram["episodeTitle150"];
    }
    else
    {
        $subtitle = "";
    }

    if (isset($jsonProgram["descriptions"]["description255"]))
    {
        /*
         * Schedules Direct can send descriptions of various lengths, but "255" will always exist if there
         * are any descriptions at all.
         */
        $description = $jsonProgram["descriptions"]["description255"];
    }
    else
    {
        $description = "";
    }

    if (isset($jsonProgram["genres"][0]))
    {
        /*
         * Schedules Direct can send multiple genres, but MythTV only uses one in the program. The rest are
         * in the programgenres table.
         */
        $category = $jsonProgram["genres"][0];
    }
    else
    {
        $category = "";
    }

    if (isset($jsonProgram["showType"]))
    {
        /*
         * Not sure why MythTV has this twice. In the program table, category_type is a lowercase version
         * of showtype?
         */
        $category_type = $jsonProgram["showType"];
        $showtype = $jsonProgram["showType"];
    }
    else
    {
        $category_type = "";
        $showtype = "";
        /*
         * May get reset later based on first two characters of programID.
         */
    }

    /*
     * MythTV sets the airdate to "0000" for EP types, but sets it to the movie release date if it's a movie.
     */
    $airdate = "0000";

    if (substr($programID, 0, 2) == "MV")
    {
        if (isset($jsonProgram["movie"]["year"]))
        {
            $airdate = $jsonProgram["movie"]["year"];
            $category_type = "movie";
        }
        if (isset($jsonProgram["movie"]["starRating"]))
        {
            $numStars = substr_count($jsonProgram["movie"]["starRating"], "*");
            $numPlus = substr_count($jsonProgram["movie"]["starRating"], "+");
            $stars = ($numStars * .25) + ($numPlus * .125);
        }
        if (isset($jsonProgram["movie"]["mpaaRating"]))
        {
            $rating = $jsonProgram["movie"]["mpaaRating"];
            $ratingSystem = "MPAA";
        }
    }
    else
    {
        $stars = 0;
    }

}

?>