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
    printMSG("Exception with PDO: " . $e->getMessage() . "\n");
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

printMSG("\n\nGlobal. Start Time:" . date("Y-m-d H:i:s", $globalStartTime) . "\n");
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

    $programCache = array();
    $dbProgramCache = array();
    $schedTempDir = tempdir();
    $chanData = array();

    printMSG("Sending schedule request.\n");
    $res = array();
    $res["action"] = "get";
    $res["object"] = "schedules";
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = $stationIDs;

    $response = sendRequest(json_encode($res));

    $res = array();
    $res = json_decode($response, true);

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
            $stmt = $dbh->prepare("SELECT chanid,channum,sourceid FROM channel WHERE visible=1 AND
                xmltvid=:stationid");

            foreach (glob("$schedTempDir/sched_*.json.txt") as $f)
            {
                // printMSG("***DEBUG: Reading schedule $f\n");
                $a = json_decode(file_get_contents($f), true);
                $stationID = $a["stationID"];
                $stmt->execute(array("stationid" => $stationID));
                $r = $stmt->fetchAll(PDO::FETCH_ASSOC);
                $chanData[$stationID] = $r;

                foreach ($a["programs"] as $v)
                {
                    $programCache[$v["programID"]] = array("md5" => $v["md5"], "json" => $v);
                }
            }
        }
        else
        {
            printMSG("FATAL: Could not open zip file.\n");
            exit;
        }
    }

    printMSG("There are " . count($programCache) . " programIDs in the upcoming schedule.\n");
    printMSG("Retrieving existing MD5 values.\n");

    $stmt = $dbh->prepare("SELECT programID,md5 FROM SDprogramCache");
    $stmt->execute();
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    foreach ($result as $v)
    {
        $dbProgramCache[$v["programID"]] = $v["md5"];
    }

    $insertStack = array();
    $replaceStack = array();

    /*
     * An array to hold the programIDs that we need to request from the server.
     */
    $retrieveStack = array();

    foreach ($programCache as $progID => $dataArray)
    {
        if (array_key_exists($progID, $dbProgramCache))
        {
            /*
             * First we'll check if the key (the programID) exists in the database already, and if yes, does it have
             * the same md5 value as the one that we downloaded?
             */
            if ($dbProgramCache[$progID] != $dataArray["md5"])
            {
                $replaceStack[$progID] = $dataArray["md5"];
                $retrieveStack[] = $progID;
            }
        }
        else
        {
            /*
             * The programID wasn't in the database, so we'll need to get it.
             */
            $insertStack[$progID] = $dataArray["md5"];
            $retrieveStack[] = $progID;
        }
    }

    /*
     * Now we've got an array of programIDs that we need to download, either because we didn't have them,
     * or they have different md5's.
     */

    printMSG("Need to download " . count($insertStack) . " new programs.\n");
    printMSG("Need to download " . count($replaceStack) . " updated programs.\n");

    if (count($insertStack) + count($replaceStack) > 10000)
    {
        printMSG("Requesting more than 10000 programs. Please be patient.\n");
    }

    if (count($insertStack) + count($replaceStack) > 0)
    {
        printMSG("Requesting new and updated programs.\n");
        $res = array();
        $res["action"] = "get";
        $res["object"] = "programs";
        $res["randhash"] = $randHash;
        $res["api"] = $api;
        $res["request"] = $retrieveStack;

        $response = sendRequest(json_encode($res));

        $res = array();
        $res = json_decode($response, true);

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

            $stmt = $dbh->prepare("INSERT INTO SDprogramCache(programID,md5,json) VALUES (:programID,:md5,:json)");
            foreach ($insertStack as $progID => $v)
            {
                $counter++;
                if ($counter % 1000)
                {
                    printMSG("$counter / " . count($insertStack) . "             \r");
                }
                $stmt->execute(array("programID" => $progID, "md5" => $v,
                                     "json"      => file_get_contents("$tempDir/$progID.json.txt")));
                if ($debug == FALSE)
                {
                    unlink("$tempDir/$progID.json.txt");
                }
            }

            $counter = 0;
            printMSG("\nPerforming updates.\n");

            // $stmt = $dbh->prepare("REPLACE INTO SDprogramCache(programID,md5,json) VALUES (:programID,:md5,:json)");
            $stmt = $dbh->prepare("UPDATE SDprogramCache SET md5=:md5,json=:json WHERE programID=:programID");
            foreach ($replaceStack as $progID => $v)
            {
                $counter++;
                if ($counter % 10)
                {
                    printMSG("$counter / " . count($replaceStack) . "             \r");
                }
                $stmt->execute(array("programID" => $progID, "md5" => $v,
                                     "json"      => file_get_contents("$tempDir/$progID.json.txt")));

                if ($debug == FALSE)
                {
                    unlink("$tempDir/$progID.json.txt");
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

    $s1 = $dbh->exec("DROP TABLE IF EXISTS p_rogram, p_rogramgenres, p_rogramrating, c_redits");

    $s1 = $dbh->exec("CREATE TABLE p_rogram LIKE program");
    $s1 = $dbh->exec("CREATE TABLE p_rogramgenres LIKE programgenres");
    $s1 = $dbh->exec("CREATE TABLE c_redits LIKE credits");
    $s1 = $dbh->exec("CREATE TABLE p_rogramrating LIKE programrating");

    $programInsert = $dbh->prepare
        ("INSERT INTO p_rogram(chanid,starttime,endtime,title,subtitle,description,category,category_type,airdate,stars,
    previouslyshown,stereo,subtitled,hdtv,closecaptioned,partnumber,parttotal,seriesid,originalairdate,showtype,
    colorcode,syndicatedepisodenumber,programid,generic,listingsource,first,last,audioprop,subtitletypes,videoprop)
    VALUES(:chanid,:starttime,:endtime,:title,:subtitle,:description,:category,:category_type,:airdate,
    :stars,:previouslyshown,:stereo,:subtitled,:hdtv,:closecaptioned,:partnumber,:parttotal,:seriesid,
    :originalairdate,:showtype,:colorcode,:syndicatedepisodenumber,:programid,:generic,:listingsource,
    :first,:last,:audioprop,:subtitletypes,:videoprop)");

    $insertPerson = $dbh->prepare("INSERT INTO people(name) VALUES(:name)");
    $insertCredit = $dbh->prepare("INSERT IGNORE INTO c_redits(person,chanid,starttime,role)
    VALUES(:person,:chanid,:starttime,:role)");

    $getProgramDetails = $dbh->prepare("SELECT json FROM SDprogramCache WHERE programID=:pid");

    $insertProgramGenres = $dbh->prepare("INSERT INTO p_rogramgenres(chanid,starttime,relevance,genre)
    VALUES(:chanid,:starttime,:relevance,:genre)");

    $insertProgramRating = $dbh->prepare("INSERT INTO p_rogramrating(chanid,starttime,system,rating)
    VALUES(:chanid,:starttime,:system,:rating)");

    $counter = 0;
    $total = count($chanData);

    $peopleArray = array();
    /*
     * We're going to read in the people that are already in the database into an associative array with the name as
     * the key, and the person number as the value.
     */

    printMSG("Reading people table into memory.\n");

    $s1 = $dbh->prepare("SELECT name, person FROM people");
    $s1->execute();

    while ($row = $s1->fetch(PDO::FETCH_NUM))
    {
        $peopleArray[$row[0]] = $row[1];
    }

    //printMSG("size of array is \n");
    //printMSG(strlen(serialize($peopleArray)) . " bytes.\n");
    /*
     * 53579 people is 1.7megs in memory.
     */

    printMSG("Starting insert loop. $total station schedules to insert.\n");
    foreach ($chanData as $stationID => $row)
    {

        /*
         * Row is an array containing: chanid,channum,sourceid
         */

        $a = json_decode(file_get_contents("$schedTempDir/sched_$stationID.json.txt"), true);
        /*
         * These are used to set MPAA or V-CHIP schemes.
         */
        $ratingSystem = "";
        $rating = "";

        // printMSG("Reading $stationID\n");

        $counter++;
        if ($counter % 100 == 0)
        {
            printMSG("Inserted $counter of $total stationIDs.                                         \r");
        }

        foreach ($a["programs"] as $v)
        {
            $programID = $v["programID"];

            $getProgramDetails->execute(array("pid" => $programID));
            $tempJsonProgram = $getProgramDetails->fetchAll(PDO::FETCH_COLUMN);
            $jsonProgram = json_decode($tempJsonProgram[0], true);

            $startDate = DateTime::createFromFormat("Y-m-d\TH:i:s\Z", $v["airDateTime"]);
            $endDate = DateTime::createFromFormat("Y-m-d\TH:i:s\Z", $v["airDateTime"]);
            $endDate->add(new DateInterval("PT" . $v["duration"] . "S"));

            $starttime = $startDate->format("Y-m-d H:i:s");
            $endtime = $endDate->format("Y-m-d H:i:s");
            $title = $jsonProgram["titles"]["title120"];

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

            $isStereo = $v["stereo"];

            if (isset($v["hasSubTitles"]))
            {
                $isSubtitled = true;
            }
            else
            {
                $isSubtitled = false;
            }

            if (isset($v["cc"]))
            {
                $isClosedCaption = true;
            }
            else
            {
                $isClosedCaption = false;
            }

            if (isset($v["hdtv"]))
            {
                $isHDTV = true;
                $videoprop = "HDTV";
            }
            else
            {
                $isHDTV = false;
                $videoprop = "";
            }

            if (isset($jsonProgram["colorCode"]))
            {
                $colorcode = $jsonProgram["colorCode"];
            }
            else
            {
                $colorcode = "";
            }

            if (isset($jsonProgram["partNumber"]))
            {
                $partnumber = $jsonProgram["partNumber"];
            }
            else
            {
                $partnumber = 0;
            }

            if (isset($jsonProgram["numberOfParts"]))
            {
                $parttotal = $jsonProgram["numberOfParts"];
            }
            else
            {
                $parttotal = 0;
            }

            $seriesid = substr($programID, 0, 10);

            if (isset($jsonProgram["originalAirDate"]))
            {
                $originalairdate = $jsonProgram["originalAirDate"];
            }
            else
            {
                $originalairdate = "";
            }

            if (isset($jsonProgram["syndicatedEpisodeNumber"]))
            {
                $syndicatedepisodenumber = $jsonProgram["syndicatedEpisodeNumber"];
            }
            else
            {
                $syndicatedepisodenumber = "";
            }


            if ((substr($programID, -4) == "0000") AND (substr($programID, 0, 2) != "MV"))
            {
                $isGeneric = true;
            }
            else
            {
                $isGeneric = false;
            }

            if (isset($v["tvRating"]))
            {
                $ratingSystem = "V-CHIP";
                $rating = $v["tvRating"];
            }

            /*
             * Not sure why MythTV has multiple places for certain values.
             */

            /*
             * Figure out how to calculate this.
             */
            $previouslyshown = 0;
            $audioprop = "";
            $isFirst = false;
            $isLast = false;
            $subtitletypes = "";

            /*
             * This is where we'll actually perform the insert as many times as necessary based on how many copies
             * of this stationid there are across the multiple sources.
             */
            foreach ($row as $value)
            {
                /*
                 * There may be multiple videosources which have the same xmltvid,
                 * so we may be inserting the value multiple times.
                 *
                 * $value is an array with "chanid", "channum" and "sourceid"
                 */

                $programInsert->execute(array(
                    "chanid"                  => $value["chanid"], "starttime" => $starttime, "endtime" => $endtime,
                    "title"                   => $title,
                    "subtitle"                => $subtitle, "description" => $description, "category" => $category,
                    "category_type"           => $category_type, "airdate" => $airdate, "stars" => $stars,
                    "previouslyshown"         => $previouslyshown, "stereo" => $isStereo, "subtitled" => $isSubtitled,
                    "hdtv"                    => $isHDTV,
                    "closecaptioned"          => $isClosedCaption, "partnumber" => $partnumber,
                    "parttotal"               => $parttotal,
                    "seriesid"                => $seriesid,
                    "originalairdate"         => $originalairdate, "showtype" => $showtype, "colorcode" => $colorcode,
                    "syndicatedepisodenumber" => $syndicatedepisodenumber,
                    "programid"               => $programID,
                    "generic"                 => $isGeneric,
                    "listingsource"           => $value["sourceid"],
                    "first"                   => $isFirst, "last" => $isLast, "audioprop" => $audioprop,
                    "subtitletypes"           => $subtitletypes, "videoprop" => $videoprop
                ));

                if (isset($jsonProgram["castAndCrew"]))
                {
                    foreach ($jsonProgram["castAndCrew"] as $credit)
                    {
                        list ($role, $name) = explode(":", $credit);
                        $role = strtolower($role);

                        if ($role == "executive producer")
                        {
                            $role = "executive_producer";
                        }
                        if (array_key_exists($name, $peopleArray))
                        {
                            $personNumber = $peopleArray[$name];
                        }
                        else
                        {
                            $insertPerson->execute(array("name" => $name));
                            $personNumber = $dbh->lastInsertId();
                            $peopleArray[$name] = $personNumber;
                        }

                        $insertCredit->execute(array("person"    => $personNumber, "chanid" => $value["chanid"],
                                                     "starttime" => $starttime, "role" => $role));
                    }
                }

                if ($rating != "")
                {
                    $insertProgramRating->execute(array("chanid" => $value["chanid"], "starttime" => $starttime,
                                                        "system" => $ratingSystem, "rating" => $rating));
                }

                if (isset($jsonProgram["genres"]))
                {
                    foreach ($jsonProgram["genres"] as $relevance => $genre)
                    {
                        $insertProgramGenres->execute(array("chanid"    => $value["chanid"], "starttime" => $starttime,
                                                            "relevance" => $relevance, "genre" => $genre));
                    }
                }
            }
        }
    }

    $stmt = $dbh->exec("DROP TABLE IF EXISTS program_prev, programgenres_prev, programrating_prev, credits_prev");
    $stmt = $dbh->exec("RENAME TABLE program TO program_prev");
    $stmt = $dbh->exec("RENAME TABLE p_rogram TO program");
    $stmt = $dbh->exec("RENAME TABLE programgenres TO programgenres_prev");
    $stmt = $dbh->exec("RENAME TABLE p_rogramgenres TO programgenres");
    $stmt = $dbh->exec("RENAME TABLE programrating TO programrating_prev");
    $stmt = $dbh->exec("RENAME TABLE p_rogramrating TO programrating");
    $stmt = $dbh->exec("RENAME TABLE credits TO credits_prev");
    $stmt = $dbh->exec("RENAME TABLE c_redits TO credits");
    printMSG("\n\nDone inserting schedules.\n");

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
                print_r($dbh->errorInfo(), true);
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

function holder()
{
    foreach (glob("$tempDir/*.json.txt") as $f)
    {
        $a = json_decode(file_get_contents($f), true);
        $pid = $a["programID"];
        $md5 = $a["md5"];

        foreach ($a["program"] as $v)
        {
            $programCache[$v["programID"]] = $v["md5"];
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

    $str = date("H:i:s") . ":$str";

    if (!$quiet)
    {
        print "$str";
    }

    $str = str_replace("\r", "\n", $str);
    fwrite($fh_log, $str);
}


?>