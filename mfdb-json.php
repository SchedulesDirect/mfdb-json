#!/usr/bin/php

<?php
/*
 * This file is a grabber which downloads data from Schedules Direct's JSON service.
 */

$isBeta = TRUE;
$doInit = FALSE;
$debug = TRUE;
$doSetup = FALSE;

date_default_timezone_set("America/Chicago");
$date = new DateTime();
$todayDate = $date->format("Y-m-d");

$user = "mythtv";
$password = "mythtv";
$host = "localhost";
$db = "mythconverg";

$longoptions = array("beta::", "debug::", "help::", "host::", "init::", "password::", "setup::", "user::");

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
        case "init":
            $doInit = TRUE;
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

if ($doInit)
{
    dbInit($dbh);
}

if ($doSetup)
{
    setup($dbh);
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
    printStatus($dbh, $randHash, getStatus($randHash, $api));
    getSchedules($dbh, $randHash, $api, $stationIDs, $debug);
}

function getSchedules($dbh, $rh, $api, array $stationIDs, $debug)
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
            foreach (glob("$tempDir/sched_*.json.txt") as $f)
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

    $stmt = $dbh->prepare("SELECT programID,md5 FROM SDprogramCache");
    $stmt->execute();
    $dbProgramCache = $stmt->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_GROUP);

    $insertStack = array();
    $replaceStack = array();

    /*
     * An array to hold the programIDs that we need to request from the server.
     */
    $retrieveStack = array();

    foreach ($programCache as $progID => $md5)
    {
        if (array_key_exists($progID, $dbProgramCache))
        {
            /*
             * First we'll check if the key (the programID) exists in the database already, and if yes, does it have
             * the same md5 value as the one that we downloaded?
             */
            if ($dbProgramCache[$progID] != $md5)
            {
                $replaceStack[$progID] = $md5;
                $retrieveStack[] = $progID;
            }
        }
        else
        {
            /*
             * The programID wasn't in the database, so we'll need to get it.
             */
            $insertStack[$progID] = $md5;
            $retrieveStack[] = $progID;
        }
    }

    /*
     * Now we've got an array of programIDs that we need to download, either because we didn't have them,
     * or they have different md5's.
     */

    print "Need to download " . count($insertStack) . " new programs.\n";
    print "Need to download " . count($replaceStack) . " updated programs.\n";

    print "Sending program request.\n";
    $res = array();
    $res["action"] = "get";
    $res["object"] = "programs";
    $res["randhash"] = $rh;
    $res["api"] = $api;
    $res["request"] = $retrieveStack;

    $response = sendRequest(json_encode($res));

    $res = array();
    $res = json_decode($response, true);

    if ($res["response"] == "OK")
    {
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
            print "FATAL: Could not open .zip file while extracting programIDs.\n";
            exit;
        }

        $stmt = $dbh->prepare("INSERT INTO SDprogramCache(programID,md5,json) VALUES (:programID,:md5,:json)");
        foreach ($insertStack as $progID => $v)
        {
            $stmt->execute(array("programID" => $progID, "md5" => $v,
                                 "json"      => file_get_contents("$tempDir/$progID.json.txt")));
            if ($debug == FALSE)
            {
                unlink("$tempDir/$progID.json.txt");
            }
        }

        $stmt = $dbh->prepare("REPLACE INTO SDprogramCache(programID,md5,json) VALUES (:programID,:md5,:json)");
        foreach ($replaceStack as $progID => $v)
        {
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

    /*
     * Now that we've grabbed the program details for all the programs that we need to schedule, get to work.
     */
}

function setup($dbh)
{
    $done = FALSE;

    while ($done == FALSE)
    {
        $stmt = $dbh->prepare("SELECT sourceid,name,userid,lineupid,password FROM videosource");
        $stmt->execute();
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (count($result))
        {
            print "Existing sources:\n";
            foreach ($result as $v)
            {
                print "sourceid: " . $v["sourceid"] . "\n";
                print "name: " . $v["name"] . "\n";
                print "userid: " . $v["userid"] . "\n";
                $username = $v["userid"];
                print "lineupid: " . $v["lineupid"] . "\n";
                print "password: " . $v["password"] . "\n\n";
                $password = $v["password"];
            }
        }
        else
        {
            $username = readline("Schedules Direct username:");
            $password = readline("Schedules Direct password:");
        }

        print "Checking existing lineups at Schedules Direct.\n";
        $randHash = getRandhash($username, sha1($password), "http://23.21.174.111", "20130512");

        if ($randHash != "ERROR")
        {
            $res = array();
            $res = json_decode(getStatus($randHash, "20130512"), true);
            $he = array();

            foreach ($res as $k => $v)
            {
                if ($k == "headend")
                {
                    foreach ($v as $hv)
                    {
                        $he[$hv["ID"]] = 1;
                        print "Headend: " . $hv["ID"] . "\n";
                    }
                }
            }

            if (count($he))
            {
                print "A to add a new sourceid in MythTV\n";
                print "L to Link an existing sourceid to an existing headend at SD\n";
                print "Q to Quit\n";
                $response = strtoupper(readline(">"));

                switch ($response)
                {
                    case "A":
                        print "Adding new sourceid\n\n";
                        $newName = readline("Source name:>");
                        $stmt = $dbh->prepare("INSERT INTO videosource(name,userid,password)
                        VALUES(:name,:userid,:password)");
                        $stmt->execute(array("name"     => $newName, "userid" => $username,
                                             "password" => $password));
                        break;
                    case "L":
                        print "Linking Schedules Direct headend to sourceid\n\n";
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

function addHeadendsToSchedulesDirect($rh)
{
    print "\n\nNo headends are configured in your Schedules Direct account.\n";
    print "Enter your 5-digit zip code for U.S.\n";
    print "Enter your 6-character postal code for Canada.\n";
    print "Two-character ISO3166 code for international.\n";

    $response = readline(">");

    $res = array();
    $res["action"] = "get";
    $res["object"] = "headends";
    $res["randhash"] = $rh;
    $res["api"] = "20130512";
    $res["request"] = "PC:$response";

    $res = json_decode(sendRequest(json_encode($res)), true);

    foreach ($res["data"] as $v)
    {
        print "headend: " . $v["headend"] . "\nname: " . $v["name"] . "(" . $v["location"] . ")\n\n";
    }

    $he = readline("Headend to add>");
    if ($he == "")
    {
        return;
    }

    $res = array();
    $res["action"] = "add";
    $res["object"] = "headends";
    $res["randhash"] = $rh;
    $res["api"] = "20130512";
    $res["request"] = $he;

    $res = json_decode(sendRequest(json_encode($res)), true);

    if ($res["response"] == "OK")
    {
        print "Successfully added headend.\n";
    }
    else
    {
        print "\n\n-----\nERROR:Received error response from server:\n";
        print $res["message"] . "\n\n-----\n";
        print "Press ENTER to continue.\n";
        $a = fgets(STDIN);
    }
}

function getLineup($rh, array $he)
{
    print "Retrieving lineup from Schedules Direct.\n";

    $res = array();
    $res["action"] = "get";
    $res["object"] = "lineups";
    $res["randhash"] = $rh;
    $res["api"] = "20130512";
    $res["request"] = $he;

    return sendRequest(json_encode($res), true);
}

function commitToDb($dbh, array $stack, $base, $chunk, $useTransaction, $verbose)
{
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
            print "Loop: $loop of $numLoops\r";

            try
            {
                $count = $dbh->exec("$base$str");
            } catch (Exception $e)
            {
                print "Exception: " . $e->getMessage();
            }

            if ($count === FALSE)
            {
                print_r($dbh->errorInfo(), true);
                print "line:\n\n$base$str\n";
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

    print "\n";

    // Remainder
    $str = rtrim($str, ","); // Get rid of the last comma.

    $count = $dbh->exec("$base$str");
    if ($count === FALSE)
    {
        print_r($dbh->errorInfo(), true);
    }

    if ($verbose)
    {
        print "Done inserting.\n";
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


function getStatus($rh, $api)
{
    $res = array();
    $res["action"] = "get";
    $res["object"] = "status";
    $res["randhash"] = $rh;
    $res["api"] = $api;

    return sendRequest(json_encode($res));
}

function printStatus($dbh, $rh, $json)
{
    print "Status messages from Schedules Direct:\n";

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
        }
    }

    print "Server: " . $res["serverID"] . "\n";
    print "Last data refresh: " . $res["lastDataUpdate"] . "\n";
    print "Account expires: $expires\n";
    print "Max number of headends for your account: $maxHeadends\n";
    print "Next suggested connect time: $nextConnectTime\n";

    if (count($he))
    {
        $stmt = $dbh->prepare("SELECT modified FROM SDlineupCache WHERE headend=:he");
        print "The following headends are in your account:\n";

        $retrieveLineups = array();
        foreach ($he as $id => $modified)
        {
            print "ID: $id\t\tSD Modified:$modified\n";
            $stmt->execute(array("he" => $id));
            $result = $stmt->fetchAll(PDO::FETCH_COLUMN);

            if ((count($result) == 0) OR ($result[0] < $modified))
            {
                $retrieveLineups[] = $id;
            }
        }

        if (count($retrieveLineups))
        {
            processLineups($dbh, $rh, $retrieveLineups);
        }
    }
}

function processLineups($dbh, $rh, array $retrieveLineups)
{
    /*
     * If we're here, that means that either the lineup has been updated, or it didn't exist at all.
     * The overall group of lineups in a headend have a modified date based on the "max" of the modified dates
     * of the lineups in the headend. But we may not be using that particular lineup, so dig deeper...
     */

    print "Checking for updated lineups from Schedules Direct.\n";

    $res = array();
    $res = json_decode(getLineup($rh, $retrieveLineups), true);

    if ($res["response"] != "OK")
    {
        print "\n\n-----\nERROR: Bad response from Schedules Direct.\n";
        print $res["message"] . "\n\n-----\n";
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
        print "FATAL: Could not open lineups zip file.\n";
        print "tempdir is $tempDir\n";
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
        // print "headend:$headend device:$device modified:$modified\n";
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
                print "$headend:$device local modified date:" . $lineup[$lineupid]["modified"] . "\n";
                print "server modified date:$jsonModified\n";
                print "Use new lineup?\n";
                $updateDB = strtoupper(readline(">"));
                if ($updateDB == "Y")
                {
                    updateChannelTable($dbh, $lineupid, $headend, $device, $transport, $json);
                }
            }
        }


    }
    $tt = fgets(STDIN);
}

function updateChannelTable($dbh, $sourceid, $he, $dev, $transport, array $json)
{
    print "Updating channel table for sourceid:$sourceid\n";
    $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sourceid");
    if ($sourceid > 1)
    {
        $stmt->execute(array("sourceid" => $sourceid));
    }

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
                                 "freqid" => $freqid, "sourceid" => $sourceid, "xmltvid" => $stationID));
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

    print "***DEBUG: Exiting updateChannelTable.\n";
    $tt = fgets(STDIN);
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
                                               'timeout' => 480,
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
  UNIQUE KEY `pid-MD5` (`programID`,`md5`)
  )
  ENGINE = InnoDB DEFAULT CHARSET=utf8");

    $stmt->execute();
}

?>