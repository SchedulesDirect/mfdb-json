#!/usr/bin/php

<?php
$isBeta = FALSE;
$debug = TRUE;
$doSetup = FALSE;
$quiet = FALSE;
$schedulesDirectHeadends = array();

$headendToRefresh = array();

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

if ($doSetup)
{
    setup();
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

printMSG("Logging into Schedules Direct.\n");
$randHash = getRandhash($username, $password);

if ($randHash != "ERROR")
{
    printStatus(getStatus());
}

function setup()
{
    global $dbh;
    global $schedulesDirectHeadends;

    $username = readline("Schedules Direct username:");
    $password = readline("Schedules Direct password:");

    printMSG("Checking existing lineups at Schedules Direct.\n");

    if (count($he))
    {
        displayLocalVideoSource();

        printMSG("A to add a new videosource to MythTV\n");
        printMSG("L to Link a videosource to a headend at SD\n");
        printMSG("Q to Quit\n");
        $response = strtoupper(readline(">"));

        switch ($response)
        {
            case "A":
                printMSG("Adding new videosource\n\n");
                $newName = readline("Name:>");
                $stmt = $dbh->prepare("INSERT INTO videosource(name,userid,password)
                        VALUES(:name,:userid,:password)");
                $stmt->execute(array("name"     => $newName, "userid" => $username,
                                     "password" => $password));
                break;
            case "L":
                printMSG("Linking Schedules Direct headend to sourceid\n\n");
                $sid = readline("Source id:>");
                $he = readline("Headend:>");

                /*
                 * TODO: Add a way to pull a specific lineup from the headend
                 */

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

function addHeadendsToSchedulesDirect()
{
    global $randHash;
    global $api;

    printMSG("No headends are configured in your Schedules Direct account.\n");
    printMSG("Two-character ISO3166 country code: (CA, US or ZZ");
    $country = readline(">");
    printMSG("Enter your 5-digit zip code for U.S.\n");
    printMSG("Enter leftmost 4-character postal code for Canada.\n");

    $postalcode = readline(">");

    $res = array();
    $res["action"] = "get";
    $res["object"] = "headends";
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = array("country" => $country, "postalcode" => "PC:$postalcode");

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
        printMSG("ERROR:Received error response from server:\n");
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

function getStatus()
{
    global $api;
    global $randHash;

    $res = array();
    $res["action"] = "get";
    $res["object"] = "status";
    $res["randhash"] = $randHash;
    $res["api"] = $api;

    $response = sendRequest(json_encode($res));

    var_dump($response);
    exit;

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
        printMSG("The following headends are in your account:\n\n");

        $retrieveLineups = array();
        foreach ($he as $id => $modified)
        {
            $line = "ID: $id\t\t";
            if (strlen($id) < 4)
            {
                // We want the tabs to align.
                $line .= "\t";
            }
            $line .= "Last Updated: $modified\n";
            printMSG($line);
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

    if ($res["code"] != 0)
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

function displayLocalVideoSource()
{
    global $dbh;

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
}

function getSchedulesDirectHeadends()
{
    global $randHash;
    $res = array();
    $res = json_decode(getStatus(), true);

    foreach ($res as $k => $v)
    {
        if ($k == "headend")
        {
            foreach ($v as $hv)
            {
                $schedulesDirectHeadends[$hv["ID"]] = 1;
                printMSG("Headend: " . $hv["ID"] . "\n");
            }
        }
    }
}

?>