#!/usr/bin/php

<?php
$isBeta = TRUE;
$debug = TRUE;
$doSetup = FALSE;
$done = FALSE;
$schedulesDirectHeadends = array();
$sdStatus = "";
$username = "";
$password = "";

$updatedHeadendsToRefresh = array();

date_default_timezone_set("UTC");
$date = new DateTime();
$todayDate = $date->format("Y-m-d");
$fh_log = fopen("$todayDate.log", "a");

$dbUser = "mythtv";
$dbPassword = "mythtv";
$host = "localhost";
$db = "mythconverg";

$longoptions = array("beta::", "debug::", "help::", "host::", "dbpassword::", "dbuser::", "setup::", "username::",
    "password::");

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
            print "--dbuser=\t\tUsername to access database\n";
            print "--dbpassword=\tPassword to access database.\n";
            print "--username=\tSchedules Direct username.\n";
            print "--password=\tSchedules Direct password.\n";
            exit;
        case "host":
            $host = $v;
            break;
        case "dbpassword":
            $dbPassword = $v;
            break;
        case "dbuser":
            $dbUser = $v;
            break;
        case "username":
            $username = $v;
            break;
        case "password":
            $password = $v;
            $passwordHash = sha1($v);
            break;
        case "setup":
            $doSetup = TRUE;
            break;
    }
}

print "Attempting to connect to database.\n";
try
{
    $dbh = new PDO("mysql:host=$host;dbname=$db;charset=utf8", $dbUser, $dbPassword,
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
    print "Using beta server.\n";
    # API must match server version.
    $api = 20130709;
}
else
{
    $baseurl = "https://data2.schedulesdirect.org";
    print "Using production server.\n";
    $api = 20130512;
}

if ($username == "" AND $password == "")
{
    $stmt = $dbh->prepare("SELECT userid,password FROM videosource WHERE xmltvgrabber='schedulesdirect1' LIMIT 1");
    $stmt->execute();
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    if (isset($result[0]["userid"]))
    {
        $username = $result[0]["userid"];
        $userPassword = $result[0]["password"];
        $passwordHash = sha1($userPassword);
    }
    else
    {
        $username = readline("Schedules Direct username:");
        $password = readline("Schedules Direct password:");
        $passwordHash = sha1($password);
    }
}

print "Logging into Schedules Direct.\n";
$randHash = getRandhash($username, $passwordHash);
if ($randHash != "ERROR")
{
    getStatus();
    printStatus();
}

while (!$done)
{
    print "Main Menu:\n";
    print "1 Add a headend to account at Schedules Direct\n";
    print "2 Delete a headend from account at Schedules Direct\n";
    print "L to Link a videosource to a headend at SD\n";
    print "S to Setup\n";
    print "V to add a new videosource to MythTV\n";
    print "Q to Quit\n";

    $response = strtoupper(readline(">"));

    switch ($response)
    {

        case "L":
            print "Linking Schedules Direct headend to sourceid\n\n";
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
        case "S":
            getSchedulesDirectHeadends();
            setup();
            break;
        case "V":
            print "Adding new videosource\n\n";
            $newName = readline("Name:>");
            $stmt = $dbh->prepare("INSERT INTO videosource(name,userid,password,xmltvgrabber)
                        VALUES(:name,:userid,:password,'schedulesdirect1')");
            $stmt->execute(array("name"     => $newName, "userid" => $username,
                                 "password" => $password));
            break;
        case "Q":
        default:
            $done = TRUE;
            break;

    }
}

exit;

if (count($updatedHeadendsToRefresh))
{
    foreach ($updatedHeadendsToRefresh as $he => $modified)
    {
        print "Headend update for $he\n";
        $response = strtoupper(readline("Use entire lineup? (Y/n)>"));
        if ($response == "" OR $response == "Y")
        {

        }
    }
}

function setup()
{
    global $dbh;
    global $schedulesDirectHeadends;
    global $randHash;

    print "Checking existing lineups at Schedules Direct.\n";

    if (count($schedulesDirectHeadends))
    {
        displayLocalVideoSource();


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

    print "Two-character ISO3166 country code: (CA, US or ZZ)";
    $country = readline(">");
    print "Enter your 5-digit zip code for U.S.\n";
    print "Enter leftmost 4-character postal code for Canada.\n";

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
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = $he;

    $res = json_decode(sendRequest(json_encode($res)), true);

    if ($res["response"] == "OK")
    {
        print "Successfully added headend.\n";
    }
    else
    {
        print "ERROR:Received error response from server:\n";
        print$res["message"] . "\n\n-----\n";
        print "Press ENTER to continue.\n";
        $a = fgets(STDIN);
    }
}

function getLineup(array $heToGet)
{
    global $randHash;
    global $api;

    print "Retrieving lineup from Schedules Direct.\n";

    $res = array();
    $res["action"] = "get";
    $res["object"] = "lineups";
    $res["randhash"] = $randHash;
    $res["api"] = $api;
    $res["request"] = array_keys($heToGet);

    return sendRequest(json_encode($res), true);
}

function getStatus()
{
    global $api;
    global $randHash;
    global $sdStatus;

    $res = array();
    $res["action"] = "get";
    $res["object"] = "status";
    $res["randhash"] = $randHash;
    $res["api"] = $api;

    $sdStatus = sendRequest(json_encode($res));
}

function printStatus()
{
    global $dbh;
    global $sdStatus;
    global $updatedHeadendsToRefresh;
    global $printTS;

    $printTS = FALSE;

    print "Status messages from Schedules Direct:\n";

    $res = array();
    $res = json_decode($sdStatus, TRUE);

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
            case "code":
                if ($v != 0)
                {
                    /*
                     * Error notification - we're going to have to abort because the server didn't like what we sent.
                     */
                    print "Received error response from server!\n";
                    print "ServerID: " . $res["serverID"] . "\n";
                    print "Message: " . $res["message"] . "\n";
                    print "\nFATAL ERROR. Terminating execution.\n";
                    exit;
                }
        }
    }

    print "Server: " . $res["serverID"] . "\n";
    print "Last data refresh: " . $res["lastDataUpdate"] . "\n";
    print "Account expires: $expires\n";
    print "Max number of headends for your account: $maxHeadends\n";
    print "Next suggested connect time: $nextConnectTime\n";

    $getLocalModified = $dbh->prepare("SELECT modified FROM headendCacheSD WHERE headend=:he");
    print "The following headends are in your account at Schedules Direct:\n\n";

    $he = getSchedulesDirectHeadends();

    if (count($he))
    {
        foreach ($he as $id => $modified)
        {
            $line = "ID: $id\t\t";
            if (strlen($id) < 4)
            {
                // We want the tabs to align.
                $line .= "\t";
            }
            $line .= "Last Updated: $modified\n";
            print "$line\n";
            $getLocalModified->execute(array("he" => $id));
            $result = $getLocalModified->fetchAll(PDO::FETCH_COLUMN);

            if ((count($result) == 0) OR ($result[0] < $modified))
            {
                $updatedHeadendsToRefresh[$id] = $modified;
            }
        }

        if (count($updatedHeadendsToRefresh))
        {
            updateLocalHeadendCache($updatedHeadendsToRefresh);
        }
    }
}

function updateLocalHeadendCache(array $updatedHeadendsToRefresh)
{
    global $dbh;

    /*
     * If we're here, that means that either the lineup has been updated, or it didn't exist at all.
     * The overall group of lineups in a headend have a modified date based on the "max" of the modified dates
     * of the lineups in the headend. But we may not be using that particular lineup, so dig deeper...
     */

    print "Checking for updated lineups from Schedules Direct.\n";

    $res = array();
    $res = json_decode(getLineup($updatedHeadendsToRefresh), true);

    if ($res["code"] != 0)
    {
        print "\n\n-----\nERROR: Bad response from Schedules Direct.\n";
        print$res["message"] . "\n\n-----\n";
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
     * Store a copy of the data that we just downloaded into the cache.
     */
    $stmt = $dbh->prepare("REPLACE INTO headendCacheSD(headend,json,modified) VALUES(:he,:json,:modified)");
    foreach (glob("$tempDir/*.json.txt") as $f)
    {
        $json = file_get_contents($f);
        $a = json_decode($json, true);
        $he = $a["headend"];

        $stmt->execute(array("he" => $he, "modified" => $updatedHeadendsToRefresh[$he], "json" => $json));
    }
}

function test()
{
    global $dbh;

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

    $stmt = $dbh->prepare("SELECT json FROM headendCacheSD WHERE headend=:he");
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
                // Eventually we won't print "once coding is done.
                print "$headend:$device local modified date:" . $lineup[$lineupid]["modified"] . "\n";
                print "server modified date:$jsonModified\n";
                if ($jsonModified != $lineup[$lineupid]["modified"])
                {
                    print "Use new lineup?\n";
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

    print "Updating channel table for sourceid:$sourceid\n";
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
        print "Adding QAM data.\n";
        $dtvMultiplex = array();

        $channelInsert =
            $dbh->prepare("UPDATE channel SET tvformat='ATSC',visible='1',mplexid=:mplexid,serviceid=:qamprogram
        WHERE xmltvid=:stationID");

        $qamModified = $json["QAM"]["metadata"]["modified"];
        print "qam modified:$qamModified\n";

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

            // print "$stationID $qamType $qamFreq $qamProgram $channel\n";

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

    print "***DEBUG: Exiting updateChannelTable.\n";
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

function getRandhash($username, $passwordHash)
{
    global $api;
    $res = array();
    $res["action"] = "get";
    $res["object"] = "randhash";
    $res["request"] = array("username" => $username, "password" => $passwordHash);
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
        print "tempdir is $tempfile\n";

        return $tempfile;
    }
}

function displayLocalVideoSource()
{
    global $dbh;

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
}

function getSchedulesDirectHeadends()
{
    global $sdStatus;
    $schedulesDirectHeadends = array();

    $status = json_decode($sdStatus, TRUE);
    foreach ($status["headend"] as $hv)
    {
        $schedulesDirectHeadends[$hv["ID"]] = $hv["modified"];
    }

    return ($schedulesDirectHeadends);
}

?>