#!/usr/bin/php

<?php
/*
 * This file is a utility program which performs the necessary setup for using the Schedules Direct API and the
 * native MythTV tables.
 * Robert Kulagowski
 * grabber@schedulesdirect.org
 *
 */
$isBeta = FALSE;
$debug = FALSE;
$done = FALSE;
$test = FALSE;
$schedulesDirectLineups = array();
$sdStatus = "";
$username = "";
$password = "";
$passwordHash = "";
$scriptVersion = "0.18";
$scriptDate = "2014-04-29";
$useServiceAPI = FALSE;

require_once "vendor/autoload.php";
require_once "functions.php";
use Guzzle\Http\Client;

$agentString = "sd-utility.php utility program v$scriptVersion/$scriptDate";

$updatedLineupsToRefresh = array();
$needToStoreUserPassword = FALSE;

$tz = "UTC";

date_default_timezone_set($tz);
$date = new DateTime();
$todayDate = $date->format("Y-m-d");

$fh_log = fopen("$todayDate.log", "a");
$fh_error = fopen("$todayDate.debug.log", "a");

$dbUser = "mythtv";
$dbPassword = "mythtv";
$dbHost = "localhost";
$dbName = "mythconverg";
$host = "localhost";

$helpText = <<< eol
The following options are available:
--beta
--debug\t\tEnable debugging. (Default: FALSE)
--dbname=\tMySQL database name. (Default: $dbName)
--dbuser=\tUsername for database access. (Default: $dbUser)
--dbpassword=\tPassword for database access. (Default: $dbPassword)
--dbhost=\tMySQL database hostname. (Default: $dbHost)
--help\t\t(this text)
--host=\t\tIP address of the MythTV backend. (Default: $host)
--username=\tSchedules Direct username.
--password=\tSchedules Direct password.
--timezone=\tSet the timezone for log file timestamps. See http://www.php.net/manual/en/timezones.php (Default:$tz)
eol;

$longoptions = array("beta", "debug", "help", "host::", "dbname::", "dbuser::", "dbpassword::", "dbhost::",
                     "username::", "password::", "test", "timezone::");

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
            print "$agentString\n\n";
            print "$helpText\n";
            exit;
            break;
        case "dbname":
            $dbName = $v;
            break;
        case "dbuser":
            $dbUser = $v;
            break;
        case "dbpassword":
            $dbPassword = $v;
            break;
        case "dbhost":
            $dbHost = $v;
            break;
        case "host":
            $host = $v;
            break;
        case "username":
            $username = $v;
            break;
        case "password":
            $password = $v;
            $passwordHash = sha1($v);
            break;
        case "test":
            $test = TRUE;
            break;
        case "timezone":
            date_default_timezone_set($v);
            break;
    }
}

print "$agentString\n";
print "Attempting to connect to database.\n";
try
{
    $dbh = new PDO("mysql:host=$dbHost;dbname=$dbName;charset=utf8", $dbUser, $dbPassword,
        array(PDO::ATTR_PERSISTENT => true));
    $dbh->exec("SET CHARACTER SET utf8");
    $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
} catch (PDOException $e)
{
    print "Exception with PDO: " . $e->getMessage() . "\n";
    exit;
}

checkDatabase();

if ($isBeta)
{
    # Test server. Things may be broken there.
    $baseurl = "https://json.schedulesdirect.org/20131021/";
    print "Using beta server.\n";
    # API must match server version.
    $api = 20131021;
}
else
{
    $baseurl = "https://json.schedulesdirect.org/20131021/";
    print "Using production server.\n";
    $api = 20131021;
}

$client = new Guzzle\Http\Client($baseurl);
$client->setUserAgent($agentString);

$useServiceAPI = checkForServiceAPI();

if ($test)
{
}

if ($username == "" AND $password == "")
{
    $stmt = $dbh->prepare("SELECT userid,password FROM videosource WHERE xmltvgrabber='schedulesdirect2'
    AND password != '' LIMIT 1");
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
        $needToStoreUserPassword = TRUE;
    }
}
else
{
    if ($username == "")
    {
        $username = readline("Schedules Direct username:");
    }

    if ($password == "")
    {
        $password = readline("Schedules Direct password:");
        $passwordHash = sha1($password);
        $needToStoreUserPassword = TRUE;
    }
}

print "Logging into Schedules Direct.\n";
$token = getToken($username, $passwordHash);

if ($token == "ERROR")
{
    print("Got error when attempting to retrieve token from Schedules Direct.\n");
    print("Check username / password in videosource table, or check if you entered it incorrectly when typing.\n");
    exit;
}
elseif ($needToStoreUserPassword)
{
    $stmt = $dbh->prepare("UPDATE videosource SET userid=:user,password=:password WHERE password IS NULL");
    $stmt->execute(array("user" => $username, "password" => $password));
}

while (!$done)
{
    $sdStatus = getStatus();

    if ($sdStatus == "ERROR")
    {
        printMSG("Received error from Schedules Direct. Exiting.");
        exit;
    }

    printStatus($sdStatus);

    displayLocalVideoSources();

    print "\nSchedules Direct functions:\n";
    print "1 Add a lineup to your account at Schedules Direct\n";
    print "2 Delete a lineup from your account at Schedules Direct\n";
    print "3 Acknowledge a message\n";
    print "4 Print a channel table for a lineup\n";

    print "\nMythTV functions:\n";
    print "A to Add a new videosource to MythTV\n";
    print "D to Delete a videosource in MythTV\n";
    print "L to Link a videosource to a lineup at Schedules Direct\n";
    print "R to refresh a videosource with new lineup information\n";
    print "Q to Quit\n";

    $response = strtoupper(readline(">"));

    switch ($response)
    {
        case "1":
            addLineupsToSchedulesDirect();
            break;
        case "2":
            deleteLineupFromSchedulesDirect();
            break;
        case "3":
            deleteMessageFromSchedulesDirect();
            break;
        case "4":
            printLineup();
            break;
        case "A":
            print "Adding new videosource\n\n";
            $newName = readline("Name:>");
            if ($newName != "")
            {
                $stmt = $dbh->prepare("INSERT INTO videosource(name,userid,password,xmltvgrabber)
                        VALUES(:name,:userid,:password,'schedulesdirect2')");
                try
                {
                    $stmt->execute(array("name"     => $newName, "userid" => $username,
                                         "password" => $password));
                } catch (PDOException $e)
                {
                    if ($e->getCode() == 23000)
                    {
                        print "\n\n";
                        print "*************************************************************\n";
                        print "\n\n";
                        print "Duplicate video source name.\n";
                        print "\n\n";
                        print "*************************************************************\n";
                    }
                }
            }
            break;
        case "D":
            $toDelete = readline("Delete sourceid:>");
            $stmt = $dbh->prepare("DELETE FROM videosource WHERE sourceid=:sid");
            $stmt->execute(array("sid" => $toDelete));
            $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sid");
            $stmt->execute(array("sid" => $toDelete));
            break;
        case "L":
            print "Linking Schedules Direct lineup to sourceid\n\n";
            linkSchedulesDirectLineup();
            break;
        case "R":
            $lineup = strtoupper(readline("Which lineup:>"));
            if ($lineup != "")
            {
                updateChannelTable($lineup);
            }
            break;
        case "Q":
        default:
            $done = TRUE;
            break;
    }
}

exit;

function updateChannelTable($lineup)
{
    global $dbh;
    $transport = "";

    $stmt = $dbh->prepare("SELECT sourceid FROM videosource WHERE lineupid=:lineup");
    $stmt->execute(array("lineup" => $lineup));
    $sID = $stmt->fetchAll(PDO::FETCH_COLUMN);

    if (count($sID) == 0)
    {
        print "ERROR: You do not have that lineup locally configured.\n";

        return;
    }

    $stmt = $dbh->prepare("SELECT json FROM SDheadendCache WHERE lineup=:lineup");
    $stmt->execute(array("lineup" => $lineup));
    $json = json_decode($stmt->fetchColumn(), TRUE);

    $modified = $json["metadata"]["modified"];

    print "Updating channel table for lineup:$lineup\n";

    foreach ($sID as $sourceID)
    {
        if ($json["metadata"]["transport"] == "Antenna")
        {
            /*
             * For antenna lineups, we're not going to delete the existing channel table or dtv_multiplex; we're still
             * going to use the scan, but use the atsc major and minor to correlate what we've scanned with what's in the
             * lineup file.
             */

            /*
             * TODO Check if we need to fix chanid for Antenna lineups.
             */

            $transport = "Antenna";
        }

        if ($json["metadata"]["transport"] == "Cable")
        {
            $transport = "Cable";
            $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sourceid");
            $stmt->execute(array("sourceid" => $sourceID));
        }

        if ($json["metadata"]["transport"] == "QAM")
        {
            /*
             * TODO Lots of updates for QAM
             */

            $transport = "QAM";
            $qamModified = "";

            foreach ($json["metadata"] as $v)
            {
                if ($v["device"] == "Q")
                {
                    $qamModified = $v["modified"];
                }
            }

            print "QAM modified:$qamModified\n";

            $dtvMultiplex = array();

            $channelInsertQAM =
                $dbh->prepare("UPDATE channel SET tvformat='ATSC',visible='1',mplexid=:mplexid,serviceid=:qamprogram
        WHERE xmltvid=:stationID");
            $insertDTVMultiplex = $dbh->prepare
                ("INSERT INTO dtv_multiplex
        (sourceid,frequency,symbolrate,polarity,modulation,visible,constellation,hierarchy,mod_sys,rolloff,sistandard)
        VALUES
        (:sourceid,:freq,0,'v','qam_256',1,'qam_256','a','UNDEFINED','0.35','atsc')");
        }

        $updateChannelTableATSC = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=:atscMajor AND atsc_minor_chan=:atscMinor");

        $updateChannelTableAnalog = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=0 AND atsc_minor_chan=0 AND freqID=:freqID");

        foreach ($json["map"] as $mapArray)
        {
            $stationID = $mapArray["stationID"];

            if ($transport == "Antenna")
            {
                if (array_key_exists("uhfVhf", $mapArray))
                {
                    $freqid = $mapArray["uhfVhf"];
                }
                else
                {
                    $freqid = "";
                }

                if (isset($mapArray["atscMajor"]))
                {
                    $atscMajor = $mapArray["atscMajor"];
                    $atscMinor = $mapArray["atscMinor"];
                    $channum = "$atscMajor.$atscMinor";

                    $updateChannelTableATSC->execute(array("channum"   => $channum, "sid" => $stationID,
                                                           "atscMajor" => $atscMajor,
                                                           "atscMinor" => $atscMinor));
                }
                else
                {
                    $channum = $freqid;
                    $updateChannelTableAnalog->execute(array("channum" => ltrim($channum, "0"), "sid" => $stationID,
                                                             "freqID"  => $freqid));
                }
            }

            if ($transport == "IP")
            {
                /*
                 * Nothing yet.
                 */
            }

            if ($transport == "Cable")
            {
                $channum = $mapArray["channel"];
                $stmt = $dbh->prepare(
                    "INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid)
                     VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid)");

                try
                {
                    $stmt->execute(array("chanid"  => (int)($sourceID * 1000) + (int)$channum,
                                         "channum" => ltrim($channum, "0"),
                                         "freqid"  => $channum, "sourceid" => $sourceID, "xmltvid" => $stationID));
                } catch (PDOException $e)
                {
                    if ($e->getCode() == 23000)
                    {
                        print "\n\n";
                        print "*************************************************************\n";
                        print "\n\n";
                        print "Error inserting data. Duplicate channel number exists?\n";
                        print "Send email to grabber@schedulesdirect.org with the following:\n\n";
                        print "Duplicate channel error.\n";
                        print "Transport: $transport\n";
                        print "Lineup: $lineup\n";
                        print "Channum: $channum\n";
                        print "stationID: $stationID\n";
                        print "\n\n";
                        print "*************************************************************\n";
                    }
                }
            }

            if ($transport == "QAM")
            {
                $stmt = $dbh->prepare(
                    "INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid)
                     VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid)");

                $stationID = $mapArray["stationID"];
                $qamType = $mapArray["qamType"];
                $qamProgram = $mapArray["qamProgram"];
                $qamFreq = $mapArray["qamFreq"];
                $channel = $mapArray["channel"];

                if (isset($mapArray["virtualChannel"]))
                {
                    $virtualChannel = $mapArray["virtualChannel"];
                }
                else
                {
                    $virtualChannel = "";
                }

                try
                {
                    $stmt->execute(array("chanid" => (int)($sourceID * 1000) + (int)$channel, "channum" => $channel,
                                         "freqid" => $channel, "sourceid" => $sourceID, "xmltvid" => $stationID));
                } catch (PDOException $e)
                {
                    if ($e->getCode() == 23000)
                    {
                        print "\n\n";
                        print "*************************************************************\n";
                        print "\n\n";
                        print "Error inserting data. Duplicate channel number exists?\n";
                        print "Send email to grabber@schedulesdirect.org with the following:\n\n";
                        print "Duplicate channel error.\n";
                        print "Transport: $transport\n";
                        print "Lineup: $lineup\n";
                        print "Channum: $channel\n";
                        print "stationID: $stationID\n";
                        print "\n\n";
                        print "*************************************************************\n";
                    }
                }

                /*
                 * Because multiple programs may end up on a single frequency, we only want to insert once, but we want
                 * to track the mplexid assigned when we do the insert, because that might be used more than once.
                 */

                if (!isset($dtvMultiplex[$qamFreq]))
                {
                    $insertDTVMultiplex->execute(array("sourceid" => $sourceID, "freq" => $qamFreq));
                    $dtvMultiplex[$qamFreq] = $dbh->lastInsertId();
                }

                $channelInsertQAM->execute(array("mplexid"   => $dtvMultiplex[$qamFreq], "qamprogram" => $qamProgram,
                                                 "stationID" => $stationID));
            }
        }

        /*
         * Now that we have basic information in the database, we can start filling in other things, like
         * callsigns, etc.
         */
    }

    $stmt = $dbh->prepare("UPDATE channel SET name=:name, callsign=:callsign WHERE xmltvid=:stationID");
    foreach ($json["stations"] as $stationArray)
    {
        $stationID = $stationArray["stationID"];
        $name = $stationArray["name"];
        $callsign = $stationArray["callsign"];
        $stmt->execute(array("name" => $name, "callsign" => $callsign, "stationID" => $stationID));
    }

    $updateVideosourceModified = $dbh->prepare("UPDATE videosource SET modified = :modified WHERE lineupid=:lineup");
    $updateVideosourceModified->execute(array("lineup" => $lineup, "modified" => $modified));

    /*
     * Set the startchan to a non-bogus value.
     */
    $stmt = $dbh->prepare("SELECT channum FROM channel WHERE sourceid=:sourceid
    ORDER BY CAST(channum AS SIGNED) LIMIT 1");

    foreach ($s as $sourceID)
    {
        $stmt->execute(array("sourceid" => $sourceID));
        $result = $stmt->fetchAll(PDO::FETCH_COLUMN);
        if (count($result))
        {
            $startChan = $result[0];
            $setStartChannel = $dbh->prepare("UPDATE cardinput SET startchan=:startChan WHERE sourceid=:sourceid");
            $setStartChannel->execute(array("sourceid" => $sourceID, "startChan" => $startChan));
        }
    }
}

function linkSchedulesDirectLineup()
{
    global $dbh;

    $sid = readline("MythTV sourceid:>");
    $he = strtoupper(readline("Schedules Direct lineup:>"));

    $stmt = $dbh->prepare("SELECT json FROM SDheadendCache WHERE lineup=:he");
    $stmt->execute(array("he" => $he));
    $response = json_decode($stmt->fetchColumn(), TRUE);

    if (!count($response))
    {
        return;
    }

    $stmt = $dbh->prepare("UPDATE videosource SET lineupid=:he WHERE sourceid=:sid");
    $stmt->execute(array("he" => $he, "sid" => $sid));
}

function printLineup()
{
    global $dbh;

    /*
     * First we want to get the lineup that we're interested in.
     */

    $he = strtoupper(readline("Lineup to print:>"));
    $stmt = $dbh->prepare("SELECT json FROM SDheadendCache WHERE lineup=:he");
    $stmt->execute(array("he" => $he));
    $response = json_decode($stmt->fetchColumn(), TRUE);

    if (!count($response))
    {
        return;
    }

    print "\n";

    $chanMap = array();
    $stationMap = array();

    if ($response["metadata"]["transport"] == "Antenna")
    {
        foreach ($response["map"] as $v)
        {
            if (isset($v["atscMajor"]))
            {
                $chanMap[$v["stationID"]] = "{$v["atscMajor"]}.{$v["atscMinor"]}";
            }
            else
            {
                $chanMap[$v["stationID"]] = $v["uhfVhf"];
            }
        }
    }
    else
    {
        foreach ($response["map"] as $v)
        {
            $chanMap[$v["stationID"]] = $v["channel"];
        }
    }

    foreach ($response["stations"] as $v)
    {
        if (array_key_exists("affiliate", $v))
        {
            $stationMap[$v["stationID"]] = "{$v["callsign"]} ({$v["affiliate"]})";
        }
        else
        {
            $stationMap[$v["stationID"]] = "{$v["callsign"]}";
        }
    }

    asort($chanMap, SORT_NATURAL);

    $stationData = new Zend\Text\Table\Table(array('columnWidths' => array(8, 50, 10)));
    $stationData->appendRow(array("Channel", "Callsign", "stationID"));

    foreach ($chanMap as $stationID => $channel)
    {
        $stationData->appendRow(array((string)$channel, $stationMap[$stationID], (string)$stationID));
    }

    print $stationData;
}

function addLineupsToSchedulesDirect()
{
    global $client;
    global $debug;
    global $token;

    print "Three-character ISO-3166-1 alpha3 country code:";
    $country = strtoupper(readline(">"));

    if ($country == "")
    {
        return;
    }

    print "Enter postal code:";
    $postalcode = strtoupper(readline(">"));

    if ($postalcode == "")
    {
        return;
    }

    try
    {
        $response = $client->get("headends", array(), array(
            "query"   => array("country" => $country, "postalcode" => $postalcode),
            "headers" => array("token" => $token)))->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        $s = json_decode($e->getResponse()->getBody(TRUE), TRUE);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug)
    {
        debugMSG("addLineupsToSchedulesDirect:Response:$res");
        debugMSG("Raw headers:\n" . $response->getRawHeaders());
    }

    foreach ($res as $he => $details)
    {
        print "\nheadend: $he\n";
        print "location: {$details["location"]}\n";
        foreach ($details["lineups"] as $v)
        {
            print "\tname: {$v["name"]}\n";
            print "\tLineup: " . end(explode("/", $v["uri"])) . "\n";
        }
    }

    print "\n\n";
    $he = strtoupper(readline("Lineup to add>"));

    if ($he == "")
    {
        return;
    }

    if (substr_count($he, "-") != 2)
    {
        print "Did not see two hyphens in lineup; did you enter it correctly?\n";

        return;
    }

    print "Sending request to server.\n";
    $he = str_replace(" ", "", $he);

    try
    {
        $response = $client->put("lineups/$he", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        $s = json_decode($e->getResponse()->getBody(TRUE), TRUE);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug)
    {
        debugMSG("addLineupsToSchedulesDirect:Response:$res");
        debugMSG("Raw headers:\n" . $response->getRawHeaders());
    }

    print "Message from server: {$res["message"]}\n";
}

function deleteLineupFromSchedulesDirect()
{
    global $dbh;
    global $debug;
    global $client;
    global $token;
    global $updatedLineupsToRefresh;

    $deleteCache = $dbh->prepare("DELETE FROM SDheadendCache WHERE lineup=:lineup");

    $toDelete = strtoupper(readline("Lineup to Delete:>"));

    try
    {
        $response = $client->delete("lineups/$toDelete", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        $s = json_decode($e->getResponse()->getBody(TRUE), TRUE);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug)
    {
        debugMSG("deleteLineupFromSchedulesDirect:Response:$res");
        debugMSG("Raw headers:\n" . $response->getRawHeaders());
    }

    print "Message from server: {$res["message"]}\n";
    unset ($updatedLineupsToRefresh[$toDelete]);
    $deleteCache->execute(array("lineup" => $toDelete));
}

function deleteMessageFromSchedulesDirect()
{
    global $client;
    global $debug;
    global $token;

    $toDelete = readline("MessageID to acknowledge:>");

    try
    {
        $response = $client->delete("messages/$toDelete", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        $s = json_decode($e->getResponse()->getBody(TRUE), TRUE);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug)
    {
        print "\n\n******************************************\n";
        print "Raw headers:\n";
        print $response->getRawHeaders();
        print "******************************************\n";
        print "deleteMessageFromSchedulesDirect:Response:$res\n";
        print "******************************************\n";
    }

    print "Message from server: {$res["message"]}\n";
    print "Successfully deleted message.\n";
}

function getLineup($heToGet)
{
    global $client;
    global $debug;
    global $token;

    print "Retrieving lineup $heToGet from Schedules Direct.\n";

    try
    {
        $response = $client->get("lineups/$heToGet", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        $s = json_decode($e->getResponse()->getBody(TRUE), TRUE);

        var_dump($s);


        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return "";
    }

    $res = $response->json();

    if ($debug)
    {
        print "\n\n******************************************\n";
        print "Raw headers:\n";
        print $response->getRawHeaders();
        print "******************************************\n";
        print "getLineup:Response:\n";
        var_dump($res);
        print "\n\n";
        print "******************************************\n";
    }

    return $res;
}

function updateLocalLineupCache(array $updatedLineupsToRefresh)
{
    global $dbh;

    print "Checking for updated lineups from Schedules Direct.\n";

    foreach ($updatedLineupsToRefresh as $k => $v)
    {
        $res = array();
        $res = getLineup($k);

        if (isset($res["code"]))
        {
            print "\n\n-----\nERROR: Bad response from Schedules Direct.\n";
            print $res["message"] . "\n\n-----\n";
            exit;
        }

        /*
         * Store a copy of the data that we just downloaded into the cache.
         */
        $stmt = $dbh->prepare("INSERT INTO SDheadendCache(lineup,json,modified)
        VALUES(:lineup,:json,:modified) ON DUPLICATE KEY UPDATE json=:json,modified=:modified");

        $stmt->execute(array("lineup" => $k, "modified" => $updatedLineupsToRefresh[$k],
                             "json"   => json_encode($res)));
    }
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

function displayLocalVideoSources()
{
    global $dbh;

    $stmt = $dbh->prepare("SELECT sourceid,name,lineupid FROM videosource ORDER BY sourceid");
    $stmt->execute();
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    if (count($result))
    {
        print "\nMythTV local videosources:\n";
        foreach ($result as $v)
        {
            print "sourceid: " . $v["sourceid"] . "\tname: " . $v["name"] . "\tSchedules Direct lineup: " .
                $v["lineupid"] . "\n";
        }
    }
    else
    {
        print "\nWARNING: *** No videosources configured in MythTV. ***\n";
    }
}

function getSchedulesDirectLineups()
{
    global $sdStatus;
    $schedulesDirectLineups = array();

    foreach ($sdStatus["lineups"] as $hv)
    {
        $schedulesDirectLineups [$hv["ID"]] = $hv["modified"];
    }

    return ($schedulesDirectLineups);
}

function checkDatabase()
{
    global $dbh;

    $stmt = $dbh->prepare("DESCRIBE videosource");
    $stmt->execute();
    $columnNames = $stmt->fetchAll(PDO::FETCH_COLUMN);

    if (!in_array("modified", $columnNames))
    {
        print "Adding 'modified' field to videosource.\n";
        $stmt = $dbh->exec("ALTER TABLE videosource ADD COLUMN modified CHAR(20) DEFAULT NULL
        COMMENT 'Track the last time this videosource was updated.'");

        /*
         * If they didn't have 'modified', assume they need everything.
         */

        print "Creating remaining tables.\n";

        $stmt = $dbh->exec("DROP TABLE IF EXISTS SDprogramCache,SDcredits,SDheadendCache,SDpeople,SDprogramgenres,
    SDprogramrating,SDschedule,SDMessages");

        $stmt = $dbh->exec("CREATE TABLE `SDMessages` (
`row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `id` char(22) DEFAULT NULL COMMENT 'Required to ACK a message from the server.',
  `date` char(20) DEFAULT NULL,
  `message` varchar(512) DEFAULT NULL,
  `type` char(1) DEFAULT NULL COMMENT 'Message type. G-global, S-service status, U-user specific',
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`row`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("CREATE TABLE `SDcredits` (
`personID` mediumint(8) unsigned NOT NULL DEFAULT '0',
  `programID` varchar(64) NOT NULL,
  `role` varchar(100) DEFAULT NULL,
  KEY `personID` (`personID`),
  KEY `programID` (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("CREATE TABLE `SDheadendCache` (
`row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `lineup` varchar(50) NOT NULL DEFAULT '',
  `md5` char(22) NOT NULL,
  `modified` char(20) DEFAULT '1970-01-01T00:00:00Z',
  `json` mediumtext,
  PRIMARY KEY (`row`),
  UNIQUE KEY `lineup` (`lineup`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("CREATE TABLE `SDpeople` (
`personID` mediumint(8) unsigned NOT NULL,
  `name` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',
  PRIMARY KEY (`personID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("CREATE TABLE `SDprogramCache` (
`row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `programID` varchar(64) NOT NULL,
  `md5` char(22) NOT NULL,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `json` varchar(16384) NOT NULL,
  PRIMARY KEY (`row`),
  UNIQUE KEY `pid` (`programID`),
  KEY `programID` (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("CREATE TABLE `SDprogramgenres` (
`programID` varchar(64) NOT NULL,
  `relevance` char(1) NOT NULL DEFAULT '0',
  `genre` varchar(30) NOT NULL,
  PRIMARY KEY (`programID`),
  UNIQUE KEY `pid_relevance` (`programID`,`relevance`),
  KEY `genre` (`genre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("CREATE TABLE `SDprogramrating` (
`programID` varchar(64) NOT NULL,
  `system` varchar(30) NOT NULL,
  `rating` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("CREATE TABLE `SDschedule` (
`stationID` varchar(12) NOT NULL,
  `programID` varchar(64) NOT NULL,
  `md5` char(22) NOT NULL,
  `air_datetime` char(20) NOT NULL,
  `duration` mediumint(8) unsigned DEFAULT '0' COMMENT 'Duration (in seconds) of the program.',
  `airdate` year(4) NOT NULL DEFAULT '0000',
  `previouslyshown` tinyint(1) DEFAULT '0',
  `closecaptioned` tinyint(1) NOT NULL DEFAULT '0',
  `partnumber` tinyint(3) unsigned DEFAULT '0',
  `parttotal` tinyint(3) unsigned DEFAULT '0',
  `listingsource` int(11) NOT NULL DEFAULT '0',
  `first` tinyint(1) NOT NULL DEFAULT '0',
  `last` tinyint(1) NOT NULL DEFAULT '0',
  `dvs` tinyint(1) DEFAULT '0' COMMENT 'Descriptive Video Service',
  `new` tinyint(1) DEFAULT '0' COMMENT 'New',
  `educational` tinyint(1) NOT NULL DEFAULT '0'
  COMMENT 'Identifies broadcaster-designated Educational/Instructional programming.',
  `hdtv` tinyint(1) NOT NULL DEFAULT '0',
  `3d` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates program is in 3-D.',
  `letterbox` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates program is a letterbox version.',
  `stereo` tinyint(1) DEFAULT '0',
  `dolby` varchar(5) DEFAULT NULL,
  `dubbed` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates the program is dubbed.',
  `dubLanguage` varchar(40) DEFAULT NULL,
  `subtitled` tinyint(1) NOT NULL DEFAULT '0'
  COMMENT 'Indicates if the audio is in a foreign language, the English translation appears on-screen.',
  `subtitleLanguage` varchar(40) DEFAULT NULL,
  `sap` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates the availability of Secondary Audio Programming.',
  `sapLanguage` varchar(40) DEFAULT NULL,
  `programLanguage` varchar(40) DEFAULT NULL,
  `tvRatingSystem` varchar(128) DEFAULT NULL,
  `tvRating` varchar(7) DEFAULT NULL,
  `dialogRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor D rating',
  `languageRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor L rating',
  `sexualContentRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor S rating',
  `violenceRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor V rating',
  `fvRating` tinyint(1) DEFAULT '0' COMMENT 'Indicates fantasy violence.',
  UNIQUE KEY `stationid_airdatetime` (`stationID`,`air_datetime`),
  KEY `previouslyshown` (`previouslyshown`),
  KEY `programid` (`programID`),
  KEY `md5` (`md5`),
  KEY `sid` (`stationID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");
    }

    $stmt = $dbh->exec("ALTER TABLE credits CHANGE role role SET('actor','director','producer','executive_producer',
    'writer','guest_star','host','adapter','presenter','commentator','guest','musical_guest','judge',
    'correspondent','contestant')");

    $stmt = $dbh->exec("INSERT IGNORE INTO settings(value, data, hostname)
    VALUES('mythfilldatabaseLastRunStart', '',NULL),
    ('mythfilldatabaseLastRunEnd','',NULL),
    ('mythfilldatabaseLastRunStatus','',NULL),
    ('MythFillSuggestedRunTime','',NULL),
    ('DataDirectMessage','',NULL),
    ('SchedulesDirectLastUpdate','',NULL)");
}

?>
