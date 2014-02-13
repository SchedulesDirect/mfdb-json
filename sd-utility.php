#!/usr/bin/php

<?php
/*
 * This file is a utility program which performs the necessary setup for using the Schedules Direct API and the
 * native MythTV tables.
 * Robert Kulagowski
 * grabber@schedulesdirect.org
 *
 */
$isBeta = TRUE;
$debug = TRUE;
$done = FALSE;
$schedulesDirectHeadends = array();
$sdStatus = "";
$username = "";
$password = "";
$passwordHash = "";
$scriptVersion = "0.04";
$scriptDate = "2014-02-04";

require_once 'vendor/autoload.php';
use Guzzle\Http\Client;

$agentString = "sd-utility.php utility program v$scriptVersion/$scriptDate";

$updatedHeadendsToRefresh = array();
$needToStoreUserPassword = FALSE;

date_default_timezone_set("UTC");
$date = new DateTime();
$todayDate = $date->format("Y-m-d");

$dbUser = "mythtv";
$dbPassword = "mythtv";
$host = "localhost";
$db = "mythconverg";

$helpText = <<< eol
The following options are available:
--beta
--help\t\t(this text)
--host=\t\tMySQL database hostname
--dbuser=\tUsername to access database
--dbpassword=\tPassword to access database.
--username=\tSchedules Direct username.
--password=\tSchedules Direct password.
eol;

$longoptions = array("beta::", "debug::", "help::", "host::", "dbpassword::", "dbuser::", "username::",
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
            print "$agentString\n\n";
            print "$helpText\n";
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
    }
}

print "$agentString\n";
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
    $baseurl = "http://54.84.90.174/20131021/";
    print "Using beta server.\n";
    # API must match server version.
    $api = 20131021;
}
else
{
    $baseurl = "https://data2.schedulesdirect.org";
    print "Using production server.\n";
    $api = 20130709;
}

$client = new Guzzle\Http\Client($baseurl);
$client->setUserAgent($agentString);

if ($username == "" AND $password == "")
{
    $stmt = $dbh->prepare("SELECT userid,password FROM videosource WHERE xmltvgrabber='schedulesdirect2' LIMIT 1");
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
    }
}

print "Logging into Schedules Direct.\n";
$token = getToken($username, $passwordHash);

if ($token == "ERROR")
{
    exit;
}
elseif ($needToStoreUserPassword)
{
    $stmt = $dbh->prepare("UPDATE videosource SET userid=:user,password=:password WHERE password IS NULL");
    $stmt->execute(array("user" => $username, "password" => $password));
}

while (!$done)
{
    getStatus();

    printStatus();

    displayLocalVideoSources();

    print "\nSchedules Direct functions:\n";
    print "1 Add a headend to account at Schedules Direct\n";
    print "2 Delete a headend from account at Schedules Direct\n";
    print "3 Acknowledge a message\n";
    print "4 Print a channel lineup for a headend\n";

    print "\nMythTV functions:\n";
    print "A to Add a new videosource to MythTV\n";
    print "D to Delete a videosource in MythTV\n";
    print "L to Link a videosource to a headend at SD\n";
    print "R to refresh a videosource with new lineup information\n";
    print "Q to Quit\n";

    $response = strtoupper(readline(">"));

    switch ($response)
    {
        case "1":
            addHeadendsToSchedulesDirect();
            break;
        case "2":
            deleteHeadendFromSchedulesDirect();
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
                $stmt->execute(array("name"     => $newName, "userid" => $username,
                                     "password" => $password));
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
            print "Linking Schedules Direct headend to sourceid\n\n";
            linkSchedulesDirectHeadend();
            break;
        case "R":
            refreshLineup();
            break;
        case "Q":
        default:
            $done = TRUE;
            break;
    }
}

exit;

function refreshLineup()
{
    $he = readline("lineupid:>");
    print "Headend update for $he\n";
    /*
     * For now we're just going to grab everything; selecting which channels to update is better left for
     * other applications, like MythWeb or something like that.
     */

    $sourceID = readline("Apply to sourceid:>");
    if ($sourceID != "")
    {
        updateChannelTable($he, $sourceID);
    }
}

function updateChannelTable($he, $sourceID)
{
    global $dbh;

    $stmt = $dbh->prepare("SELECT lineupid FROM videosource WHERE sourceid=:sourceid");
    $stmt->execute(array("sourceid" => $sourceID));
    $lineupid = $stmt->fetchColumn();

    list($h, $dev) = explode(":", $lineupid);

    $stmt = $dbh->prepare("SELECT json FROM SDheadendCache WHERE headend=:he");
    $stmt->execute(array("he" => $he));
    $json = json_decode($stmt->fetchColumn(), TRUE);

    print "Updating channel table for sourceid:$sourceID\n";

    if (substr($h, 0, 2) == "PC")
    {
        /*
         * For antenna lineups, we're not going to delete the existing channel table or dtv_multiplex; we're still
         * going to use the scan, but use the atsc major and minor to correlate what we've scanned with what's in the
         *  lineup file.
         */
        $transport = "Antenna";
        $dev = "Antenna";
    }
    else
    {
        $transport = "Cable";
        $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sourceid");
        $stmt->execute(array("sourceid" => $sourceID));
    }

    if ($dev == "Q")
    {
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
    }

    $dtvMultiplex = array();

    $channelInsertQAM =
        $dbh->prepare("UPDATE channel SET tvformat='ATSC',visible='1',mplexid=:mplexid,serviceid=:qamprogram
        WHERE xmltvid=:stationID");
    $insertDTVMultiplex = $dbh->prepare
        ("INSERT INTO dtv_multiplex
        (sourceid,frequency,symbolrate,polarity,modulation,visible,constellation,hierarchy,mod_sys,rolloff,sistandard)
        VALUES
        (:sourceid,:freq,0,'v','qam_256',1,'qam_256','a','UNDEFINED','0.35','atsc')");

    $updateChannelTableATSC = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=:atscMajor AND atsc_minor_chan=:atscMinor");

    $updateChannelTableAnalog = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=0 AND atsc_minor_chan=0 AND freqID=:freqID");


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
            /*
                        $stmt = $dbh->prepare(
                            "INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid,atsc_major_chan,atsc_minor_chan)
                            VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid,:atsc_major_chan,:atsc_minor_chan)");
                        $stmt->execute(array("chanid"          => (int)($sourceID * 1000) + (int)$freqid, "channum" => $freqid,
                                             "freqid"          => $freqid, "sourceid" => $sourceID, "xmltvid" => $stationID,
                                             "atsc_major_chan" => $atscMajor, "atsc_minor_chan" => $atscMinor));
            */
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

            $stmt->execute(array("chanid" => (int)($sourceID * 1000) + (int)$channum, "channum" => ltrim($channum, "0"),
                                 "freqid" => $channum, "sourceid" => $sourceID, "xmltvid" => $stationID));
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

            $stmt->execute(array("chanid" => (int)($sourceID * 1000) + (int)$channel, "channum" => $channel,
                                 "freqid" => $channel, "sourceid" => $sourceID, "xmltvid" => $stationID));

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

    /*
     * Set the startchan to a non-bogus value.
     */

    $stmt = $dbh->prepare("SELECT channum FROM channel WHERE sourceid=:sourceid
    ORDER BY CAST(channum AS SIGNED) LIMIT 1");
    $stmt->execute(array("sourceid" => $sourceID));
    $result = $stmt->fetchAll(PDO::FETCH_COLUMN);
    if (count($result))
    {
        $startChan = $result[0];
        $setStartChannel = $dbh->prepare("UPDATE cardinput SET startchan=:startChan WHERE sourceid=:sourceid");
        $setStartChannel->execute(array("sourceid" => $sourceID, "startChan" => $startChan));
    }
}

function linkSchedulesDirectHeadend()
{
    global $dbh;

    $sid = readline("sourceid:>");
    $he = strtoupper(readline("lineupid:>"));

    $stmt = $dbh->prepare("SELECT json FROM SDheadendCache WHERE headend=:he");
    $stmt->execute(array("he" => $he));
    $response = json_decode($stmt->fetchColumn(), TRUE);

    if (!count($response))
    {
        return;
    }

    if (count($response["deviceTypes"]) > 1)
    {
        print "Your headend has these devicetypes: ";
        foreach ($response["deviceTypes"] as $v)
        {
            print "$v ";
        }
        print "\n";

        $deviceToLink = strtoupper(readline("Which device to link:>"));
    }
    else
    {
        print "Your headend has only one devicetype: ";
        print $response["deviceTypes"][0] . "\n";
        $deviceToLink = $response["deviceTypes"][0];
    }

    if ($deviceToLink == "Antenna")
    {
        $compositeDevice = $he;
    }
    else
    {
        $compositeDevice = "$he:$deviceToLink";
    }

    $stmt = $dbh->prepare("UPDATE videosource SET lineupid=:he WHERE sourceid=:sid");
    $stmt->execute(array("he" => $compositeDevice, "sid" => $sid));

}

function printLineup()
{
    global $dbh;

    /*
     * First we want to get the headend that we're interested in.
     */

    $he = strtoupper(readline("Headend:>"));
    $stmt = $dbh->prepare("SELECT json FROM SDheadendCache WHERE headend=:he");
    $stmt->execute(array("he" => $he));
    $response = json_decode($stmt->fetchColumn(), TRUE);

    if (!count($response))
    {
        return;
    }

    if (count($response["deviceTypes"]) > 1)
    {
        print "Your headend has these devicetypes: ";
        foreach ($response["deviceTypes"] as $v)
        {
            print "$v ";
        }
        print "\n";

        $toPrint = strtoupper(readline("Which device to display:>"));
    }
    else
    {
        print "Your headend has only one devicetype: ";
        print $response["deviceTypes"][0] . "\n";
        $toPrint = $response["deviceTypes"][0];
    }

    print "\n";

    $chanMap = array();
    $stationMap = array();

    if ($toPrint != "Antenna")
    {
        foreach ($response[$toPrint]["map"] as $v)
        {
            $chanMap[$v["stationID"]] = $v["channel"];
        }
    }
    else
    {
        foreach ($response["Antenna"]["map"] as $v)
        {
            if (isset($v["atscMajor"]))
            {
                $chanMap[$v["stationID"]] = $v["atscMajor"] . "." . $v["atscMinor"];
            }
            else
            {
                $chanMap[$v["stationID"]] = $v["uhfVhf"];
            }
        }
    }

    foreach ($response["stations"] as $v)
    {
        $stationMap[$v["stationID"]] = $v["callsign"] . " (" . $v["affiliate"] . ")";
    }

    asort($chanMap, SORT_NATURAL);

    print "Channel\tCallsign\tStationID\n";
    foreach ($chanMap as $stationID => $channel)
    {
        print "$channel\t" . $stationMap[$stationID] . "\t$stationID\n";
    }
}

function addHeadendsToSchedulesDirect()
{
    global $token;
    global $client;

    print "Token is $token\n";

    print "Three-character ISO-3166-1 alpha3 country code:";
    $country = strtoupper(readline(">"));

    print "Enter postal code:";
    $postalcode = strtoupper(readline(">"));

    $request = $client->get("headends", array(), array(
        "query"   => array("country" => $country, "postalcode" => $postalcode),
        "headers" => array("token" => $token)));

    $response = $request->send();
    $headends = $response->json();

    if (isset($headends["code"]))
    {
        print "Error!\n";
        print "code:" . $headends["code"] . " response:" . $headends["response"] . " message:" . $headends["message"] . "\n";

        return;
    }

    foreach ($headends as $he => $details)
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
    $he = readline("Lineup to add>");
    if ($he == "")
    {
        return;
    }

    if (substr_count($he, "-") != 2)
    {
        print "Did not see two hyphens in headend; did you enter it correctly?\n";

        return;
    }
/*
    $request = $client->put("lineups/$he", array(), array("headers" => array("token" => $token)));
    $response = $request->send();
    $res = $response->json();
*/

    $request = $client->put("lineups/$he", array(), array(
        'headers' => array('token' => $token)
    ));




    $request = $client->put("lineups/$he", array(), array(
        "headers" => array("token" => $token)));



    $response = $request->send();
    $sdStatus = $response->json();





    var_dump($res);
    $tt = fgets(STDIN);
}

function deleteHeadendFromSchedulesDirect()
{
    global $token;
    global $api;

    $toDelete = readline("Headend to Delete:>");

    $res = array();
    $res["action"] = "delete";
    $res["object"] = "headends";
    $res["randhash"] = $token;
    $res["api"] = $api;
    $res["request"] = $toDelete;

    $res = json_decode(sendRequest(json_encode($res)), true);

    if ($res["code"] == 0)
    {
        print "Successfully deleted headend.\n";
    }
    else
    {
        print "ERROR:Received error response from server:\n";
        print$res["message"] . "\n\n-----\n";
        print "Press ENTER to continue.\n";
        $a = fgets(STDIN);
    }
}

function deleteMessageFromSchedulesDirect()
{
    global $token;
    global $api;

    $toDelete = readline("MessageID to acknowledge:>");

    $res = array();
    $res["action"] = "delete";
    $res["object"] = "message";
    $res["randhash"] = $token;
    $res["api"] = $api;
    $res["request"] = $toDelete;

    $res = json_decode(sendRequest(json_encode($res)), true);

    if ($res["code"] == 0)
    {
        print "Successfully deleted message.\n";
    }
    else
    {
        print "ERROR:Received error response from server:\n";
        print$res["message"] . "\n\n-----\n";
        print "Press ENTER to continue.\n";
        $a = fgets(STDIN);
    }
}

function getLineup($heToGet)
{
    global $client;
    global $token;

    print "Retrieving lineup from Schedules Direct.\n";
    $request = $client->get("lineups/$heToGet", array(), array(
        "headers" => array("token" => $token)));

    $response = $request->send();
    $lineup = $response->json();

    return $lineup;
}

function getStatus()
{
    global $token;
    global $client;
    global $sdStatus;

    $request = $client->get("status", array(), array(
        "headers" => array("token" => $token)));

    $response = $request->send();
    $sdStatus = $response->json();
}

function printStatus()
{
    global $dbh;
    global $sdStatus;
    global $updatedHeadendsToRefresh;

    print "\nStatus messages from Schedules Direct:\n";

    if ($sdStatus["code"] == 0)
    {
        $expires = $sdStatus["account"]["expires"];
        $maxHeadends = $sdStatus["account"]["maxLineups"];
        $nextConnectTime = $sdStatus["account"]["nextSuggestedConnectTime"];

        foreach ($sdStatus["account"]["messages"] as $a)
        {
            print "MessageID: " . $a["msgID"] . " : " . $a["date"] . " : " . $a["message"] . "\n";
        }
    }
    else
    {
        print "Received error response from server!\n";
        print "ServerID: " . $sdStatus["serverID"] . "\n";
        print "Message: " . $sdStatus["message"] . "\n";
        print "\nFATAL ERROR. Terminating execution.\n";
        exit;
    }

    print "Server: " . $sdStatus["serverID"] . "\n";
    print "Last data refresh: " . $sdStatus["lastDataUpdate"] . "\n";
    print "Account expires: $expires\n";
    print "Max number of headends for your account: $maxHeadends\n";
    print "Next suggested connect time: $nextConnectTime\n";

    $getLocalModified = $dbh->prepare("SELECT modified FROM SDheadendCache WHERE headend=:he");
    print "The following lineups are in your account at Schedules Direct:\n\n";

    $he = getSchedulesDirectHeadends();

    if (count($he))
    {
        foreach ($he as $id => $modified)
        {
            $line = "$id\t Last Updated: $modified";

            $getLocalModified->execute(array("he" => $id));
            $sdStatusult = $getLocalModified->fetchAll(PDO::FETCH_COLUMN);

            if ((count($sdStatusult) == 0) OR ($sdStatusult[0] < $modified))
            {
                $updatedHeadendsToRefresh[$id] = $modified;
                $line .= " (*** Update Available ***)";
            }

            print "$line\n";
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

    foreach ($updatedHeadendsToRefresh as $k => $v)
    {
        $res = array();
        $res = getLineup($k);

        if ($res["code"] != 0)
        {
            print "\n\n-----\nERROR: Bad response from Schedules Direct.\n";
            print $res["message"] . "\n\n-----\n";
            exit;
        }

        /*
         * Store a copy of the data that we just downloaded into the cache.
         */
        $stmt = $dbh->prepare("REPLACE INTO SDheadendCache(headend,json,modified) VALUES(:he,:json,:modified)");

        $stmt->execute(array("he"   => $k, "modified" => $updatedHeadendsToRefresh[$k],
                             "json" => json_encode($res)));
    }
}

function getToken($username, $passwordHash)
{
    global $client;

    $body = json_encode(array("username" => $username, "password" => $passwordHash));

    $request = $client->post("token", array(), $body);
    $response = $request->send();

    $res = array();
    $res = $response->json();

    if (json_last_error() != 0)
    {
        print "JSON decode error:\n";
        var_dump($response);
        exit;
    }

    if ($res["code"] == 0)
    {
        return $res["token"];
    }

    print "Response from schedulesdirect: $response\n";

    return "ERROR";
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

    $stmt = $dbh->prepare("SELECT sourceid,name,lineupid FROM videosource");
    $stmt->execute();
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    if (count($result))
    {
        print "\nExisting sources in MythTV:\n";
        foreach ($result as $v)
        {
            print "sourceid: " . $v["sourceid"] . "\tname: " . $v["name"] . "\tlineupid: " . $v["lineupid"] . "\n";
        }
    }
    else
    {
        print "\nWARNING: *** No videosources configured in MythTV. ***\n";
    }
}

function getSchedulesDirectHeadends()
{
    global $sdStatus;
    $schedulesDirectHeadends = array();

    foreach ($sdStatus["lineups"] as $hv)
    {
        $schedulesDirectHeadends[$hv["ID"]] = $hv["modified"];
    }

    return ($schedulesDirectHeadends);
}

?>