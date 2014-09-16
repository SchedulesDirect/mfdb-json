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
$debug = FALSE;
$done = FALSE;
$isMythTV = TRUE;
$skipChannelLogo = FALSE;
$schedulesDirectLineups = array();
$sdStatus = "";
$username = "";
$usernameFromDB = "";
$password = "";
$passwordFromDB = "";
$passwordHash = "";
$dbWithoutMythtv = FALSE;
$useServiceAPI = FALSE;
$channelLogoDirectory = "/home/mythtv/.mythtv/channels";
$lineupArray = array();
$force = FALSE;

$availableCountries = array(
    "North America" => array(
        "United States" => "USA",
        "Canada"        => "CAN",
        "Mexico"        => "MEX"),
    "Europe"        => array(
        "Denmark"       => "DNK",
        "Finland"       => "FIN",
        "Great Britain" => "GBR",
        "Sweden"        => "SWE"),
    "Latin America" => array(
        "Argentina"  => "ARG",
        "Belize"     => "BLZ",
        "Brazil"     => "BRA",
        "Chile"      => "CHL",
        "Columbia"   => "COL",
        "Costa Rica" => "CRI",
        "Ecuador"    => "ECU",
        "Guatemala"  => "GTM",
        "Guyana"     => "GUY",
        "Honduras"   => "HND",
        "Panama"     => "PAN",
        "Peru"       => "PER",
        "Uruguay"    => "URY",
        "Venezuela"  => "VEN"),
    "Caribbean"     => array(
        "Anguila"                      => "AIA",
        "Antigua/Barbuda"              => "ATG",
        "Aruba"                        => "ABW",
        "Bahamas"                      => "BHS",
        "Barbados"                     => "BRB",
        "Bermuda"                      => "BMU",
        "Bonaire, Saba, St. Eustatius" => "BES",
        "British Virgin Islands"       => "VGB",
        "Cayman Islands"               => "CYM",
        "Curacao"                      => "CUW",
        "Dominica"                     => "DMA",
        "Dominican Republic"           => "DOM",
        "Grenada"                      => "GRD",
        "Jamaica"                      => "JAM",
        "Puerto Rico"                  => "PRI",
        "Saint Martin"                 => "MAF",
        "Saint Vincent / Grenadines"   => "VCT",
        "St. Kitts and Nevis"          => "KNA",
        "St. Lucia"                    => "LCA",
        "Trinidad and Tobago"          => "TTO",
        "Turks and Caicos"             => "TCA"));

require_once "vendor/autoload.php";
require_once "functions.php";
use Guzzle\Http\Client;

if ($isBeta)
{
    # Test server. Things may be broken there.
    $baseurl = "http://ec2-54-86-226-234.compute-1.amazonaws.com/20140530/";
    print "Using beta server.\n";
    # API must match server version.
    $api = 20140530;
}
else
{
    $baseurl = "https://json.schedulesdirect.org/20131021/";
    print "Using production server.\n";
    $api = 20131021;
}

$agentString = "sd-utility.php utility program API:$api v$scriptVersion/$scriptDate";

$updatedLineupsToRefresh = array();
$needToStoreLogin = FALSE;

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
--countries\tThe list of countries that have data.
--debug\t\tEnable debugging. (Default: FALSE)
--dbname=\tMySQL database name. (Default: $dbName)
--dbuser=\tUsername for database access. (Default: $dbUser)
--dbpassword=\tPassword for database access. (Default: $dbPassword)
--dbhost=\tMySQL database hostname. (Default: $dbHost)
--help\t\t(this text)
--host=\t\tIP address of the MythTV backend. (Default: $host)
--logo=\t\tDirectory where channel logos are stored (Default: $channelLogoDirectory)
--nomyth\tDon't execute any MythTV specific functions. (Default: FALSE)
--skiplogo\tDon't download channel logos.
--username=\tSchedules Direct username.
--password=\tSchedules Direct password.
--usedb\t\tUse a database to store data, even if you're not running MythTV. (Default: FALSE)
--timezone=\tSet the timezone for log file timestamps. See http://www.php.net/manual/en/timezones.php (Default:$tz)
--version\tPrint version information and exit.
eol;

$longoptions = array("countries", "debug", "force", "help", "host::", "dbname::", "dbuser::", "dbpassword::",
                     "dbhost::", "logo::", "nomyth", "skiplogo", "username::", "password::", "timezone::", "usedb",
                     "version");

$options = getopt("h::", $longoptions);
foreach ($options as $k => $v)
{
    switch ($k)
    {
        case "countries":
            printListOfAvailableCountries();
            exit;
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
        case "force":
            $force = TRUE;
            break;
        case "host":
            $host = $v;
            break;
        case "logo":
            $channelLogoDirectory = $v;
            break;
        case "nomyth":
            $isMythTV = FALSE;
            break;
        case "skiplogo":
            $skipChannelLogo = TRUE;
            break;
        case "username":
            $username = $v;
            break;
        case "password":
            $password = $v;
            $passwordHash = sha1($v);
            break;
        case "timezone":
            date_default_timezone_set($v);
            break;
        case "usedb":
            $dbWithoutMythtv = TRUE;
            break;
        case "version":
            print "$agentString\n\n";
            exit;
            break;
    }
}

if ($knownToBeBroken AND !$force)
{
    print "This version is known to be broken and --force not specified. Exiting.\n";
    exit;
}

print "Using timezone $tz\n";
print "$agentString\n";

if ($isMythTV OR $dbWithoutMythtv)
{
    print "Attempting to connect to database.\n";
    try
    {
        $dbh = new PDO("mysql:host=$dbHost;dbname=$dbName;charset=utf8", $dbUser, $dbPassword,
            array(PDO::ATTR_PERSISTENT => true));
        $dbh->exec("SET CHARACTER SET utf8");
        $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    } catch (PDOException $e)
    {
        if ($e->getCode() == 2002)
        {
            print "Could not connect to database:\n" . $e->getMessage() . "\n";
            print "If you're running the grabber as standalone, use --nomyth\n";
            exit;
        }
    }

    checkDatabase();
}

if ($skipChannelLogo === FALSE)
{
    if (!file_exists($channelLogoDirectory))
    {
        $result = @mkdir($channelLogoDirectory);

        if ($result === FALSE)
        {
            print "Could not create $channelLogoDirectory\n";
            print "Use --logo to specify directory, or --skiplogo to bypass channel logos.\n";
            exit;
        }
    }
}

$client = new Guzzle\Http\Client($baseurl);
$client->setUserAgent($agentString);

if ($isMythTV)
{
    $useServiceAPI = checkForServiceAPI();

    $userLoginInformation = setting("SchedulesDirectLogin");

    if ($userLoginInformation !== FALSE)
    {
        $responseJSON = json_decode($userLoginInformation, TRUE);
        $usernameFromDB = $responseJSON["username"];
        $passwordFromDB = $responseJSON["password"];
    }
}
else
{
    if (file_exists("sd.json.conf"))
    {
        $userLoginInformation = file("sd.json.conf");
        $responseJSON = json_decode($userLoginInformation[0], TRUE);
        $usernameFromDB = $responseJSON["username"];
        $passwordFromDB = $responseJSON["password"];
    }
}

if ($username == "")
{
    if ($usernameFromDB == "")
    {
        $username = readline("Schedules Direct username:");
        $needToStoreLogin = TRUE;
    }
    else
    {
        $username = $usernameFromDB;
    }
}
else
{
    $needToStoreLogin = TRUE;
}

if ($password == "")
{
    if ($passwordFromDB == "")
    {
        $password = readline("Schedules Direct password:");
        $passwordHash = sha1($password);
        $needToStoreLogin = TRUE;
    }
    else
    {
        $password = $passwordFromDB;
        $passwordHash = sha1($password);
    }
}
else
{
    $passwordHash = sha1($password);
    $needToStoreLogin = TRUE;
}

print "Logging into Schedules Direct.\n";
$token = getToken($username, $passwordHash);

if ($token == "ERROR")
{
    printMSG("Got error when attempting to retrieve token from Schedules Direct.");
    printMSG("Check if you entered username/password incorrectly.");
    exit;
}

if ($needToStoreLogin)
{
    $userInformation["username"] = $username;
    $userInformation["password"] = $password;

    $credentials = json_encode($userInformation);

    if ($isMythTV)
    {
        putSchedulesDirectLoginIntoDB($credentials);

        $stmt = $dbh->prepare("UPDATE videosource SET userid=:username,
    password=:password WHERE xmltvgrabber='schedulesdirect2'");
        $stmt->execute(array("username" => $username, "password" => $password));
    }
    else
    {
        $fh = fopen("sd.json.conf", "w");
        fwrite($fh, "$credentials\n");
        fclose($fh);
    }
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

    if ($isMythTV)
    {
        displayLocalVideoSources();
    }

    print "\nSchedules Direct functions:\n";
    print "1 Add a known lineupID to your account at Schedules Direct\n";
    print "2 Search for a lineup to add to your account at Schedules Direct\n";
    print "3 Delete a lineup from your account at Schedules Direct\n";
    print "4 Acknowledge a message\n";
    print "5 Print a channel table for a lineup\n";

    if ($isMythTV)
    {
        print "\nMythTV functions:\n";
        print "A to Add a new videosource to MythTV\n";
        print "D to Delete a videosource in MythTV\n";
        print "E to Extract Antenna / QAM / DVB scan from MythTV to send to Schedules Direct\n";
        print "L to Link a videosource to a lineup at Schedules Direct\n";
        print "U to update a videosource by downloading from Schedules Direct\n";
    }

    print "Q to Quit\n";

    $response = strtoupper(readline(">"));

    switch ($response)
    {
        case "1":
            $lineup = readline("Lineup to add>");
            directAddLineup($lineup);
            break;
        case "2":
            addLineupsToSchedulesDirect();
            break;
        case "3":
            deleteLineupFromSchedulesDirect();
            break;
        case "4":
            deleteMessageFromSchedulesDirect();
            break;
        case "5":
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
        case "E":
            $sourceIDtoExtract = readline("Which sourceid do you want to extract:>");
            if ($sourceIDtoExtract != "")
            {
                extractData($sourceIDtoExtract);
            }
            break;
        case "L":
            print "Linking Schedules Direct lineup to sourceid\n\n";
            linkSchedulesDirectLineup();
            break;
        case "U":
            $lineup = getLineupFromNumber(strtoupper(readline("Schedules Direct lineup (# or lineup):>")));
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
    global $skipChannelLogo;

    $transport = "";

    $stmt = $dbh->prepare("SELECT sourceid FROM videosource WHERE lineupid=:lineup");
    $stmt->execute(array("lineup" => $lineup));
    $sID = $stmt->fetchAll(PDO::FETCH_COLUMN);

    if (count($sID) == 0)
    {
        print "ERROR: Can't update channel table; lineup not associated with a videosource.\n";

        return;
    }

    $stmt = $dbh->prepare("SELECT json FROM SDlineupCache WHERE lineup=:lineup");
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
            $updateChannelTableATSC = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=:atscMajor AND atsc_minor_chan=:atscMinor");

            $updateChannelTableAnalog = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=0 AND atsc_minor_chan=0 AND freqID=:freqID");
        }

        if ($json["metadata"]["transport"] == "Cable")
        {
            $transport = "Cable";
            $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sourceid");
            $stmt->execute(array("sourceid" => $sourceID));
        }

        if ($json["metadata"]["transport"] == "QAM")
        {
            $transport = "QAM";
        }

        if ($transport != "QAM")
        {
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
            }
        }
        else
        {
            /*
             * It's QAM. We're going to run a match based on what the user had already scanned to avoid the manual
             * matching step of correlating stationIDs.
             */

            print "You can:\n";
            print "1. Exit this program and run a QAM scan using mythtv-setup then use the QAM lineup to populate stationIDs.\n";
            print "2. Use the QAM lineup information directly. (Default)\n";
            $useScan = readline("Which do you want to do? (1 or 2)>");
            if ($useScan == "" OR $useScan == "2")
            {
                $useScan = FALSE;
            }
            else
            {
                $useScan = TRUE;
            }

            if (count($json["qamMappings"]) > 1)
            {
                /*
                 * TODO: Work on this some more. Kludgey.
                 */
                print "Found more than one QAM mapping for your headend.\n";
                foreach ($json["qamMappings"] as $m)
                {
                    print "Mapping: $m\n";
                }
                $mapToUse = readline("Which map do you want to use>");
            }
            else
            {
                $mapToUse = "1";
            }

            if ($useScan)
            {
                $stmt = $dbh->prepare("SELECT mplexid, frequency FROM dtv_multiplex WHERE modulation='qam_256'");
                $stmt->execute();
                $qamFrequencies = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

                $stmt = $dbh->prepare("SELECT * FROM channel WHERE sourceid=:sid");
                $stmt->execute(array("sid" => $sourceID));
                $existingChannelNumbers = $stmt->fetchAll(PDO::FETCH_ASSOC);

                $updateChannelTableQAM = $dbh->prepare("UPDATE channel SET xmltvid=:stationID WHERE
                mplexid=:mplexid AND serviceid=:serviceid");

                $map = array();

                foreach ($json["map"][$mapToUse] as $foo)
                {
                    $map["{$foo["qamFrequency"]}-{$foo["qamProgram"]}"] = $foo["stationID"];
                }

                foreach ($existingChannelNumbers as $foo)
                {
                    $toFind = "{$qamFrequencies[$foo["mplexid"]]}-{$foo["serviceid"]}";

                    if (array_key_exists($toFind, $map))
                    {
                        $updateChannelTableQAM->execute(array("stationID" => $map[$toFind],
                                                              "mplexid"   => $foo["mplexid"],
                                                              "serviceid" => $foo["serviceid"]));
                    }
                }

                print "Done updating QAM scan with stationIDs.\n";
            }
            else
            {
                /*
                 * The user has chosen to not run a QAM scan and just use the values that we're supplying.
                 * Work-in-progress.
                 */

                print "Inserting QAM data into tables.\n";

                $insertDTVMultiplex = $dbh->prepare
                    ("INSERT INTO dtv_multiplex
        (sourceid,frequency,symbolrate,polarity,modulation,visible,constellation,hierarchy,mod_sys,rolloff,sistandard)
        VALUES
        (:sourceid,:freq,0,'v','qam_256',1,'qam_256','a','UNDEFINED','0.35','atsc')");


                $channelInsert = $dbh->prepare("INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid,tvformat,visible,mplexid,serviceid)
                     VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid,'ATSC','1',:mplexid,:serviceid)");

                $stmt = $dbh->prepare("SELECT mplexid, frequency FROM dtv_multiplex WHERE modulation='qam_256'
                AND sourceid=:sourceid");
                $stmt->execute(array("sourceid" => $sourceID));
                $dtvMultiplex = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

                foreach ($json["map"][$mapToUse] as $mapArray)
                {
                    /*
                     * Because multiple programs may end up on a single frequency, we only want to insert once, but we want
                     * to track the mplexid assigned when we do the insert, because that might be used more than once.
                     */
                    $stationID = $mapArray["stationID"];
                    $qamType = $mapArray["qamType"];
                    $qamProgram = $mapArray["qamProgram"];
                    $qamFreq = $mapArray["qamFrequency"];
                    $channel = $mapArray["channel"];

                    if (!isset($dtvMultiplex[$qamFreq]))
                    {
                        $insertDTVMultiplex->execute(array("sourceid" => $sourceID, "freq" => $qamFreq));
                        $dtvMultiplex[$qamFreq] = $dbh->lastInsertId();
                    }

                    if (isset($mapArray["virtualChannel"]))
                    {
                        $virtualChannel = $mapArray["virtualChannel"];
                    }
                    else
                    {
                        $virtualChannel = "";
                    }

                    /*
                     * In order to insert a unique channel ID, we need to make sure that "39_1" and "39_2" map to two
                     * different values. Old code resulted in 39_1 -> 39, then 39_2 had a collision because it also
                     * turned into "39"
                     */

                    $strippedChannel = str_replace(array("-", "_"), "", $channel);

                    try
                    {
                        $channelInsert->execute(array("chanid"    => (int)($sourceID * 1000) + (int)$strippedChannel,
                                                      "channum"   => $channel,
                                                      "freqid"    => $channel, "sourceid" => $sourceID,
                                                      "xmltvid"   => $stationID,
                                                      "mplexid"   => $dtvMultiplex[$qamFreq],
                                                      "serviceid" => $qamProgram));
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
                }
                print "Done inserting QAM tuning information directly into tables.\n";
            }
        }

        /*
         * Now that we have basic information in the database, we can start filling in other things, like
         * callsigns, etc.
         */

        $stmt = $dbh->prepare("UPDATE channel SET name=:name, callsign=:callsign WHERE xmltvid=:stationID");

        foreach ($json["stations"] as $stationArray)
        {
            $stationID = $stationArray["stationID"];
            $callsign = $stationArray["callsign"];

            if (array_key_exists("name", $stationArray))
            {
                /*
                 * Not all stations have names, so don't try to insert a name if that field isn't included.
                 */
                $name = $stationArray["name"];
            }
            else
            {
                $name = "";
            }

            if (array_key_exists("logo", $stationArray))
            {
                if ($skipChannelLogo === FALSE)
                {
                    checkForChannelIcon($stationID, $stationArray["logo"]);
                }
            }

            $stmt->execute(array("name" => $name, "callsign" => $callsign, "stationID" => $stationID));
        }

        $lineupLastModifiedJSON = setting("localLineupLastModified");
        $lineupLastModifiedArray = array();

        if (count($lineupLastModifiedJSON))
        {
            $lineupLastModifiedArray = json_decode($lineupLastModifiedJSON, TRUE);
        }

        $lineupLastModifiedArray[$lineup] = $modified;

        setting("localLineupLastModified", json_encode($lineupLastModifiedArray));

        /*
         * Set the startchan to a non-bogus value.
         */
        $stmt = $dbh->prepare("SELECT channum FROM channel WHERE sourceid=:sourceid
    ORDER BY CAST(channum AS SIGNED) LIMIT 1");

        foreach ($sID as $sourceID)
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
}

function linkSchedulesDirectLineup()
{
    global $dbh;

    $sid = readline("MythTV sourceid:>");

    if ($sid == "")
    {
        return;
    }

    $lineup = getLineupFromNumber(strtoupper(readline("Schedules Direct lineup (# or lineup):>")));

    if ($lineup == "")
    {
        return;
    }

    $stmt = $dbh->prepare("SELECT json FROM SDlineupCache WHERE lineup=:lineup");
    $stmt->execute(array("lineup" => $lineup));
    $response = json_decode($stmt->fetchColumn(), TRUE);

    if (!count($response))
    {
        return;
    }

    $stmt = $dbh->prepare("UPDATE videosource SET lineupid=:lineup WHERE sourceid=:sid");
    $stmt->execute(array("lineup" => $lineup, "sid" => $sid));
}

function printLineup()
{
    global $dbh;

    /*
     * First we want to get the lineup that we're interested in.
     */

    $lineup = getLineupFromNumber(strtoupper(readline("Lineup to print (# or lineup):>")));

    if ($lineup == "")
    {
        return;
    }

    $stmt = $dbh->prepare("SELECT json FROM SDlineupCache WHERE lineup=:lineup");
    $stmt->execute(array("lineup" => $lineup));
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
            elseif (array_key_exists("uhfVhf", $v))
            {
                $chanMap[$v["stationID"]] = $v["uhfVhf"];
            }
            else
            {
                $chanMap[$v["stationID"]] = 0; //Not sure what the correct thing to use here is at this time.
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

    $sdLineupArray = array();

    $arrayCountriesWithOnePostalCode = array(
        "BLZ" => "BZ",
        "COL" => "CO",
        "GUY" => "GY",
        "HND" => "HN",
        "PAN" => "PA",
        "AIA" => "AI-2640",
        "ATG" => "AG",
        "ABW" => "AW",
        "BHS" => "BS",
        "BES" => "BQ",
        "CUW" => "CW",
        "DMA" => "DM",
        "GRD" => "GD",
        "MAF" => "97150",
        "KNA" => "KN",
        "LCA" => "LC",
        "TTO" => "TT",
        "TCA" => "TKCA1ZZ");

    $done = FALSE;
    $country = "";

    while (!$done)
    {
        print "Three-character ISO-3166-1 alpha3 country code (? to list available countries):";
        $country = strtoupper(readline(">"));

        if ($country == "")
        {
            return;
        }

        if ($country == "?")
        {
            printListOfAvailableCountries();
        }
        else
        {
            $done = TRUE;
        }
    }

    if (array_key_exists($country, $arrayCountriesWithOnePostalCode))
    {
        print "Only one valid postal code for this country: {$arrayCountriesWithOnePostalCode[$country]}\n";
        $postalcode = $arrayCountriesWithOnePostalCode[$country];
    }
    else
    {
        print "Enter postal code:";
        $postalcode = strtoupper(readline(">"));

        if ($postalcode == "")
        {
            return;
        }
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

    $counter = "0";

    foreach ($res as $he => $details)
    {
        print "\nheadend: $he\n";
        print "location: {$details["location"]}\n";
        foreach ($details["lineups"] as $v)
        {
            $counter++;
            $name = $v["name"];
            $lineup = end(explode("/", $v["uri"]));
            $sdLineupArray[$counter] = $lineup;
            print "\t#$counter:\n";
            print "\tname: $name\n";
            print "\tLineup: $lineup\n";
        }
    }

    print "\n\n";
    $lineup = readline("Lineup to add (# or lineup)>");

    if ($lineup == "")
    {
        return;
    }

    if (strlen($lineup) < 3)
    {
        $lineup = $sdLineupArray[$lineup];
    }
    else
    {
        $lineup = strtoupper($lineup);
    }

    if (substr_count($lineup, "-") != 2)
    {
        print "Did not see two hyphens in lineup; did you enter it correctly?\n";

        return;
    }

    print "Sending request to server.\n";
    $lineup = str_replace(" ", "", $lineup);

    try
    {
        $response = $client->put("lineups/$lineup", array("token" => $token), array())->send();
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

function directAddLineup($lineup)
{
    global $debug;
    global $client;
    global $token;

    if (substr_count($lineup, "-") != 2)
    {
        print "Did not see two hyphens in lineup; did you enter it correctly?\n";

        return;
    }

    print "Sending request to server.\n";
    $lineup = str_replace(" ", "", $lineup);

    try
    {
        $response = $client->put("lineups/$lineup", array("token" => $token), array())->send();
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

    $deleteFromLocalCache = $dbh->prepare("DELETE FROM SDlineupCache WHERE lineup=:lineup");
    $removeFromVideosource = $dbh->prepare("UPDATE videosource SET lineupid='' WHERE lineupid=:lineup");

    $toDelete = getLineupFromNumber(strtoupper(readline("Lineup to Delete (# or lineup):>")));

    if ($toDelete == "")
    {
        return;
    }

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
    $deleteFromLocalCache->execute(array("lineup" => $toDelete));
    $removeFromVideosource->execute(array("lineup" => $toDelete));
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

function getLineup($lineupToGet)
{
    global $client;
    global $debug;
    global $token;

    print "Retrieving lineup $lineupToGet from Schedules Direct.\n";

    try
    {
        $response = $client->get("lineups/$lineupToGet", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        $s = json_decode($e->getResponse()->getBody(TRUE), TRUE);

        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return "";
    }

    try
    {
        $res = $response->json();
    } catch (Guzzle\Common\Exception\RuntimeException $e)
    {
        /*
         * Probably couldn't decode the JSON.
         */
        $message = $e->getMessage();
        print "Received error: $message\n";

        return "";
    }

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

function updateLocalLineupCache($updatedLineupsToRefresh)
{
    global $dbh;
    global $isMythTV;

    if (!$isMythTV)
    {
        return;
    }

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
        $stmt = $dbh->prepare("INSERT INTO SDlineupCache(lineup,json,modified)
        VALUES(:lineup,:json,:modified) ON DUPLICATE KEY UPDATE json=:json,modified=:modified");

        $stmt->execute(array("lineup" => $k, "modified" => $updatedLineupsToRefresh[$k],
                             "json"   => json_encode($res)));

        unset ($updatedLineupsToRefresh[$k]);
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

    $counter = "0";

    foreach ($sdStatus["lineups"] as $hv)
    {
        $counter++;
        $schedulesDirectLineups[$counter] = array("lineup" => $hv["ID"], "modified" => $hv["modified"]);
    }

    return ($schedulesDirectLineups);
}

function checkDatabase()
{
    global $dbh;
    global $dbWithoutMythtv;

    $createBaseTables = FALSE;

    if ($dbWithoutMythtv)
    {
        $stmt = $dbh->prepare("DESCRIBE settings");
        try
        {
            $stmt->execute();
        } catch (PDOException $ex)
        {
            if ($ex->getCode() == "42S02")
            {
                printMSG("Creating settings table.\n");
                $stmt = $dbh->exec("CREATE TABLE `settings`
                (
                `value` varchar(128) NOT NULL DEFAULT '',
                `data` varchar(16000) NOT NULL DEFAULT '',
                `hostname` varchar(64) DEFAULT NULL,
                KEY `value` (`value`,`hostname`)
                ) ENGINE=MyISAM DEFAULT CHARSET=utf8");

                setting("SchedulesDirectJSONschemaVersion", "28");
                setting("SchedulesDirectWithoutMythTV", "TRUE");
                $createBaseTables = TRUE;
            }
        }
    }
    else
    {
        $schemaVersion = setting("SchedulesDirectJSONschemaVersion");

        if ($schemaVersion === FALSE OR $schemaVersion == "26")
        {
            printMSG("Upgrading database from initial configuration.\n");
            $stmt = $dbh->prepare("DESCRIBE videosource");
            $stmt->execute();
            $columnNames = $stmt->fetchAll(PDO::FETCH_COLUMN);

            if (in_array("modified", $columnNames))
            {
                /*
                 * We're going to store lineup modification dates in the settings table so that we're not
                 * modifying a core MythTV table. But first we'll pull the existing information out.
                 */

                $stmt = $dbh->prepare("SELECT lineupid, modified FROM videosource");
                $stmt->execute();
                $existingLineups = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

                $foo = array();
                foreach ($existingLineups as $lineup => $modified)
                {
                    $foo[] = array("lineup" => $lineup, "lastModified" => $modified);
                }

                $bar = json_encode($foo);

                setting("localLineupLastModified", $bar);

                $dbh->exec("ALTER TABLE videosource DROP COLUMN modified");
            }

            $result = setting("schedulesdirectLogin");

            if ($result)
            {
                $dbh->exec("DELETE IGNORE FROM settings WHERE value='schedulesdirectLogin'");
                setting("SchedulesDirectLogin", $result);
            }
            $createBaseTables = TRUE;
        }

        if ($schemaVersion == "27")
        {
            printMSG("Upgrading to Schedules Direct schema 28.\n");
            $stmt = $dbh->exec("ALTER TABLE SDimageCache DROP KEY id");
            $stmt = $dbh->exec("ALTER TABLE SDimageCache CHANGE dimension height VARCHAR(128) NOT NULL");
            $stmt = $dbh->exec("ALTER TABLE SDimageCache ADD width VARCHAR(128) NOT NULL AFTER height");
            $stmt = $dbh->exec("ALTER TABLE SDimageCache ADD UNIQUE KEY id(item,height,width)");
            setting("SchedulesDirectJSONschemaVersion", "28");
            $schemaVersion = 28;
        }

        if ($schemaVersion == "28")
        {
            printMSG("Upgrading to Schedules Direct schema 29.\n");
            $stmt = $dbh->exec("DROP TABLE SDschedule");
            $stmt = $dbh->exec("CREATE TABLE `SDschedule` (
  `stationID` varchar(12) NOT NULL,
  `md5` char(22) NOT NULL,
  UNIQUE KEY `sid` (`stationID`),
  KEY `md5` (`md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");
        }
        setting("SchedulesDirectJSONschemaVersion", "29");
        $schemaVersion = 29;
    }

    if ($createBaseTables)
    {
        print "Creating Schedules Direct tables.\n";

        $stmt = $dbh->exec("DROP TABLE IF EXISTS SDprogramCache,SDcredits,SDlineupCache,SDpeople,SDprogramgenres,
    SDprogramrating,SDschedule,SDMessages,SDimageCache");

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

        $stmt = $dbh->exec("CREATE TABLE `SDlineupCache` (
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
  `md5` char(22) NOT NULL,
  UNIQUE KEY `sid` (`stationID`),
  KEY `md5` (`md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("ALTER TABLE credits CHANGE role role SET('actor','director','producer','executive_producer',
    'writer','guest_star','host','adapter','presenter','commentator','guest','musical_guest','judge',
    'correspondent','contestant')");

        $stmt = $dbh->exec("DELETE FROM settings WHERE VALUE IN('mythfilldatabaseLastRunStart',
        'mythfilldatabaseLastRunEnd','mythfilldatabaseLastRunStatus','MythFillSuggestedRunTime',
        'MythFillSuggestedRunTime','MythFillSuggestedRunTime','MythFillDatabaseArgs')");

        $stmt = $dbh->exec("INSERT INTO settings(value, data, hostname)
    VALUES('mythfilldatabaseLastRunStart', '',NULL),
    ('mythfilldatabaseLastRunEnd','',NULL),
    ('mythfilldatabaseLastRunStatus','',NULL),
    ('MythFillSuggestedRunTime','',NULL),
    ('DataDirectMessage','',NULL),
    ('MythFillDatabaseArgs','',NULL),
    ('SchedulesDirectLastUpdate','',NULL)");

        $stmt = $dbh->exec("CREATE TABLE `SDimageCache` (
  `row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `item` varchar(128) NOT NULL,
  `md5` char(22) NOT NULL,
  `height` varchar(128) NOT NULL,
  `width` varchar(128) NOT NULL,
  `type` char(1) NOT NULL COMMENT 'L-Channel Logo',
  PRIMARY KEY (`row`),
  UNIQUE KEY `id` (`item`,`height`,`width`),
  KEY `type` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $stmt = $dbh->exec("UPDATE videosource SET lineupid=''");

        setting("SchedulesDirectJSONschemaVersion", "28");
    }

    if ($schemaVersion == "26")
    {
        /*
         * Do whatever. Stub.
         */
    }
}

function putSchedulesDirectLoginIntoDB($usernameAndPassword)
{
    global $dbh;

    $isInDB = setting("SchedulesDirectLogin");

    if ($isInDB === FALSE)
    {
        setting("SchedulesDirectLogin", $usernameAndPassword);
    }
    else
    {
        $stmt = $dbh->prepare("UPDATE settings SET data=:json WHERE value='SchedulesDirectLogin'");
        $stmt->execute(array("json" => $usernameAndPassword));
    }
}

function checkForChannelIcon($stationID, $data)
{
    global $dbh;
    global $channelLogoDirectory;

    $a = explode("/", $data["URL"]);
    $iconFileName = end($a);

    $md5 = $data["md5"];
    $height = $data["height"];
    $width = $data["width"];

    $updateChannelTable = $dbh->prepare("UPDATE channel SET icon=:icon WHERE xmltvid=:stationID");

    $stmt = $dbh->prepare("SELECT md5 FROM SDimageCache WHERE item=:item AND height=:height AND width=:width");
    $stmt->execute(array("item" => $iconFileName, "height" => $height, "width" => $width));

    $result = $stmt->fetchColumn();

    if ($result === FALSE OR $result != $md5)
    {
        /*
         * We don't already have this icon, or it's different, so it will have to be fetched.
         */

        printMSG("Fetching logo $iconFileName for station $stationID");

        $success = file_put_contents("$channelLogoDirectory/$iconFileName", file_get_contents($data["URL"]));

        if ($success === FALSE)
        {
            printMSG("Check permissions: could not write to $channelLogoDirectory\n");

            return;
        }

        $updateSDimageCache = $dbh->prepare("INSERT INTO SDimageCache(item,height,width,md5,type)
        VALUES(:item,:height, :width,:md5,'L')
        ON DUPLICATE KEY UPDATE md5=:md5");
        $updateSDimageCache->execute(array("item" => $iconFileName, "height" => $height, "width" => $width,
                                           "md5"  => $md5));
        $updateChannelTable->execute(array("icon" => $iconFileName, "stationID" => $stationID));
    }
}

function extractData($sourceIDtoExtract)
{
    /*
     * Pulls data from the scanned table and preps it for sending to Schedules Direct.
     */

    global $dbh;
    global $todayDate;

    $extractArray = array();

    $stmt = $dbh->prepare("SELECT lineupid from videosource where sourceid=:sid");
    $stmt->execute(array("sid" => $sourceIDtoExtract));
    $lineupName = $stmt->fetchColumn();

    $stmt = $dbh->prepare("SELECT channum, callsign, xmltvid, mplexid, serviceid FROM channel where sourceid=:sid");
    $stmt->execute(array("sid" => $sourceIDtoExtract));
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    $getFrequency = $dbh->prepare("SELECT frequency FROM dtv_multiplex WHERE mplexid=:mplexid");

    if (count($result) == 0)
    {
        /*
         * Dump an error message and return.
         */
    }

    $fhExtract = fopen("$lineupName.extract.conf", "a");

    fwrite($fhExtract, "# jsonextract v0.01 $todayDate $lineupName\n");

    foreach ($result as $v)
    {
        $getFrequency->execute(array("mplexid" => $v["mplexid"]));
        $freq = $getFrequency->fetchColumn();

        $extractArray[] = array("chanNum"   => $v["channum"], "callSign" => $v["callsign"],
                                "xmltvid"   => $v["xmltvid"],
                                "mplexid"   => $v["mplexid"], "serviceid" => $v["serviceid"],
                                "frequency" => $freq[0]);

    }

    $json = json_encode($extractArray);

    fwrite($fhExtract, $json . "\n");

    print "JSON is \n$json\n";

    /*
     * TODO: send json automatically.
     */

    fclose($fhExtract);

    print "Please send $lineupName.extract.conf to grabber@schedulesdirect.org\n";
}

function getLineupFromNumber($numberOrLineup)
{
    global $lineupArray;

    if (strlen($numberOrLineup) < 3)
    {
        $foo = (int)$numberOrLineup;

        if (!array_key_exists($foo, $lineupArray))
        {
            return "";
        }
        else
        {
            return $lineupArray[$numberOrLineup]["lineup"];
        }
    }
    else
    {
        return ($numberOrLineup); //We're actually just returning the name of the array.
    }
}

function printListOfAvailableCountries()
{
    global $availableCountries;

    foreach ($availableCountries as $region => $data)
    {
        print "\nRegion:$region\n";

        $countryWidth = 0;
        foreach ($data as $country => $tla)
        {
            if (strlen($country) > $countryWidth)
            {
                $countryWidth = strlen($country);
            }
        }

        $countryList = new Zend\Text\Table\Table(array('columnWidths' => array($countryWidth + 2, 18)));
        $countryList->appendRow(array("Country", "Three-letter code"));

        foreach ($data as $country => $tla)
        {
            $countryList->appendRow(array($country, $tla));
        }

        print $countryList;
    }
}

?>