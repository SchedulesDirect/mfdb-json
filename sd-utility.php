#!/usr/bin/php

<?php
/*
sd-utility.php. This file is a utility program which performs the necessary setup for using the
Schedules Direct API and the native MythTV tables.
Copyright (C) 2012-2014  Robert Kulagowski grabber@schedulesdirect.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/

$done = false;
$skipChannelLogo = false;
$forceLogoUpdate = false;
$schedulesDirectLineups = array();
$username = "";
$password = "";
$passwordHash = "";
$printFancyTable = true;
$printCountries = false;
$justExtract = false;
$updatedLineupsToRefresh = array();
$needToStoreLogin = false;
$resetDatabase = false;
$useServiceAPI = false;
$usernameFromDB = "";
$passwordFromDB = "";

require_once "vendor/autoload.php";
require_once "functions.php";
use Guzzle\Http\Client;

$channelLogoDirectory = getChannelLogoDirectory();
$agentString = "sd-utility.php utility program API:$api v$scriptVersion/$scriptDate";

$helpText = <<< eol
The following options are available:
--beta\t\tUse beta server. (Default: FALSE)
--countries\tThe list of countries that have data.
--debug\t\tEnable debugging. (Default: FALSE)
--dbname=\tMySQL database name for MythTV. (Default: mythconverg)
--dbuser=\tUsername for database access for MythTV. (Default: mythtv)
--dbpassword=\tPassword for database access for MythTV. (Default: mythtv)
--dbhost=\tMySQL database hostname for MythTV. (Default: localhost)
--dbhostsd=\tMySQL database hostname for SchedulesDirect JSON data. (Default: localhost)
--extract\tDon't do anything but extract data from the table for QAM/ATSC. (Default: FALSE)
--forcelogo\tForce update of channel logos.
--forcerun\tContinue to run even if we're known to be broken. (Default: FALSE)
--help\t\t(this text)
--host=\t\tIP address of the MythTV backend. (Default: localhost)
--logo=\t\tDirectory where channel logos are stored (Default: $channelLogoDirectory)
--nodb\t\tThe scripts won't store anything in databases and just become fetchers. (Default: FALSE)
--nomyth\tDon't execute any MythTV specific functions. (Default: FALSE)
--skiplogo\tDon't download channel logos.
--username=\tSchedules Direct username.
--password=\tSchedules Direct password.
--reset\t\tReset the database.
--timezone=\tSet the timezone for log file timestamps. See http://www.php.net/manual/en/timezones.php (Default:$tz)
--skipversion\tForce the program to run even if there's a version mismatch between the client and the server.
--usedb\t\tUse a database to store data, even if you're not running MythTV. (Default: FALSE)
--usesqlite\tUse a SQLite database to store Schedules Direct data. (Default: FALSE)
--version\tPrint version information and exit.
eol;

$longoptions = array(
    "beta",
    "countries",
    "debug",
    "extract",
    "forcelogo",
    "forcerun",
    "help",
    "host::",
    "dbname::",
    "dbuser::",
    "dbpassword::",
    "dbhost::",
    "dbhostsd::",
    "logo::",
    "notfancy",
    "nodb",
    "nomyth",
    "skiplogo",
    "username::",
    "password::",
    "reset",
    "skipversion",
    "timezone::",
    "usedb",
    "usesqlite",
    "version"
);

$options = getopt("h::", $longoptions);
foreach ($options as $k => $v) {
    $k = strtolower($k);
    switch ($k) {
        case "beta":
            $isBeta = true;
            break;
        case "countries":
            $printCountries = true;
            break;
        case "debug":
            $debug = true;
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
        case "dbhostsd":
            $dbHostSchedulesDirectData = $v;
            break;
        case "extract":
            $justExtract = true;
            break;
        case "forcerun":
            $forceRun = true;
            break;
        case "forcelogo":
            $forceLogoUpdate = true;
            break;
        case "host":
            $host = $v;
            break;
        case "logo":
            $channelLogoDirectory = $v;
            break;
        case "nomyth":
            $isMythTV = false;
            $dbWithoutMythTV = true;
            break;
        case "nodb":
            $noDB = true; // Just become a fancy wget
            $isMythTV = false;
            $skipChannelLogo = true;
            $dbWithoutMythTV = false;
            break;
        case "notfancy":
            $printFancyTable = false;
            break;
        case "skiplogo":
            $skipChannelLogo = true;
            break;
        case "username":
            $username = $v;
            break;
        case "password":
            $password = $v;
            $passwordHash = sha1($v);
            break;
        case "reset":
            $resetDatabase = true;
            break;
        case "skipversion":
            $skipVersionCheck = true;
            break;
        case "timezone":
            date_default_timezone_set($v);
            break;
        case "usedb":
            $dbWithoutMythTV = true;
            break;
        case "usesqlite":
            $useSQLite = true;
            break;
        case "version":
            print "$agentString\n\n";
            exit;
            break;
    }
}

if ($printCountries === true) {
    $availableCountries = getListOfAvailableCountries();
    printListOfAvailableCountries($printFancyTable);
    exit;
}

if (($knownToBeBroken === true) AND ($forceRun === false)) {
    print "This version is known to be broken and --forcerun not specified. Exiting.\n";
    exit;
}

if ($channelLogoDirectory == "UNKNOWN" AND $skipChannelLogo === false) {
    print "Can't determine directory for station logos. Please specify using --logo or use --skiplogo\n";
    exit;
}

$baseurl = getBaseURL($isBeta);

$client = new Guzzle\Http\Client($baseurl);
$client->setUserAgent($agentString);

print "Using timezone $tz\n";
print "$agentString\n";

if ($isMythTV === true) {
    if
    (
        (isset($dbHost) === false) AND
        (isset($dbName) === false) AND
        (isset($dbUser) === false) AND
        (isset($dbPassword) === false)
    ) {
        list($dbHost, $dbName, $dbUser, $dbPassword) = getLoginFromFiles();
        if ($dbHost == "NONE") {
            $dbUser = "mythtv";
            $dbPassword = "mythtv";
            $dbHost = "localhost";
            $dbName = "mythconverg";
            $host = "localhost";
        }
    }
}

if (($isMythTV === true) OR ($dbWithoutMythTV === true)) {
    printMSG("Connecting to Schedules Direct database.");

    if ($useSQLite === true) {
        $dbhSD = new PDO("sqlite:schedulesdirect.db");
        $dbhSD->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    } else {
        try {
            $dbhSD = new PDO("mysql:host=$dbHostSchedulesDirectData;dbname=schedulesdirect;charset=utf8", "sd", "sd");
            $dbhSD->exec("SET CHARACTER SET utf8");
            $dbhSD->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            switch ($e->getCode()) {
                case 2002:
                    print "Could not connect to database:\n" . $e->getMessage() . "\n";
                    exit;
                    break;
                case 1049:
                    print "Initial database not created for Schedules Direct tables.\n";
                    print "Please run\nmysql -uroot -p < sd.sql\n";
                    print "Then re-run this script.\n";
                    print "Please check the updated README.md for more information.\n";
                    exit;
                    break;
                case "42S02":
                    print "Creating initial database.\n";
                    createDatabase($useSQLite);
                    $dbhSD = new PDO("mysql:host=$dbHostSchedulesDirectData;dbname=schedulesdirect;charset=utf8", "sd",
                        "sd");
                    $dbhSD->exec("SET CHARACTER SET utf8");
                    $dbhSD->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
                    break;
            }
        }
    }

    /*
     * OK, so we have a connection, but that doesn't mean that there's anything in the database yet.
     */

    $result = array();

    try {
        $stmt = $dbhSD->prepare("SELECT * FROM settings");
        $stmt->execute();
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
    } catch (PDOException $e) {
        switch ($e->getCode()) {
            case "42S02":
            case "HY000":
                createDatabase($useSQLite);
                break;
            default:
                print "Got error connecting to database.\n";
                print "Code: " . $e->getCode() . "\n";
                print "Message: " . $e->getMessage() . "\n";
                exit;
        }

        $dbhSD = new PDO("mysql:host=$dbHostSchedulesDirectData;dbname=schedulesdirect;charset=utf8", "sd",
            "sd");
        $dbhSD->exec("SET CHARACTER SET utf8");
        $dbhSD->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    if ($resetDatabase === true OR count($result) == 0) {
        createDatabase($useSQLite);
    }

    if ($isMythTV === true) {
        print "Connecting to MythTV database.\n";
        try {
            $dbh = new PDO("mysql:host=$dbHost;dbname=$dbName;charset=utf8", $dbUser, $dbPassword);
            $dbh->exec("SET CHARACTER SET utf8");
            $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            if ($e->getCode() == 2002) {
                print "Could not connect to database:\n" . $e->getMessage() . "\n";
                print "If you're running the grabber as standalone, use --nomyth\n";
                exit;
            } else {
                print "Got error connecting to database.\n";
                print "Code: " . $e->getCode() . "\n";
                print "Message: " . $e->getMessage() . "\n";
                exit;
            }
        }
    }

    if ($justExtract === true) {
        if (isset($dbh) === false) {
            print "Don't have dbh. Exiting.\n";
            exit;
        }

        if (isset($dbhSD) === false) {
            print "Don't have dbhSD. Exiting.\n";
            exit;
        }

        displayLocalVideoSources();
        $sourceIDtoExtract = readline("Which sourceid do you want to extract:>");
        if ($sourceIDtoExtract != "") {
            extractData($sourceIDtoExtract);
        }
        exit;
    }

    checkForSchemaUpdate($useSQLite);
}

if ($skipChannelLogo === false) {
    if (file_exists($channelLogoDirectory) === false) {
        $result = @mkdir($channelLogoDirectory);

        if ($result === false) {
            print "Could not create $channelLogoDirectory\n";
            print "Use --logo to specify directory, or --skiplogo to bypass channel logos.\n";
            exit;
        }
    }
}

if ($skipVersionCheck === false) {
    print "Checking to see if we're running the latest client.\n";

    list($hadError, $serverVersion) = checkForClientUpdate($client);

    if ($hadError !== false) {
        print "Received error response from server. Exiting.\n";
        exit;
    }

    if ($serverVersion != $scriptVersion) {
        print "***Version mismatch.***\n";
        print "Server version: $serverVersion\n";
        print "Our version: $scriptVersion\n";
        if ($forceRun === false) {
            print "Exiting. Do you need to run 'git pull' to refresh?\n";
            print "Restart script with --skipversion to ignore mismatch.\n";
            exit;
        } else {
            print "Continuing because of --skipversion.\n";
        }
    }
}

if ($isMythTV === true) {
    $useServiceAPI = checkForServiceAPI();
}

if (($isMythTV === true) OR ($dbWithoutMythTV === true)) {
    $userLoginInformation = settingSD("SchedulesDirectLogin");

    if ($userLoginInformation !== false) {
        $responseJSON = json_decode($userLoginInformation, true);
        $usernameFromDB = $responseJSON["username"];
        $passwordFromDB = $responseJSON["password"];
    }
} else {
    if (file_exists("sd.json.conf") === true) {
        $userLoginInformation = file("sd.json.conf");
        $responseJSON = json_decode($userLoginInformation[0], true);
        $usernameFromDB = $responseJSON["username"];
        $passwordFromDB = $responseJSON["password"];
    }
}

if ($username == "") {
    if ($usernameFromDB == "") {
        $username = readline("Schedules Direct username:");
        $needToStoreLogin = true;
    } else {
        $username = $usernameFromDB;
    }
} else {
    $needToStoreLogin = true;
}

if ($password == "") {
    if ($passwordFromDB == "") {
        $password = readline("Schedules Direct password:");
        $passwordHash = sha1($password);
        $needToStoreLogin = true;
    } else {
        $password = $passwordFromDB;
        $passwordHash = sha1($password);
    }
} else {
    $passwordHash = sha1($password);
    $needToStoreLogin = true;
}

print "Logging into Schedules Direct.\n";
list($hadError, $token) = getToken($username, $passwordHash);

if ($hadError === true) {
    printMSG("Got error when attempting to retrieve token from Schedules Direct.");
    printMSG("Check if you entered username/password incorrectly.");
    exit;
}

if ($needToStoreLogin === true) {
    $userInformation["username"] = $username;
    $userInformation["password"] = $password;

    $credentials = json_encode($userInformation);

    if (($isMythTV === true) OR ($dbWithoutMythTV === true)) {
        settingSD("SchedulesDirectLogin", $credentials);
        // We need to check and see if there are any lineups in the database yet before running an update.
        $stmt = $dbh->prepare("UPDATE videosource SET userid=:username,
    password=:password WHERE xmltvgrabber='schedulesdirect-json'");
        $stmt->execute(array("username" => $username, "password" => $password));
    } else {
        $fh = fopen("sd.json.conf", "w");
        fwrite($fh, "$credentials\n");
        fclose($fh);
    }
}

while ($done === false) {
    $sdStatus = getStatus();

    if ($sdStatus == "ERROR") {
        printMSG("Received error from Schedules Direct. Exiting.");
        exit;
    }

    printStatus($sdStatus);

    if ($isMythTV === true) {
        displayLocalVideoSources();
    }

    print "\nSchedules Direct functions:\n";
    print "1 Add a known lineupID to your account at Schedules Direct\n";
    print "2 Search for a lineup to add to your account at Schedules Direct\n";
    print "3 Delete a lineup from your account at Schedules Direct\n";
    print "4 Refresh the local lineup cache\n";
    print "5 Acknowledge a message\n";
    print "6 Print a channel table for a lineup\n";

    if ($isMythTV === true) {
        print "\nMythTV functions:\n";
        print "Videosource functions\n---------------------\n";
        print "A to Add a new videosource to MythTV\n";
        print "D to Delete a videosource in MythTV\n";
        print "L to Link a videosource to a lineup at Schedules Direct\n";
        print "U to Update a videosource by downloading from Schedules Direct\n";

        print "\n\nCapture card functions\n----------------------\n";
        print "C to Connect a capture card input to a videosource\n";

        print "\n\nMiscellaneous\n-------------\n";
        print "E to Extract Antenna / QAM / DVB scan from MythTV to send to Schedules Direct\n";
    }

    print "Q to Quit\n";

    $response = strtoupper(readline(">"));

    switch ($response) {
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
            if (count($sdStatus["lineups"]) != 0) {
                $lineupToRefresh = array();
                foreach ($sdStatus["lineups"] as $v) {
                    $lineupToRefresh[$v["lineup"]] = 1;
                }
                storeLocalLineup($lineupToRefresh);
            }
            break;
        case "5":
            deleteMessageFromSchedulesDirect();
            break;
        case "6":
            printLineup();
            break;
        case "A":
            print "Adding new videosource\n\n";
            $newName = readline("Name:>");
            if ($newName != "") {
                if ($useServiceAPI === true) {
                    $request = $client->post("http://$host:6544/Channel/AddVideoSource")
                        ->setPostField("Grabber", "schedulesdirect-json")
                        ->setPostField("SourceName", $newName)
                        ->setPostField("UserId", $username)
                        ->setPostField("FreqTable", "default")
                        ->setPostField("Password", $password)
                        ->setPostField("NITId", "-1");

                    $response = $request->send();
                } else {
                    $stmt = $dbh->prepare("INSERT INTO videosource(name,userid,freqtable,password,xmltvgrabber,
                    dvb_nit_id)
                        VALUES(:name,:userid,'default',:password,'schedulesdirect-json','-1')");
                    try {
                        $stmt->execute(array(
                            "name"     => $newName,
                            "userid"   => $username,
                            "password" => $password
                        ));
                    } catch (PDOException $e) {
                        if ($e->getCode() == 23000) {
                            print "\n\n";
                            print "*************************************************************\n";
                            print "\n\n";
                            print "Duplicate video source name.\n";
                            print "\n\n";
                            print "*************************************************************\n";
                        }
                    }
                }
            }
            break;
        case "C":
            break;
        case "D":
            $toDelete = readline("Delete sourceid:>");
            if ($useServiceAPI === true) {
                $request = $client->post("http://$host:6544/Channel/RemoveVideoSource")
                    ->setPostField("SourceID", $toDelete);
                $response = $request->send();
            } else {
                $stmt = $dbh->prepare("DELETE FROM videosource WHERE sourceid=:sid");
                $stmt->execute(array("sid" => $toDelete));
                $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sid");
                $stmt->execute(array("sid" => $toDelete));
            }
            break;
        case "E":
            $sourceIDtoExtract = readline("Which sourceid do you want to extract:>");
            if ($sourceIDtoExtract != "") {
                extractData($sourceIDtoExtract);
            }
            break;
        case "L":
            print "Linking Schedules Direct lineup to sourceid\n\n";
            linkSchedulesDirectLineup();
            break;
        case "U":
            list($lineup, $isDeleted) =
                getLineupFromNumber(strtoupper(readline("Schedules Direct lineup (# or lineup):>")));
            if ($lineup != "" AND $isDeleted === false) {
                updateChannelTable($lineup);
            }
            break;
        case "Q":
        default:
            $done = true;
            break;
    }
}

exit;

function updateChannelTable($lineup)
{
    global $dbh;
    global $dbhSD;
    global $skipChannelLogo;
    global $useServiceAPI;
    global $client;
    global $host;

    $transport = "";

    if ($useServiceAPI === true) {
        try {
            $response = $client->get("http://$host:6544/Channel/GetVideoSourceList", array(), array(
                "headers" => array("Accept" => "application/json")
            ))->send();
        } catch (Guzzle\Http\Exception\BadResponseException $e) {
            $s = json_decode($e->getResponse()->getBody(true), true);

            print "********************************************\n";
            print "\tError response from server:\n";
            print "\tCode: {$s["code"]}\n";
            print "\tMessage: {$s["message"]}\n";
            print "\tServer: {$s["serverID"]}\n";
            print "********************************************\n";

            return "";
        }

        $res = $response->json();
        var_dump($res);


    } else {
        $stmt = $dbh->prepare("SELECT sourceid FROM videosource WHERE lineupid=:lineup");
        $stmt->execute(array("lineup" => $lineup));
        $sourceID = $stmt->fetch(PDO::FETCH_COLUMN);
    }

    if ($sourceID == "") {
        print "ERROR: Can't update channel table; lineup not associated with a videosource.\n";

        return;
    }

    $stmt = $dbhSD->prepare("SELECT json FROM lineups WHERE lineup=:lineup");
    $stmt->execute(array("lineup" => $lineup));
    $json = json_decode($stmt->fetchColumn(), true);

    $modified = $json["metadata"]["modified"];

    print "Updating channel table for lineup:$lineup\n";

    $dbh->exec("DROP TABLE IF EXISTS t_channel");
    $dbh->exec("CREATE TABLE t_channel LIKE channel");
    $dbh->exec("INSERT INTO t_channel SELECT * FROM channel");

    if ($json["metadata"]["transport"] == "Antenna") {
        /*
         * For antenna lineups, we're not going to delete the existing channel table or dtv_multiplex; we're still
         * going to use the scan, but use the atsc major and minor to correlate what we've scanned with what's in the
         * lineup file.
         */
        $transport = "Antenna";
    }

    if ($json["metadata"]["transport"] == "Cable") {
        if (isset($json["metadata"]["modulation"]) === true) {
            $transport = "QAM";
        } else {
            $transport = "Cable";

            $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sourceid");
            $stmt->execute(array("sourceid" => $sourceID));
        }
    }

    if ($json["metadata"]["transport"] == "Satellite") {
        $transport = "Satellite";

        $stmt = $dbh->prepare("DELETE FROM channel WHERE sourceid=:sourceid");
        $stmt->execute(array("sourceid" => $sourceID));
    }

    if ($json["metadata"]["transport"] == "FTA") {
        $transport = "FTA";
    }

    if ($json["metadata"]["transport"] == "DVB-S") {
        $transport = "DVB-S";
    }

    /*
     * This whole next part needs to get rewritten.
     */

    if ($transport == "Satellite" OR $transport == "Antenna" OR $transport == "Cable") {
        $updateChannelTableATSC = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=:atscMajor AND atsc_minor_chan=:atscMinor");

        $updateChannelTableAnalog = $dbh->prepare("UPDATE channel SET channum=:channum,
    xmltvid=:sid, useonairguide=0 WHERE atsc_major_chan=0 AND atsc_minor_chan=0 AND freqID=:freqID");

        foreach ($json["map"] as $mapArray) {
            $stationID = $mapArray["stationID"];

            if ($transport == "Antenna") {
                if (isset($mapArray["uhfVhf"]) === true) {
                    $freqid = $mapArray["uhfVhf"];
                } else {
                    $freqid = "";
                }

                if (isset($mapArray["atscMajor"]) === true) {
                    $atscMajor = $mapArray["atscMajor"];
                    $atscMinor = $mapArray["atscMinor"];
                    $channum = "$atscMajor.$atscMinor";

                    $updateChannelTableATSC->execute(array(
                        "channum"   => $channum,
                        "sid"       => $stationID,
                        "atscMajor" => $atscMajor,
                        "atscMinor" => $atscMinor
                    ));
                } else {
                    $channum = $freqid;
                    $updateChannelTableAnalog->execute(array(
                        "channum" => ltrim($channum, "0"),
                        "sid"     => $stationID,
                        "freqID"  => $freqid
                    ));
                }
            }

            if ($transport == "IP") {
                /*
                 * Nothing yet.
                 */
            }

            if ($transport == "Cable" OR $transport == "Satellite") {
                $channum = $mapArray["channel"];
                $stmt = $dbh->prepare(
                    "INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid,mplexid,serviceid,atsc_major_chan)
                         VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid,:mplexid,:serviceid,:atsc_major_chan)");

                try {
                    $stmt->execute(array(
                        "chanid"          => (int)($sourceID * 1000) + (int)$channum,
                        "channum"         => ltrim($channum, "0"),
                        "freqid"          => (int)$channum,
                        "sourceid"        => $sourceID,
                        "xmltvid"         => $stationID,
                        "mplexid"         => 32767,
                        "serviceid"       => 0,
                        "atsc_major_chan" => $channum
                    ));
                } catch (PDOException $e) {
                    if ($e->getCode() == 23000) {
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

                $getOriginalRecPriority = $dbh->prepare("SELECT t_channel.chanid,t_channel.recpriority FROM
                    t_channel INNER JOIN channel WHERE channel.xmltvid=t_channel.xmltvid and t_channel.sourceid=:sid");
                $getOriginalRecPriority->execute(array("sid" => $sourceID));
                $originalRecPriorityArray = $getOriginalRecPriority->fetchAll(PDO::FETCH_KEY_PAIR);

                $getOriginalVisibility = $dbh->prepare("SELECT t_channel.chanid,t_channel.visible FROM
                    t_channel INNER JOIN channel WHERE channel.xmltvid=t_channel.xmltvid and t_channel.sourceid=:sid");
                $getOriginalVisibility->execute(array("sid" => $sourceID));
                $originalVisibilityArray = $getOriginalVisibility->fetchAll(PDO::FETCH_KEY_PAIR);

                $updateChannel = $dbh->prepare("UPDATE channel SET recpriority=:rp,
                    visible=:visible WHERE chanid=:chanid");

                foreach ($originalRecPriorityArray as $chanid => $foo) {
                    $updateChannel->execute(array(
                        "rp"      => $originalRecPriorityArray[$chanid],
                        "visible" => $originalVisibilityArray[$chanid],
                        "chanid"  => $chanid
                    ));
                }
            }
        }
    } else {
        /*
         * It's QAM or FTA. We're going to run a match based on what the user had already scanned to avoid the
         * manual matching step of correlating stationIDs.
         */

        print "You can:\n";
        print "1. Use the $transport lineup from SD to populate stationIDs/xmltvids after having run a mythtv-setup channel scan.\n";
        print "2. Use the $transport lineup information and update the database without running a scan.\n";
        $useScan = readline("Which do you want to do? (1 or 2)>");

        if ($useScan == "") {
            return;
        }

        if ($useScan == "2") {
            $useScan = false;
        } else {
            $useScan = true;
        }

        if ($useScan === true) {
            if ($transport == "QAM") {
                $matchType = $json["map"]["matchType"];

                print "Matching $transport scan based on: $matchType\n";

                if ($matchType == "multiplex") {
                    $stmt = $dbh->prepare("SELECT mplexid, frequency FROM dtv_multiplex WHERE modulation='qam_256'");
                    $stmt->execute();
                    $qamFrequencies = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

                    $stmt = $dbh->prepare("SELECT * FROM channel WHERE sourceid=:sid");
                    $stmt->execute(array("sid" => $sourceID));
                    $existingChannelNumbers = $stmt->fetchAll(PDO::FETCH_ASSOC);

                    $updateChannelTableQAM = $dbh->prepare("UPDATE channel SET xmltvid=:stationID WHERE
                mplexid=:mplexid AND serviceid=:serviceid");

                    $map = array();

                    foreach ($json["map"] as $foo) {
                        $map["{$foo["frequency"]}-{$foo["serviceID"]}"] = $foo["stationID"];
                    }

                    foreach ($existingChannelNumbers as $foo) {
                        $toFind = "{$qamFrequencies[$foo["mplexid"]]}-{$foo["serviceid"]}";

                        if (isset($map[$toFind]) === true) {
                            $updateChannelTableQAM->execute(array(
                                "stationID" => $map[$toFind],
                                "mplexid"   => $foo["mplexid"],
                                "serviceid" => $foo["serviceID"]
                            ));
                        }
                    }
                }

                if ($matchType == "providerCallsign") {
                    $updateChannelTable = $dbh->prepare("UPDATE channel SET xmltvid=:stationID,freqid=:virtualChannel
                    WHERE callsign=:providerCallsign");
                    foreach ($json["map"] as $foo) {
                        $updateChannelTable->execute(array(
                            "stationID"        => $foo["stationID"],
                            "virtualChannel"   => $foo["virtualChannel"],
                            "providerCallsign" => $foo["providerCallsign"]
                        ));
                    }
                }

                if ($matchType == "channel") {
                    $updateChannelTable = $dbh->prepare("UPDATE channel SET xmltvid=:stationID,freqid=:virtualChannel
                    WHERE channum=:channel");
                    foreach ($json["map"] as $foo) {
                        $updateChannelTable->execute(array(
                            "stationID"      => $foo["stationID"],
                            "virtualChannel" => $foo["virtualChannel"],
                            "channum"        => $foo["channel"]
                        ));
                    }
                }
                print "Done updating QAM scan with stationIDs.\n";
            }
        } else {
            if ($transport == "QAM") {
                /*
                 * The user has chosen to not run a QAM scan and just use the values that we're supplying.
                 * Work-in-progress.
                 */

                print "Inserting $transport data into tables.\n";

                $dtvMultiplex = array();

                $insertDTVMultiplex = $dbh->prepare
                ("INSERT INTO dtv_multiplex
        (sourceid,frequency,symbolrate,polarity,modulation,visible,constellation,hierarchy,mod_sys,rolloff,sistandard)
        VALUES
        (:sourceid,:freq,0,'v',:modulation,1,:modulation,'a','UNDEFINED','0.35','atsc')");
                $stmt = $dbh->prepare("SELECT mplexid, frequency FROM dtv_multiplex WHERE modulation='qam_256'
                AND sourceid=:sourceid");
                $stmt->execute(array("sourceid" => $sourceID));
                $dtvMultiplex = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
                $channelInsert = $dbh->prepare("INSERT INTO channel(chanid,channum,freqid,sourceid,xmltvid,tvformat,
visible,mplexid,serviceid,atsc_major_chan,atsc_minor_chan)
                     VALUES(:chanid,:channum,:freqid,:sourceid,:xmltvid,'ATSC','1',:mplexid,:serviceid,:channelMajor,
                     :channelMinor)");

                foreach ($json["map"] as $mapArray) {
                    $virtualChannel = $mapArray["virtualChannel"]; // "Channel 127"

                    if (isset($mapArray["channel"]) === true) {
                        $channel = $mapArray["channel"]; // "54-18" or whatever.
                    } else {
                        $channel = $virtualChannel;
                    }

                    switch ($mapArray["modulation"]) {
                        case "QAM64":
                            $modulation = "qam_64";
                            break;
                        case "QAM256":
                            $modulation = "qam_256";
                            break;
                        default:
                            print "Unknown modulation: {$mapArray["modulation"]}\n";
                            exit;
                    }

                    $frequency = $mapArray["frequencyHz"];
                    $serviceID = $mapArray["serviceID"];
                    $stationID = $mapArray["stationID"];
                    $channelMajor = (int)$mapArray["channelMajor"];
                    $channelMinor = (int)$mapArray["channelMinor"];

                    /*
                     * Because multiple programs may end up on a single frequency, we only want to insert once,
                     * but we want to track the mplexid assigned when we do the insert,
                     * because that might be used more than once.
                     */
                    if (isset($dtvMultiplex[$frequency]) === false) {
                        $insertDTVMultiplex->execute(array(
                            "sourceid"   => $sourceID,
                            "freq"       => $frequency,
                            "modulation" => $modulation
                        ));
                        $dtvMultiplex[$frequency] = $dbh->lastInsertId();
                    }

                    /*
                     * In order to insert a unique channel ID, we need to make sure that "39_1" and "39_2" map to two
                     * different values. Old code resulted in 39_1 -> 39, then 39_2 had a collision because it also
                     * turned into "39"
                     */

                    $strippedChannel = (int)str_replace(array("-", "_"), "", $channel);

                    if ($channelMajor > 0) {
                        $chanid = ($sourceID * 1000) + ($channelMajor * 10) + $channelMinor;
                    } else {
                        $chanid = ($sourceID * 1000) + ($strippedChannel * 10) + $serviceID;
                    }

                    try {
                        $channelInsert->execute(array(
                            "chanid"    => $chanid,
                            "channum"   => $channel,
                            "freqid"    => $virtualChannel,
                            "sourceid"  => $sourceID,
                            "xmltvid"   => $stationID,
                            "mplexid"   => $dtvMultiplex[$frequency],
                            "serviceid" => $serviceID,
                            "atscMajor" => $channelMajor,
                            "atscMinor" => $channelMinor
                        ));
                    } catch (PDOException $e) {
                        if ($e->getCode() == 23000) {
                            print "\n\n";
                            print "*************************************************************\n";
                            print "\n\n";
                            print "Error inserting data. Duplicate channel number exists?\n";
                            print "Send email to grabber@schedulesdirect.org with the following:\n\n";
                            print "Duplicate channel error.\n";
                            print "Transport: $transport\n";
                            print "Lineup: $lineup\n";
                            print "Channum: $channel\n";
                            print "Virtual: $virtualChannel\n";
                            print "stationID: $stationID\n";
                            print "\n\n";
                            print "*************************************************************\n";
                        }
                    }
                }
                print "Done inserting QAM tuning information directly into tables.\n";
            }
        }

        if ($transport == "FTA") {
            $insertDTVMultiplex = $dbh->prepare
            ("INSERT INTO dtv_multiplex
        (sourceid,transportid,networkid,frequency,symbolrate,polarity,modulation,visible,constellation,hierarchy,
        mod_sys,rolloff,sistandard)
        VALUES
        (:sourceid,:tid,:nid,:freq,:symbolrate,:polarity,:modulation,1,:constellation,'a','0.35','dvb')");

            $channelInsert = $dbh->prepare("INSERT INTO channel
                    (chanid,
                    channum,
                    sourceid,
                    xmltvid,
                    visible,
                    mplexid,
                    serviceid
                    )
                    VALUES(
                    :chanid,
                    :channum,
                    :sourceid,
                    :xmltvid,
                    '1',
                    :mplexid,
                    :serviceid,
                    )");
        }

        foreach ($json["satelliteDetail"] as $satellite) {
            foreach ($satellite as $entry) {
                $transportID = $entry["transportID"];
                $networkID = $entry["networkID"];
                $frequency = $entry["frequencyMHz"] * 1000;
                $symbolrate = $entry["symbolrate"] * 1000;
                $polarity = $entry["polarization"];
                $modulation = strtolower($entry["modulationSystem"]);
                $constellation = strtolower($entry["modulationSystem"]);
                $mod_sys = $entry["deliverySystem"];
            }
        }
    }

    /*
     * Now that we have basic information in the database, we can start filling in other things, like
     * callsigns, etc.
     */

    $stmt = $dbh->prepare("UPDATE channel SET name=:name, callsign=:callsign WHERE xmltvid=:stationID");

    foreach ($json["stations"] as $stationArray) {
        $stationID = $stationArray["stationID"];
        $callsign = $stationArray["callsign"];

        if (array_key_exists("name", $stationArray)) {
            /*
             * Not all stations have names, so don't try to insert a name if that field isn't included.
             */
            $name = $stationArray["name"];
        } else {
            $name = "";
        }

        if (isset($stationArray["logo"]) === true) {
            if ($skipChannelLogo === false) {
                checkForChannelIcon($stationID, $stationArray["logo"]);
            }
        }

        $stmt->execute(array("name" => $name, "callsign" => $callsign, "stationID" => $stationID));
    }

    $lineupLastModifiedJSON = settingSD("localLineupLastModified");
    $lineupLastModifiedArray = array();

    if (count($lineupLastModifiedJSON) != 0) {
        $lineupLastModifiedArray = json_decode($lineupLastModifiedJSON, true);
    }

    $lineupLastModifiedArray[$lineup] = $modified;

    settingSD("localLineupLastModified", json_encode($lineupLastModifiedArray));

    /*
     * Set the startchan to a non-bogus value.
     */
    $getChanNum = $dbh->prepare("SELECT channum FROM channel WHERE sourceid=:sourceid
    ORDER BY CAST(channum AS SIGNED) LIMIT 1");

    $getChanNum->execute(array("sourceid" => $sourceID));
    $result = $getChanNum->fetchColumn();

    if ($result != "") {
        $startChan = $result;
        $setStartChannel = $dbh->prepare("UPDATE cardinput SET startchan=:startChan WHERE sourceid=:sourceid");
        $setStartChannel->execute(array("sourceid" => $sourceID, "startChan" => $startChan));
    }

    $dbh->exec("DROP TABLE t_channel");
}

function linkSchedulesDirectLineup()
{
    global $dbh;
    global $dbhSD;
    global $useServiceAPI;
    global $client;
    global $host;

    $sid = readline("MythTV sourceid:>");

    if ($sid == "") {
        return;
    }

    list ($lineup, $isDeleted) = getLineupFromNumber(strtoupper(readline("Schedules Direct lineup (# or lineup):>")));

    if (($lineup == "") OR ($isDeleted === true)) {
        return;
    }

    $stmt = $dbhSD->prepare("SELECT json FROM lineups WHERE lineup=:lineup");
    $stmt->execute(array("lineup" => $lineup));
    $response = json_decode($stmt->fetchColumn(), true);

    if (count($response) == 0) // We've already decoded the JSON.
    {
        print "Fatal Error in Link SchedulesDirect Lineup.\n";
        print "No JSON for lineup in schedules direct local cache?\n";
        print "lineup:$lineup\n";
        exit;
    }

    if ($response == "[]") {
        print "Fatal Error in Link SchedulesDirect Lineup.\n";
        print "Empty JSON for lineup in Schedules Direct local cache?\n";
        print "lineup:$lineup\n";
        exit;
    }

    if ($useServiceAPI === true) {
        try {
            $response = $client->get("http://$host:6544/Channel/GetVideoSource", array(), array(
                "query"   => array("SourceID" => $sid),
                "headers" => array("Accept" => "application/json")
            ))->send();
        } catch (Guzzle\Http\Exception\BadResponseException $e) {
            $s = json_decode($e->getResponse()->getBody(true), true);

            print "********************************************\n";
            print "\tError response from server:\n";
            print "\tCode: {$s["code"]}\n";
            print "\tMessage: {$s["message"]}\n";
            print "\tServer: {$s["serverID"]}\n";
            print "********************************************\n";

            return "";
        }

        $res = $response->json();

        $request = $client->post("http://$host:6544/Channel/UpdateVideoSource")
            ->setPostField("SourceID", $res["VideoSource"]["Id"])
            ->setPostField("LineupId", $lineup);

        $response = $request->send();
    } else {
        $stmt = $dbh->prepare("UPDATE videosource SET lineupid=:lineup WHERE sourceid=:sid");
        $stmt->execute(array("lineup" => $lineup, "sid" => $sid));
    }

    return "";
}

function printLineup()
{
    global $dbhSD;

    /*
     * First we want to get the lineup that we're interested in.
     */

    list($lineup, $isDeleted) = getLineupFromNumber(strtoupper(readline("Lineup to print (# or lineup):>")));

    if ($lineup == "" OR $isDeleted) {
        return;
    }

    $stmt = $dbhSD->prepare("SELECT json FROM lineups WHERE lineup=:lineup");
    $stmt->execute(array("lineup" => $lineup));
    $response = json_decode($stmt->fetchColumn(), true);

    if (!count($response)) {
        return;
    }

    print "\n";

    $chanMap = array();
    $stationMap = array();

    if ($response["metadata"]["transport"] == "Antenna") {
        foreach ($response["map"] as $v) {
            if (isset($v["atscMajor"]) === true) {
                $chanMap[$v["stationID"]] = "{$v["atscMajor"]}.{$v["atscMinor"]}";
            } elseif (isset($v["uhfVhf"]) === true) {
                $chanMap[$v["stationID"]] = $v["uhfVhf"];
            } else {
                $chanMap[$v["stationID"]] = 0; //Not sure what the correct thing to use here is at this time.
            }
        }
    } else {
        foreach ($response["map"] as $v) {
            $chanMap[$v["stationID"]] = $v["channel"];
        }
    }

    foreach ($response["stations"] as $v) {
        if (isset($v["affiliate"]) === true) {
            $stationMap[$v["stationID"]] = "{$v["callsign"]} ({$v["affiliate"]})";
        } else {
            $stationMap[$v["stationID"]] = "{$v["callsign"]}";
        }
    }

    asort($chanMap, SORT_NATURAL);

    $stationData = new Zend\Text\Table\Table(array('columnWidths' => array(8, 50, 10)));
    $stationData->appendRow(array("Channel", "Callsign", "stationID"));

    foreach ($chanMap as $stationID => $channel) {
        $stationData->appendRow(array((string)$channel, $stationMap[$stationID], (string)$stationID));
    }

    print $stationData;
}

function addLineupsToSchedulesDirect()
{
    global $client;
    global $token;
    global $availableCountries;
    global $debug;

    $sdLineupArray = array();
    $countriesWithOnePostalCode = array();

    foreach ($availableCountries as $region => $foo) {
        foreach ($foo as $item) {
            if (isset($item["onePostalCode"]) === true) {
                $countriesWithOnePostalCode[$item["shortName"]] = $item["postalCodeExample"];
            }
        }
    }

    $done = false;
    $country = "";
    $postalCode = "";

    while ($done === false) {
        print "Three-character ISO-3166-1 alpha3 country code (? to list available countries):";
        $country = trim(strtoupper(readline(">")));

        if ($country == "") {
            return;
        }

        if ($country == "?") {
            printListOfAvailableCountries(true);
        } else {
            $done = true;
        }
    }

    if (isset($countriesWithOnePostalCode[$country]) === true) {
        $postalCode = $countriesWithOnePostalCode[$country];
        print "This country has only one postal code: $postalCode\n";
    } elseif ($country != "DVB") {
        print "Enter postal code:";
        $postalCode = strtoupper(readline(">"));

        if ($postalCode == "") {
            return;
        }
    }

    try {
        $response = $client->get("headends", array(), array(
            "query"   => array("country" => $country, "postalcode" => $postalCode),
            "headers" => array("token" => $token)
        ))->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        $s = json_decode($e->getResponse()->getBody(true), true);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug === true) {
        debugMSG("addLineupsToSchedulesDirect:Response: $response");
        debugMSG("addLineupsToSchedulesDirect:Response: " . print_r($res, true));
        debugMSG("Raw headers:\n" . $response->getRawHeaders());
    }

    $counter = "0";

    foreach ($res as $details) {
        print "\nheadend: {$details["headend"]}\n";
        print "location: {$details["location"]}\n";
        foreach ($details["lineups"] as $v) {
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

    if ($lineup == "") {
        return;
    }

    if (strlen($lineup) < 3) {
        $lineup = $sdLineupArray[$lineup];
    } else {
        $lineup = strtoupper($lineup);
    }

    if ($lineup != "USA-C-BAND-DEFAULT" AND substr_count($lineup, "-") != 2) {
        print "Did not see at least two hyphens in lineup; did you enter it correctly?\n";

        return;
    }

    print "Sending request to server.\n";
    $lineup = str_replace(" ", "", $lineup);

    try {
        $response = $client->put("lineups/$lineup", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        $s = json_decode($e->getResponse()->getBody(true), true);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug === true) {
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

    if ($lineup != "USA-C-BAND-DEFAULT" AND substr_count($lineup, "-") != 2) {
        print "Did not see at least two hyphens in lineup; did you enter it correctly?\n";

        return;
    }

    print "Sending request to server.\n";
    $lineup = str_replace(" ", "", $lineup);

    try {
        $response = $client->put("lineups/$lineup", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        $s = json_decode($e->getResponse()->getBody(true), true);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug === true) {
        debugMSG("addLineupsToSchedulesDirect:Response:$res");
        debugMSG("Raw headers:\n" . $response->getRawHeaders());
    }

    print "Message from server: {$res["message"]}\n";
}

function deleteLineupFromSchedulesDirect()
{
    global $dbh;
    global $dbhSD;
    global $debug;
    global $client;
    global $token;
    global $updatedLineupsToRefresh;

    $deleteFromLocalCache = $dbhSD->prepare("DELETE FROM lineups WHERE lineup=:lineup");
    $removeFromVideosource = $dbh->prepare("UPDATE videosource SET lineupid='' WHERE lineupid=:lineup");

    list($toDelete,) = getLineupFromNumber(strtoupper(readline("Lineup to Delete (# or lineup):>")));

    if ($toDelete == "") {
        return;
    }

    try {
        $response = $client->delete("lineups/$toDelete", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        $s = json_decode($e->getResponse()->getBody(true), true);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug === true) {
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

    try {
        $response = $client->delete("messages/$toDelete", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        $s = json_decode($e->getResponse()->getBody(true), true);
        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return;
    }

    $res = $response->json();

    if ($debug === true) {
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

    try {
        $response = $client->get("lineups/$lineupToGet", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        $s = json_decode($e->getResponse()->getBody(true), true);

        print "********************************************\n";
        print "\tError response from server:\n";
        print "\tCode: {$s["code"]}\n";
        print "\tMessage: {$s["message"]}\n";
        print "\tServer: {$s["serverID"]}\n";
        print "********************************************\n";

        return "";
    }

    try {
        $res = $response->json();
    } catch (Guzzle\Common\Exception\RuntimeException $e) {
        /*
         * Probably couldn't decode the JSON.
         */
        $message = $e->getMessage();
        print "Received error: $message\n";

        return "";
    }

    if ($debug === true) {
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

function storeLocalLineup($updatedLineupsToRefresh)
{
    global $dbhSD;
    global $useSQLite;
    global $noDB;

    if (($noDB === true) AND (is_null($dbhSD) === true)) {
        global $argv;
        print "Send the following to grabber@schedulesdirect.org\n";
        print "storeLocalLine->dbhSD is null and noDB is false?\n";
        var_dump($argv);
        exit;
    }

    print "Checking for updated lineups from Schedules Direct.\n";

    foreach ($updatedLineupsToRefresh as $lineupToGet => $v) {
        $fetchedLineup = array();
        $fetchedLineup = getLineup($lineupToGet);

        if (isset($fetchedLineup["code"]) === true) {
            print "\n\n-----\nERROR: Bad response from Schedules Direct.\n";
            print $fetchedLineup["message"] . "\n\n-----\n";
            exit;
        }

        /*
         * Store a copy of the data that we just downloaded into the cache.
         */
        if ($noDB === false) {
            if ($useSQLite === false) {
                $stmt = $dbhSD->prepare("INSERT INTO lineups(lineup,json,modified)
        VALUES(:lineup,:json,:modified) ON DUPLICATE KEY UPDATE json=:json,modified=:modified");

                $stmt->execute(array(
                    "lineup"   => $lineupToGet,
                    "modified" => $updatedLineupsToRefresh[$lineupToGet],
                    "json"     => json_encode($fetchedLineup)
                ));
            } else {
                $stmt = $dbhSD->prepare("INSERT OR IGNORE INTO lineups(lineup,json,modified)
        VALUES(:lineup,:json,:modified)");

                $stmt->execute(array(
                    "lineup"   => $lineupToGet,
                    "modified" => $updatedLineupsToRefresh[$lineupToGet],
                    "json"     => json_encode($fetchedLineup)
                ));

                $stmt = $dbhSD->prepare("UPDATE lineups SET json=:json,modified=:modified WHERE lineup=:lineup");
                $stmt->execute(array(
                    "lineup"   => $lineupToGet,
                    "modified" => $updatedLineupsToRefresh[$lineupToGet],
                    "json"     => json_encode($fetchedLineup)
                ));
            }
        } else {
            var_dump($fetchedLineup); // Temp
        }

        unset ($updatedLineupsToRefresh[$lineupToGet]);
    }
}

function tempdir()
{
    $tempfile = tempnam(sys_get_temp_dir(), "mfdb");
    if (file_exists($tempfile) === true) {
        unlink($tempfile);
    }
    mkdir($tempfile);
    if (is_dir($tempfile) === true) {
        print "tempdir is $tempfile\n";

        return $tempfile;
    }
}

function displayLocalVideoSources()
{
    global $dbh;
    global $useServiceAPI;
    global $client;
    global $host;

    $result = array();

//    if ($useServiceAPI === true) {
//        try {
//            $response = $client->get("http://$host:6544/Channel/GetVideoSource", array(), array(
//                "query"   => array("SourceID" => $sid),
//                "headers" => array("Accept" => "application/json")
//            ))->send();
//        } catch (Guzzle\Http\Exception\BadResponseException $e) {
//            $s = json_decode($e->getResponse()->getBody(true), true);
//
//            print "********************************************\n";
//            print "\tError response from server:\n";
//            print "\tCode: {$s["code"]}\n";
//            print "\tMessage: {$s["message"]}\n";
//            print "\tServer: {$s["serverID"]}\n";
//            print "********************************************\n";
//        }
//    } else {
    $stmt = $dbh->prepare("SELECT sourceid,name,lineupid FROM videosource ORDER BY sourceid");
    $stmt->execute();
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
//    }

    if (count($result) != 0) {
        print "\nMythTV local videosources:\n";
        foreach ($result as $v) {
            print "sourceid: " . $v["sourceid"] . "\tname: " . $v["name"] . "\tSchedules Direct lineup: " .
                $v["lineupid"] . "\n";
        }
    } else {
        print "\nWARNING: *** No videosources configured in MythTV. ***\n";
    }
}

function createDatabase($useSQLite)
{
    global $dbhSD;
    $createBaseTables = false;
    global $schemaVersion;

    printMSG("Creating settings table.");

    if ($useSQLite === true) {
        printMSG("Creating initial database.");
        $dbhSD->exec(
            "CREATE TABLE settings
          (
            keyColumn TEXT NOT NULL UNIQUE,
            valueColumn TEXT NOT NULL
          )");
        print "Creating Schedules Direct tables.\n";
        $dbhSD->exec("CREATE TABLE messages
                  (
                    row INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    id char(22) NOT NULL UNIQUE, -- Required to ACK a message from the server.
                    date char(20) DEFAULT NULL,
                    message varchar(512) DEFAULT NULL,
                    type char(1) DEFAULT NULL, -- Message type. G-global, S-service status, U-user specific
                    modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
  )");
        $dbhSD->exec("CREATE TABLE credits
                  (
                    personID INTEGER,
                    programID varchar(64) NOT NULL,
                    role varchar(100) DEFAULT NULL
                  )");
        $dbhSD->exec("CREATE INDEX programID ON credits (programID)");
        $dbhSD->exec("CREATE UNIQUE INDEX person_pid_role ON credits (personID,programID,role)");
        $dbhSD->exec("CREATE TABLE lineups
                  (
                    row INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                    lineup varchar(50) NOT NULL,
                    modified char(20) DEFAULT '1970-01-01T00:00:00Z',
                    json TEXT
                  )");
        $dbhSD->exec("CREATE UNIQUE INDEX lineup ON lineups (lineup)");
        $dbhSD->exec("CREATE TABLE people
                  (
                    personID INTEGER PRIMARY KEY,
                    name varchar(128)
                  )");
        $dbhSD->exec("CREATE TABLE programs
                  (
                    row INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                    programID varchar(64) NOT NULL UNIQUE,
                    md5 char(22) NOT NULL,
                    modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    json TEXT NOT NULL
                  )");
        $dbhSD->exec("CREATE TABLE programGenres
                  (
                    programID varchar(64) PRIMARY KEY NOT NULL,
                    relevance char(1) NOT NULL DEFAULT '0',
                    genre varchar(30) NOT NULL
                  )");
        $dbhSD->exec("CREATE INDEX genre ON programGenres (genre)");
        $dbhSD->exec("CREATE UNIQUE INDEX pid_relevance ON programGenres (programID,relevance)");
        $dbhSD->exec("CREATE TABLE programRatings
                  (
                    programID varchar(64) PRIMARY KEY NOT NULL,
                    system varchar(30) NOT NULL,
                    rating varchar(16) DEFAULT NULL
                  )");
        $dbhSD->exec("CREATE TABLE schedules
                  (
                    stationID varchar(12) NOT NULL UNIQUE,
                    md5 char(22) NOT NULL
                  )");
        $dbhSD->exec("CREATE INDEX md5 ON schedules (md5)");
        $dbhSD->exec("CREATE TABLE imageCache
                  (
                    row INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
                    item varchar(128) NOT NULL,
                    md5 char(22) NOT NULL,
                    height varchar(128) NOT NULL,
                    width varchar(128) NOT NULL,
                    type char(1) NOT NULL -- COMMENT 'L-Channel Logo'
                  )");
        $dbhSD->exec("CREATE UNIQUE INDEX id ON imageCache (item,height,width)");
        $dbhSD->exec("CREATE INDEX type ON imageCache (type)");
    } else {
        printMSG("Creating Schedules Direct tables.");

        $dbhSD->exec("DROP TABLE IF EXISTS settings,messages,credits,lineups,programs,people,programGenres,
    programRating,schedules,imageCache");

        $dbhSD->exec(
            "CREATE TABLE `settings` (
                    `keyColumn` varchar(255) NOT NULL,
                    `valueColumn` varchar(255) NOT NULL,
                    PRIMARY KEY (`keyColumn`),
                    UNIQUE KEY `keyColumn` (`keyColumn`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `messages` (
`row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `id` char(22) DEFAULT NULL COMMENT 'Required to ACK a message from the server.',
  `date` char(20) DEFAULT NULL,
  `message` varchar(512) DEFAULT NULL,
  `type` char(1) DEFAULT NULL COMMENT 'Message type. G-global, S-service status, U-user specific',
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`row`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `credits` (
  `personID` mediumint(8) unsigned NOT NULL DEFAULT '0',
  `programID` varchar(64) NOT NULL,
  `role` varchar(100) DEFAULT NULL,
  KEY `personID` (`personID`),
  KEY `programID` (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `lineups` (
  `row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `lineup` varchar(50) NOT NULL DEFAULT '',
  `md5` char(22) NOT NULL,
  `modified` char(20) DEFAULT '1970-01-01T00:00:00Z',
  `json` mediumtext,
  PRIMARY KEY (`row`),
  UNIQUE KEY `lineup` (`lineup`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `people` (
  `personID` mediumint(8) unsigned NOT NULL,
  `name` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',
  `nameID` mediumint(8) unsigned DEFAULT NULL,
  UNIQUE KEY `personID` (`personID`),
  KEY `name` (`name`),
  KEY `nameID` (`nameID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `programs` (
  `row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `programID` varchar(64) NOT NULL,
  `md5` char(22) NOT NULL,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `json` varchar(16384) NOT NULL,
  PRIMARY KEY (`row`),
  UNIQUE KEY `pid` (`programID`),
  KEY `programID` (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `programGenres` (
  `programID` varchar(64) NOT NULL,
  `relevance` char(1) NOT NULL DEFAULT '0',
  `genre` varchar(30) NOT NULL,
  PRIMARY KEY (`programID`),
  UNIQUE KEY `pid_relevance` (`programID`,`relevance`),
  KEY `genre` (`genre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `programRating` (
  `programID` varchar(64) NOT NULL,
  `system` varchar(30) NOT NULL,
  `rating` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `schedules` (
  `stationID` varchar(12) NOT NULL,
  `md5` char(22) NOT NULL,
  `date` CHAR(10) NOT NULL,
  UNIQUE KEY `station_date` (`stationID`,`date`),
  KEY `md5` (`md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8");

        $dbhSD->exec("CREATE TABLE `imageCache` (
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

        settingSD("SchedulesDirectJSONschemaVersion", "4");
    }
}

function checkForSchemaUpdate($useSQLite)
{
    global $dbhSD;
    global $schemaVersion;
    printMSG("Checking for schema updates.");
    $dbSchemaVersion = settingSD("SchedulesDirectJSONschemaVersion");
    printMSG("Database schema version is $dbSchemaVersion.");
    if ($dbSchemaVersion == $schemaVersion) {
        return;
    }

    $stmt = $dbhSD->prepare("DESCRIBE settings");
    try {
        $stmt->execute();
    } catch (PDOException $ex) {
        if ($ex->getCode() == "42S02") {
        }
    }

    if ($dbSchemaVersion == "1") {
        $dbhSD->exec("ALTER TABLE people ADD INDEX(name)");
        settingSD("SchedulesDirectJSONschemaVersion", "2");
    }

    if ($dbSchemaVersion == "2") {
        try {
            $dbhSD->exec("RENAME TABLE SDMessages TO messages");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDcredits TO credits");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDimageCache TO imageCache");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDlineupCache TO lineups");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDpeople TO people");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDprogramCache TO programs");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDprogramgenres TO programGenres");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDprogramrating TO programRatings");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("RENAME TABLE SDschedule TO schedules");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("ALTER TABLE schedules ADD column date CHAR(10) NOT NULL");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("ALTER TABLE schedules DROP KEY sid");
        } catch (PDOException $ex) {
            //do nothing
        }
        try {
            $dbhSD->exec("ALTER TABLE schedules ADD UNIQUE KEY station_date (stationID,date)");
        } catch (PDOException $ex) {
            //do nothing
        }
        settingSD("SchedulesDirectJSONschemaVersion", "3");
    }

    if ($dbSchemaVersion == "3") {
        $dbhSD->exec("ALTER TABLE people DROP PRIMARY KEY");
        $dbhSD->exec("ALTER TABLE people ADD UNIQUE KEY(personID)");
        $dbhSD->exec("ALTER TABLE people ADD COLUMN nameID mediumint unsigned");
        $dbhSD->exec("ALTER TABLE people ADD INDEX(nameID)");
        settingSD("SchedulesDirectJSONschemaVersion", "4");
    }
}

function checkForChannelIcon($stationID, $data)
{
    global $dbh;
    global $dbhSD;
    global $channelLogoDirectory;
    global $forceLogoUpdate;
    global $useSQLite;

    $a = explode("/", $data["URL"]);
    $iconFileName = end($a);

    $md5 = $data["md5"];
    $height = $data["height"];
    $width = $data["width"];

    $updateChannelTable = $dbh->prepare("UPDATE channel SET icon=:icon WHERE xmltvid=:stationID");

    $stmt = $dbhSD->prepare("SELECT md5 FROM imageCache WHERE item=:item AND height=:height AND width=:width");
    $stmt->execute(array("item" => $iconFileName, "height" => $height, "width" => $width));

    $result = $stmt->fetchColumn();

    if (($result === false) OR ($result != $md5) OR ($forceLogoUpdate === true)) {
        /*
         * We don't already have this icon, or it's different, so it will have to be fetched.
         */

        printMSG("Fetching logo $iconFileName for station $stationID");

        $success = @file_put_contents("$channelLogoDirectory/$iconFileName", file_get_contents($data["URL"]));

        if ($success === false) {
            printMSG("Check permissions: could not write to $channelLogoDirectory\n");

            return;
        }

        if ($useSQLite === false) {
            $updateSDimageCache = $dbhSD->prepare("INSERT INTO imageCache(item,height,width,md5,type)
        VALUES(:item,:height,:width,:md5,'L') ON DUPLICATE KEY UPDATE md5=:md5");
            $updateSDimageCache->execute(array(
                "item"   => $iconFileName,
                "height" => $height,
                "width"  => $width,
                "md5"    => $md5
            ));
        } else {
            $updateSDimageCache = $dbhSD->prepare("INSERT OR IGNORE INTO imageCache(item,height,width,md5,type)
        VALUES(:item,:height,:width,:md5,'L')");
            $updateSDimageCache->execute(array(
                "item"   => $iconFileName,
                "height" => $height,
                "width"  => $width,
                "md5"    => $md5
            ));

            $updateSDimageCache = $dbhSD->prepare("UPDATE imageCache SET
            item=:item,height=:height,width=:width,md5=:md5,type='L' WHERE md5=:md5");
            $updateSDimageCache->execute(array(
                "item"   => $iconFileName,
                "height" => $height,
                "width"  => $width,
                "md5"    => $md5
            ));
        }

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
    $extractChannel = array();
    $extractMultiplex = array();

    $stmt = $dbh->prepare("SELECT lineupid from videosource where sourceid=:sid");
    $stmt->execute(array("sid" => $sourceIDtoExtract));
    $lineupName = $stmt->fetchColumn();

    $stmt = $dbh->prepare("SELECT channum, freqid, callsign, name, xmltvid, mplexid, serviceid, atsc_major_chan,
atsc_minor_chan FROM channel where sourceid=:sid");
    $stmt->execute(array("sid" => $sourceIDtoExtract));
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    $getDTVMultiplex = $dbh->prepare("SELECT * FROM dtv_multiplex WHERE mplexid=:mplexid");

    if (count($result) == 0) {
        print "Channel table is empty; nothing to do.\n";

        return;
    }

    $fhExtract = fopen("$lineupName.extract.conf", "w");

    foreach ($result as $v) {
        $getDTVMultiplex->execute(array("mplexid" => $v["mplexid"]));
        $dtv = $getDTVMultiplex->fetch(PDO::FETCH_ASSOC);

        $extractChannel[] = array(
            "channel"        => $v["channum"],
            "virtualChannel" => $v["freqid"],
            "callsign"       => $v["callsign"],
            "name"           => $v["name"],
            "mplexID"        => $v["mplexid"],
            "stationID"      => $v["xmltvid"],
            "serviceID"      => $v["serviceid"],
            "atscMajor"      => $v["atsc_major_chan"],
            "atscMinor"      => $v["atsc_minor_chan"]
        );

        $extractMultiplex[$v["mplexid"]] = array(
            "transportID" => $dtv["transportid"],
            "networkID"   => $dtv["networkid"],
            "frequency"   => $dtv["frequency"],
            "symbolRate"  => $dtv["symbolrate"],
            "fec"         => $dtv["fec"],
            "polarity"    => $dtv["polarity"],
            "modulation"  => strtoupper(
                str_replace("_", "", $dtv["modulation"])),
            "transport"   => $dtv["mod_sys"]
        );
    }

    $extractArray["version"] = "0.09";
    $extractArray["date"] = $todayDate;
    $extractArray["lineup"] = $lineupName;
    $extractArray["channel"] = $extractChannel;
    $extractArray["multiplex"] = $extractMultiplex;

    $json = json_encode($extractArray);

    fwrite($fhExtract, "$json\n");

    /*
     * TODO: send json automatically.
     */

    fclose($fhExtract);

    print "Please send the $lineupName.extract.conf to qam-info@schedulesdirect.org\n";
}

function getLineupFromNumber($numberOrLineup)
{
    global $sdStatus;

    if (strlen($numberOrLineup) < 3) {
        $numberOrLineup = (int)$numberOrLineup;

        if (isset($sdStatus["lineups"][$numberOrLineup]) === false) {
            return array("", "");
        } else {
            if (isset($sdStatus["lineups"][$numberOrLineup]["isDeleted"]) === true) {
                return array($sdStatus["lineups"][$numberOrLineup]["lineup"], true);
            } else {
                return array($sdStatus["lineups"][$numberOrLineup]["lineup"], false);
            }
        }
    } else {
        return ($numberOrLineup); //We're actually just returning the name of the array.
    }
}

function getListOfAvailableCountries()
{
    global $client;

    try {
        $response = $client->get("/20141201/available/countries", array(), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        if ($e->getCode() != 200) {
            print "Could not download list of countries. Exiting.\n";
            exit;
        }
    }

    $res = $response->json();

    return $res;

}

function printListOfAvailableCountries($fancyTable)
{
    global $availableCountries;

    foreach ($availableCountries as $region => $data) {
        print "\nRegion:$region\n";

        $countryWidth = 0;
        foreach ($data as $item) {
            if (strlen($item["fullName"]) > $countryWidth) {
                $countryWidth = strlen($item["fullName"]);
            }
        }

        if ($fancyTable === true) {
            $col1 = "Country";
            $col2 = "Three-letter code";
            $col3 = "Example Postal Code";
            $countryList = new Zend\Text\Table\Table(array(
                'columnWidths' => array(
                    $countryWidth + 2,
                    strlen($col2) + 2,
                    strlen($col3) + 2
                )
            ));
            $countryList->appendRow(array($col1, $col2, $col3));

            foreach ($data as $item) {
                if (isset($item["onePostalCode"]) === true) {
                    $countryList->appendRow(array(
                        $item["fullName"],
                        $item["shortName"],
                        $item["postalCodeExample"] . " *"
                    ));
                } else {
                    $countryList->appendRow(array($item["fullName"], $item["shortName"], $item["postalCodeExample"]));
                }
            }
            print $countryList;
        } else {
            foreach ($data as $item) {
                print "{$item["fullName"]}\n";
            }
        }
    }

    print "\n* The only valid postal code for this country.\n";
}

function getChannelLogoDirectory()
{
    if (is_dir(getenv("HOME") . "/.mythtv/channels") === true) {
        return (getenv("HOME") . "/.mythtv/channels");
    }

    if (is_dir("/home/mythtv/.mythtv/channels") === true) {
        return ("/home/mythtv/.mythtv/channels");
    }

    return ("UNKNOWN");

}

?>
