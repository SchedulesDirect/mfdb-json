<?php
/*
functions.php * This file contains commonly used functions between the various programs.
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

$scriptVersion = "0.25";
$scriptDate = "2015-02-28";
$knownToBeBroken = FALSE;
$skipVersionCheck = TRUE;

function getToken($username, $passwordHash)
{
    global $client;
    global $debug;

    $body = json_encode(array("username" => $username, "password" => $passwordHash));

    if ($debug === TRUE)
    {
        print "getToken: Sending $body\n";
    }

    try
    {
        $response = $client->post("token", array(), $body)->send();
    } catch (Guzzle\Http\Exception\ClientErrorResponseException $e)
    {
        $errorReq = $e->getRequest();
        $errorResp = $e->getResponse();
        $errorMessage = $e->getMessage();
        exceptionErrorDump($errorReq, $errorResp, $errorMessage);

        return ("ERROR");
    } catch (Guzzle\Http\Exception\ServerErrorResponseException $e)
    {
        $errorReq = $e->getRequest();
        $errorResp = $e->getResponse();
        $errorMessage = $e->getMessage();
        exceptionErrorDump($errorReq, $errorResp, $errorMessage);

        return ("ERROR");
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        $errorReq = $e->getRequest();
        $errorResp = $e->getResponse();
        $errorMessage = $e->getMessage();
        exceptionErrorDump($errorReq, $errorResp, $errorMessage);

        return ("ERROR");
    } catch (Guzzle\HTTP\Exception\RequestException $e)
    {
        $errorReq = $e->getRequest();
        $errorResp = $e->getResponse();
        $errorMessage = $e->getMessage();
        exceptionErrorDump($errorReq, $errorResp, $errorMessage);

        return ("ERROR");
    } catch (Exception $e)
    {
        print "getToken:HCF. Uncaught exception.\n";
        print $e->getMessage() . "\n";

        print "e is \n";
        var_dump($e);

        return ("ERROR");
    }

    $res = array();
    $res = $response->json();

    if (json_last_error())
    {
        print "JSON decode error:\n";
        var_dump($response);
        exit;
    }

    if ($debug === TRUE)
    {
        print "\n\n******************************************\n";
        print "Raw headers:\n";
        print $response->getRawHeaders();
        print "******************************************\n";
        print "getToken:Response:\n";
        var_dump($res);
        print "\n\n";
        print "******************************************\n";
    }

    if ($res["code"] == 0)
    {
        return $res["token"];
    }

    print "Response: {$res["response"]}\n";
    print "code: {$res["code"]}\n";
    print "serverID: {$res["serverID"]}\n";
    print "message: {$res["message"]}\n";

    return "ERROR";
}

function getStatus()
{
    global $debug;
    global $token;
    global $client;

    try
    {
        $response = $client->get("status", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        if ($e->getCode() != 200)
        {
            return ("ERROR");
        }
    }

    $res = $response->json();

    if ($debug === TRUE)
    {
        print "\n\n******************************************\n";
        print "Raw headers:\n";
        print $response->getRawHeaders();
        print "******************************************\n";
        print "getStatus:Response:\n";
        var_dump($res);
        print "\n\n";
        print "******************************************\n";
    }

    return $res;
}

function printStatus($sdStatus)
{
    global $updatedLineupsToRefresh;
    global $isMythTV;
    global $debug;
    global $printFancyTable;

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

    $videosourceModifiedArray = array();

    if ($isMythTV)
    {
        $videosourceModifiedJSON = settingSD("localLineupLastModified");

        if ($videosourceModifiedJSON)
        {
            $videosourceModifiedArray = json_decode($videosourceModifiedJSON, TRUE);
        }
    }

    if ($debug === TRUE)
    {
        print "isMythTV status is:\n";
        var_dump($isMythTV);
        print "\n\nLineup array is:\n";
        var_dump($sdStatus);
    }

    if (count($sdStatus["lineups"]) == 0)
    {
        print "\nWARNING: *** No lineups configured at Schedules Direct. ***\n";

        return;
    }

    print "The following lineups are in your account at Schedules Direct:\n\n";

    if ($isMythTV)
    {
        if ($printFancyTable)
        {
            $lineupData = new Zend\Text\Table\Table(array('columnWidths' => array(6, 20, 20, 25, 7)));
            $lineupData->appendRow(array("Number", "Lineup", "Server modified", "MythTV videosource update",
                                         "Status"));
        }
        else
        {
            print "Number\tLineup\tServer modified\tMythTV videosource update\tStatus\n";
        }
    }
    else
    {
        if ($printFancyTable)
        {
            $lineupData = new Zend\Text\Table\Table(array('columnWidths' => array(6, 20, 20)));
            $lineupData->appendRow(array("Number", "Lineup", "Server modified"));
        }
        else
        {
            print "Number\tLineup\tServer modified\n";
        }
    }

    if ($debug === TRUE)
    {
        print "Temp printout.\n";
        print $lineupData;
    }

    foreach ($sdStatus["lineups"] as $lineupNumber => $v)
    {
        $lineup = $v["ID"];
        $serverModified = $v["modified"];
        if (isset($v["isDeleted"]))
        {
            $lineupIsDeleted = TRUE;
        }
        else
        {
            $lineupIsDeleted = FALSE;
        }

        if ($debug === TRUE)
        {
            print "Raw lineup:\n";
            var_dump($v);
        }

        if ($isMythTV)
        {
            if (count($videosourceModifiedArray))
            {
                if (array_key_exists($lineup, $videosourceModifiedArray))
                {
                    $mythModified = $videosourceModifiedArray[$lineup];
                }
                else
                {
                    $mythModified = "";
                }
            }
            else
            {
                $mythModified = "";
            }

            if ($serverModified > $mythModified)
            {
                if ($lineupIsDeleted === FALSE)
                {
                    $updatedLineupsToRefresh[$lineup] = $serverModified;
                }

                if ($printFancyTable)
                {
                    if ($lineupIsDeleted === FALSE)
                    {
                        $lineupData->appendRow(array("$lineupNumber", $lineup, $serverModified, $mythModified,
                                                     "Updated"));
                    }
                    else
                    {
                        $lineupData->appendRow(array("$lineupNumber", $lineup, $serverModified, $mythModified,
                                                     "DELETED"));
                    }
                    continue;
                }
                else
                {
                    print "$lineupNumber\t$lineup\t$serverModified\n";
                }
            }

            if ($printFancyTable)
            {
                $lineupData->appendRow(array("$lineupNumber", $lineup, $serverModified, $mythModified, ""));
            }
            else
            {
                print "$lineupNumber\t$lineup\t$serverModified\t$mythModified\n";
            }
        }
        else
        {
            if ($printFancyTable)
            {
                $lineupData->appendRow(array("$lineupNumber", $lineup, $serverModified));
            }
            else
            {
                print "$lineupNumber\t$lineup\t$serverModified\n";
            }
            if ($lineupIsDeleted === FALSE)
            {
                $updatedLineupsToRefresh[$lineup] = $serverModified;
            }
        }
    }

    if ($printFancyTable)
    {
        print $lineupData;
    }

    if (count($updatedLineupsToRefresh))
    {
        updateLocalLineupCache($updatedLineupsToRefresh);
    }
}

function exceptionErrorDump($errorReq, $errorResp, $errorMessage)
{
    print "errorReq\n";
    print "$errorReq\n\n";

    print "errorResp\n";
    print "$errorResp\n\n";

    print "errorMessage\n";
    print "$errorMessage\n\n";
}

function checkForServiceAPI()
{
    global $client;
    global $host;

    printMSG("Checking for MythTV Service API.");

    try
    {
        $request = $client->get("http://$host:6544/Myth/GetHostName")->send();
    } catch (Guzzle\Http\Exception\ClientErrorResponseException $e)
    {
        return (FALSE);
    } catch (Guzzle\Http\Exception\ServerErrorResponseException $e)
    {
        return (FALSE);
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        return (FALSE);
    } catch (Exception $e)
    {
        return (FALSE);
    }

    printMSG("Found Service API.");

    return (TRUE);
}

function debugMSG($str)
{
    global $fh_error;
    global $quiet;

    $str = date("Y-m-d H:i:s") . ":$str";

    if (!$quiet)
    {
        print "$str\n";
    }

    $str = str_replace("\r", "\n", $str);
    fwrite($fh_error, "$str\n");
}

function printMSG($str)
{
    global $fh_log;
    global $quiet;
    global $printTimeStamp;

    if ($printTimeStamp)
    {
        $str = date("Y-m-d H:i:s") . ":$str";
    }

    if (!$quiet)
    {
        print "$str\n";
    }

    $str = str_replace("\r", "\n", $str);
    fwrite($fh_log, "$str\n");
}

function setting()
{
    /*
     * If there is one argument, then we're reading from the database. If there are two,
     * then we're writing to the database.
     */

    global $dbh;

    $key = func_get_arg(0);

    if (func_num_args() == 1)
    {
        $stmt = $dbh->prepare("SELECT data FROM settings WHERE value = :key");
        $stmt->execute(array("key" => $key));
        $result = $stmt->fetchColumn();

        if ($result === FALSE)
        {
            return FALSE;
        }
        else
        {
            return $result;
        }
    }

    $value = func_get_arg(1);

    $keyAlreadyExists = setting($key);

    if ($keyAlreadyExists === FALSE)
    {
        $stmt = $dbh->prepare("INSERT INTO settings(value,data) VALUES(:key,:value)");
    }
    else
    {
        $stmt = $dbh->prepare("UPDATE settings SET data=:value WHERE value=:key");
        /*
         * This would be a whole lot less obtuse if settings table had two columns:
         * "keyColumn" and "valueColumn".
         */
    }

    $stmt->execute(array("key" => $key, "value" => $value));

    return;
}

function settingSD()
{
    /*
     * If there is one argument, then we're reading from the database. If there are two,
     * then we're writing to the database.
     */

    global $dbhSD;

    $key = func_get_arg(0);

    if (func_num_args() == 1)
    {
        $stmt = $dbhSD->prepare("SELECT valueColumn FROM settings WHERE keyColumn = :key");
        $stmt->execute(array("key" => $key));
        $result = $stmt->fetchColumn();
        if ($result === FALSE)
        {
            return FALSE;
        }
        else
        {
            return $result;
        }
    }

    $value = func_get_arg(1);

    $stmt = $dbhSD->prepare("INSERT INTO settings(keyColumn,valueColumn) VALUES(:key,:value)
    ON DUPLICATE KEY UPDATE valueColumn=:value");

    $stmt->execute(array("key" => $key, "value" => $value));

}

function getBaseurl($isBeta)
{
    global $api;

    if ($isBeta)
    {
        # Test server. Things may be broken there.
        $baseurl = "https://json.schedulesdirect.org/20141201/";
        print "Using beta server.\n";
        $api = 20141201;
    }
    else
    {
        $baseurl = "https://json.schedulesdirect.org/20140530/";
        print "Using production server.\n";
        $api = 20140530;
    }

    return ($baseurl);
}

function checkForClientUpdate($client)
{
    try
    {
        $response = $client->get("version/mfdb-json", array(), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e)
    {
        if ($e->getCode() != 200)
        {
            return (array(TRUE, "ERROR"));
        }
    }

    $res = $response->json();

    if ($res["code"] == 0)
    {
        return (array(FALSE, $res["version"]));
    }

    if ($res["code"] == 1005)
    {
        print "Got error message from server: unknown client.\n";

        return (array(1005, ""));
    }
}

function getLoginFromFiles()
{
    $etcFile = file_exists("/etc/mythtv/config.xml");
    $localFile = file_exists(getenv("HOME") . "/.mythtv/config.xml");

    if ($localFile)
    {
        printMSG("Using database information from ~/.mythtv/config.xml");
        $xml = simplexml_load_file(getenv("HOME") . "/.mythtv/config.xml");
    }

    /*
     * We want to use the file in the local directory first if it exists.
     */

    if (!isset($xml) AND $etcFile)
    {
        printMSG("Using database information from /etc/mythtv/config.xml");
        $xml = simplexml_load_file("/etc/mythtv/config.xml");
    }

    if (!isset($xml))
    {
        return (array("NONE", "", "", ""));
    }

    /*
     * xml to array.
     */

    $foo = json_decode(json_encode($xml), TRUE);

    if (count($foo))
    {
        if (isset($foo["Database"]))
        {
            return array($foo["Database"]["Host"], $foo["Database"]["DatabaseName"], $foo["Database"]["UserName"],
                         $foo["Database"]["Password"]);
        }
        else
        {
            printMSG("Fatal: couldn't parse XML to JSON.");
            printMSG("Open ticket with grabber@schedulesdirect.org and send the following:");
            var_dump($foo);
            printMSG(print_r($foo, TRUE));
            exit;
        }
    }
}

function checkForOS()
{
    if (PHP_OS == "WINNT")
    {
        function readline($string)
        {
            echo $string;
            $handle = fopen("php://stdin", "r");
            $line = fgets($handle);

            return trim($line);
        }
    }
}

function getMplexID($frequency, $polarization, $modulation, $networkid, $transportid)
{
    global $dbh;
    $stmt = $dbh->prepare("SELECT mplexid FROM dtv_multiplex WHERE
      frequency=:frequency AND
      polarity=:polarization AND
      modulation=:modulation AND
      networkid=:networkID AND
      transportid=:transportID
      ");
    $stmt->execute(array("frequency" => $frequency, "polarization" => $polarization, "modulation" => $modulation,
                         "networkID" => $networkid, "transportID" => $transportid));

    $stmt->fetchColumn();
}