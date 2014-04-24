<?php
/**
 * Created by PhpStorm.
 * User: Bob
 * Date: 2/16/14
 * Time: 2:22 AM
 */

function getToken($username, $passwordHash)
{
    global $client;
    global $debug;

    $body = json_encode(array("username" => $username, "password" => $passwordHash));

    if ($debug)
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
    } catch (Exception $e)
    {
        print "getToken:HCF. Uncaught exception.\n";

        return ("ERROR");
    }

    $res = array();
    $res = $response->json();

    if (json_last_error() != 0)
    {
        print "JSON decode error:\n";
        var_dump($response);
        exit;
    }

    if ($debug)
    {
        print "\n\n******************************************\n";
        print "Raw headers:\n";
        print $response->getRawHeaders();
        print "******************************************\n";
        print "getToken:Response:$res\n";
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

    if ($debug)
    {
        print "\n\n******************************************\n";
        print "Raw headers:\n";
        print $response->getRawHeaders();
        print "******************************************\n";
        print "getStatus:Response:$res\n";
        print "******************************************\n";
    }

    return $res;
}

function printStatus($sdStatus)
{
    global $dbh;
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

    $getLocalCacheModified = $dbh->prepare("SELECT modified FROM SDheadendCache WHERE lineup=:he");
    $getVideosourceModified = $dbh->prepare("SELECT modified FROM videosource WHERE lineupid=:he");

    $he = getSchedulesDirectLineups();

    if (count($he))
    {
        print "The following lineups are in your account at Schedules Direct:\n\n";

        $lineupData = new Zend\Text\Table\Table(array('columnWidths' => array(20, 20, 25, 4)));
        $lineupData->appendRow(array("Lineup", "Server modified", "MythTV videosource update", "New?"));

        foreach ($he as $id => $serverModified)
        {
            $mythStatus = array();
            $mythModified = "";

            $getVideosourceModified->execute(array("he" => $id));
            $mythStatus = $getVideosourceModified->fetchAll(PDO::FETCH_COLUMN);

            if (count($mythStatus))
            {
                $mythModified = $mythStatus[0];
            }

            if ($serverModified > $mythModified)
            {
                $updatedHeadendsToRefresh[$id] = $serverModified;
                $lineupData->appendRow(array($id, $serverModified, $mythModified, "***"));
            }
            else
            {
                $lineupData->appendRow(array($id, $serverModified, $mythModified, ""));
            }
        }

        print $lineupData;

        if (count($updatedHeadendsToRefresh))
        {
            updateLocalLineupCache($updatedHeadendsToRefresh);
        }
    }
    else
    {
        print "\nWARNING: *** No lineups configured at Schedules Direct. ***\n";
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
    global $useServiceAPI;

    print "Checking for MythTV Service API.\n";

    try
    {
        $request = $client->get("http://$host:6544/Myth/GetHostName")->send();
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
    } catch (Exception $e)
    {
        print "getToken:HCF. Uncaught exception.\n";

        return ("ERROR");
    }

    print "Found Service API.\n";
    $useServiceAPI = TRUE;
}

function debugMSG($str)
{
    global $fh_error;

    $str = date("H:i:s") . ":$str";

    print "$str\n";
    fwrite($fh_error, "$str\n");
}

function printMSG($str)
{
    global $fh_log;
    global $quiet;
    global $printTimeStamp;

    if ($printTimeStamp)
    {
        $str = date("H:i:s") . ":$str";
    }

    if (!$quiet)
    {
        print "$str";
    }

    $str = str_replace("\r", "\n", $str);
    fwrite($fh_log, $str);
}


?>