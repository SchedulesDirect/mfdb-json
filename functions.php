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

    $body = json_encode(array("username" => $username, "password" => $passwordHash));

    try {
        $response = $client->post("token", array(), $body)->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        if ($e->getCode() == 400)
        {
            return("ERROR");
        }
    }

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

function getStatus()
{
    global $token;
    global $client;
    global $sdStatus;

    try {
        $response = $client->get("status", array("token" => $token), array())->send();
    } catch (Guzzle\Http\Exception\BadResponseException $e) {
        if ($e->getCode() != 200)
        {
            return("ERROR");
        }
    }

    $sdStatus = $response->json();

    return $sdStatus;
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

    $getLocalModified = $dbh->prepare("SELECT modified FROM SDheadendCache WHERE lineup=:he");

    $he = getSchedulesDirectLineups();

    if (count($he))
    {
        print "The following lineups are in your account at Schedules Direct:\n\n";

        $lineupData = new Zend\Text\Table\Table(array('columnWidths' => array(15, 35, 35,4)));
        $lineupData->appendRow(array("Lineup", "Local cache update", "MythTV videosource update","New?"));

        foreach ($he as $id => $modified)
        {
            $getLocalModified->execute(array("he" => $id));
            $sdStatus = $getLocalModified->fetchAll(PDO::FETCH_COLUMN);

            if ((count($sdStatus) == 0) OR ($sdStatus[0] < $modified))
            {
                $updatedHeadendsToRefresh[$id] = $modified;
                $lineupData->appendRow(array($id, $modified, "***"));
            }
            else
            {
                $lineupData->appendRow(array($id, $modified, ""));
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

?>