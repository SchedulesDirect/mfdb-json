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

function getStatus()
{
    global $token;
    global $client;
    global $sdStatus;

    $request = $client->get("status", array("token" => $token), array());
    $response = $request->send();
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

        $lineupData = new Zend\Text\Table\Table(array('columnWidths' => array(15, 20, 25)));
        $lineupData->appendRow(array("Lineup", "Last updated", "Update Available?"));

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
}

?>