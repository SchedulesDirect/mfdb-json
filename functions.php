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

?>