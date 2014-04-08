#mfdb-json

mythfilldatabase grabber for the Schedules Direct JSON service.
v0.07, 2014-04-05
Robert Kulagowski
grabber@schedulesdirect.org

This program runs as a replacement to mythfilldatabase (for now) and
downloads data from Schedules Direct using the new JSON format.

#Features:

- only downloads programs that have changes. Your first download may be
  40000 programs, but daily downloads after that will be 2-3000 depending on
  how many channels you have.  If the same program is broadcast on multiple
  channels, it's still only downloaded once.
- QAM tuning information.
- program-specific language information. (A program that's being broadcast
  in Mandarin will have a Mandarin tag associated with it)
- Additional downloaded metadata regarding programs relating to content advisories.
- Season and Episode information.
- Logos, fanart, etc.
- Full support for United States, Canada, Great Britain.
- See http://forums.schedulesdirect.org/viewtopic.php?f=8&t=2530 for the
  complete list of countries with data.
- Self-hosted data so not dependent on Tribune's XML servers

#Installation

Install the prerequisites:

sudo apt-get install git php5-cli php5-curl curl

Start by cloning into a local directory:

git clone https://github.com/SchedulesDirect/mfdb-json.git

Create the required tables with the dbInit.sql script

mysql -umythtv -p < dbInit.sql

Install Composer:

curl -sS https://getcomposer.org/installer | php

php composer.phar install

#Configuration
##MythTV

If you've never run mythtv-setup, configure the following first, otherwise
skip to the next section.
```
1. General
2. Capture cards
3. Recording Profiles
7. Storage Directories
```
Do not configure Video Sources or Input Connections!

Exit mythtv-setup. You will be prompted that you haven't set your start
channel.  Select "No, I know what I'm doing."

##mfdb

Run the sd-utility.php script.

If you've never used the Schedules Direct JSON service before, you will be
prompted to enter your username and password; otherwise, your username and
password will be read from the "videosource" table in mythconverg.

Add your headend to your Schedules Direct JSON-service account.  (Your
existing Schedules Direct information for the XML service isn't
automatically copied over, and the service is running on different hosts.)

```
sd-utility.php utility program v0.10/2014-04-03
Attempting to connect to database.
Using beta server.
Logging into Schedules Direct.

Status messages from Schedules Direct:
Server: AWS-SD-web.1
Last data refresh: 2014-04-03T17:13:48Z
Account expires: 2014-06-28T05:16:29Z
Max number of headends for your account: 16
Next suggested connect time: 2014-04-04T05:58:57Z

WARNING: *** No lineups configured at Schedules Direct. ***

WARNING: *** No videosources configured in MythTV. ***

Schedules Direct functions:
1 Add a lineup to your account at Schedules Direct
2 Delete a lineup from your account at Schedules Direct
3 Acknowledge a message
4 Print a channel table for a lineup

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
L to Link a videosource to a lineup at Schedules Direct
R to refresh a videosource with new lineup information
Q to Quit

>**1**
Three-character ISO-3166-1 alpha3 country code:>**USA**
Enter postal code:>**60030**

headend: 4DTV
location: USA
        name: 4DTV
        Lineup: USA-4DTV-DEFAULT

headend: AFN
location: USA
        name: AFN Satellite
        Lineup: USA-AFN-DEFAULT

headend: C-BAND
location: USA
        name: C-Band
        Lineup: USA-C-BAND-DEFAULT

headend: DISH602
location: Chicago
        name: DISH Chicago
        Lineup: USA-DISH602-DEFAULT

headend: DITV
location: USA
        name: DIRECTV
        Lineup: USA-DITV-DEFAULT

headend: DITV602
location: Chicago
        name: DIRECTV Chicago
        Lineup: USA-DITV602-DEFAULT

headend: ECHOST
location: USA
        name: DISH Network
        Lineup: USA-ECHOST-DEFAULT

headend: GLOBCST
location: USA
        name: Globecast World TV
        Lineup: USA-GLOBCST-DEFAULT

headend: IL57303
location: Lake Forest
        name: Comcast Waukegan/Lake Forest Area - Digital
        Lineup: USA-IL57303-X

headend: IL61078
location: Grayslake
        name: Saddlebrook Farms - Cable
        Lineup: USA-IL61078-DEFAULT

headend: IL63063
location: Mchenry
        name: Comcast McHenry & Fox Areas - Digital
        Lineup: USA-IL63063-X

headend: IL63463
location: Carpentersville
        name: Comcast Algonquin/Elgin Areas - Digital
        Lineup: USA-IL63463-X

headend: IL67050
location: Chicago
        name: AT&T U-verse TV - Digital
        Lineup: USA-IL67050-X

headend: RELAYTV
location: USA
        name: RELAYTV
        Lineup: USA-RELAYTV-DEFAULT

headend: 60030
location: 60030
        name: Antenna
        Lineup: USA-OTA-60030

Lineup to add>

```

Notice that in this particular country / postal code, there are headends,
and there are lineups within that headend, but there aren't multiple lineups
in a headend - all devices are either "-DEFAULT" or "-X".

You may see this:

```
Three-character ISO-3166-1 alpha3 country code:>USA
Enter postal code:>90210

headend: 4DTV
location: USA
        name: 4DTV
        Lineup: USA-4DTV-DEFAULT


(snip)

headend: CA00053
location: Beverly Hills
        name: Time Warner Cable - Cable
        Lineup: USA-CA00053-DEFAULT
        name: Time Warner Cable - Digital
        Lineup: USA-CA00053-X

headend: CA61222
location: Beverly Hills
        name: Mulholland Estates - Cable
        Lineup: USA-CA61222-DEFAULT

headend: CA66511
location: Los Angeles
        name: AT&T U-verse TV - Digital
        Lineup: USA-CA66511-X

headend: CA67309
location: Westchester
        name: Time Warner Cable - Cable
        Lineup: USA-CA67309-DEFAULT
        name: Time Warner Cable - Digital
        Lineup: USA-CA67309-X

headend: CA67310
location: Eagle Rock
        name: Time Warner Cable City of Los Angeles - Cable
        Lineup: USA-CA67310-DEFAULT
        name: Time Warner Cable City of Los Angeles - Digital
        Lineup: USA-CA67310-X

(etc)

```

Notice that headend CA00053 has multiple lineups, as does CA67309 and CA67310.

You can encounter this overseas:

```
Three-character ISO-3166-1 alpha3 country code:>GBR
Enter postal code:>W2

headend: 0000218
location: Watford
        name: Virgin Media Lewisham - Cable
        Lineup: GBR-0000218-DEFAULT
        name: Virgin Media Lewisham - Digital
        Lineup: GBR-0000218-X
        name: Virgin Media Lewisham - Cable
        Lineup: GBR-0000218-Y

headend: 0001122
location: London
        name: Virgin Media Westminster - Cable
        Lineup: GBR-0001122-DEFAULT
        name: Virgin Media Westminster - Digital
        Lineup: GBR-0001122-X
        name: Virgin Media Westminster - Cable
        Lineup: GBR-0001122-Y

(snip)
```

In any case, the script will ask you which lineup to add.

```
Lineup to add>USA-IL57303-X
Message from server: Added lineup.

Status messages from Schedules Direct:
Server: AWS-SD-web.1
Last data refresh: 2014-04-03T17:13:48Z
Account expires: 2014-06-28T05:16:29Z
Max number of headends for your account: 16
Next suggested connect time: 2014-04-04T04:41:13Z
The following lineups are in your account at Schedules Direct:

┌───────────────┬────────────────────┬─────────────────────────┬────┐
│Lineup         │Server modified     │MythTV videosource update│New?│
├───────────────┼────────────────────┼─────────────────────────┼────┤
│USA-IL57303-X  │2014-04-03T13:50:00Z│                         │*** │
└───────────────┴────────────────────┴─────────────────────────┴────┘
Checking for updated lineups from Schedules Direct.
Retrieving lineup from Schedules Direct.
Retrieving lineup from Schedules Direct.
```

Repeat this process until you have added the necessary lineups to your
account.

Create a videosource
--------------------

```
Schedules Direct functions:
1 Add a lineup to your account at Schedules Direct
2 Delete a lineup from your account at Schedules Direct
3 Acknowledge a message
4 Print a channel table for a lineup

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
L to Link a videosource to a lineup at SD
R to refresh a videosource with new lineup information
Q to Quit
>A
Adding new videosource

Name:>Comcast

Status messages from Schedules Direct:

<status snipped>

MythTV videosource:
sourceid: 4     name: Comcast   Schedules Direct lineup:

```

=====================================

Now that we've got a videosource configured, you have to link the
videosource to the lineup you've added to your account.

```
Schedules Direct functions:
1 Add a lineup to your account at Schedules Direct
2 Delete a lineup from your account at Schedules Direct
3 Acknowledge a message
4 Print a channel table for a lineup

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
L to Link a videosource to a lineup at SD
R to refresh a videosource with new lineup information
Q to Quit

>L
Linking Schedules Direct lineup to sourceid

MythTV sourceid:>4
Schedules Direct lineup:>USA-IL57303-X

(status snipped)

MythTV videosource:
sourceid: 4     name: Comcast   Schedules Direct lineup: USA-IL57303-X
```

=====================================

Once that's done, you'll need to download the channel mapping using the "R"
function.  This is also done whenever the utility program tells you that
there's an update available to the headend by putting "***" in the "New"
column.

```
(status deleted)

>R
Which lineup:>USA-IL57303-X
Updating channel table for lineup:USA-IL57303-X

Status messages from Schedules Direct:
Server: AWS-SD-web.1
Last data refresh: 2014-04-03T17:13:48Z
Account expires: 2014-06-28T05:16:29Z
Max number of headends for your account: 16
Next suggested connect time: 2014-04-04T08:46:21Z
The following lineups are in your account at Schedules Direct:

┌───────────────┬────────────────────┬─────────────────────────┬────┐
│Lineup         │Server modified     │MythTV videosource update│New?│
├───────────────┼────────────────────┼─────────────────────────┼────┤
│USA-IL57303-X  │2014-04-03T13:50:00Z│2014-04-03T13:50:00Z     │    │
└───────────────┴────────────────────┴─────────────────────────┴────┘
Checking for updated lineups from Schedules Direct.
Retrieving lineup from Schedules Direct.
Retrieving lineup from Schedules Direct.
```

Quit out of the utility program.

If there are channels you don't want, unset the "visible" state in the
channel table, because that's what the script will use to determine what
schedules to download.

=====================================

Restart mythtv-setup

Go into "5. Input connections" and associate the hardware inputs with the
video source you created in the sd-utility script.

Exit mythtv-setup

=====================================


Run the mfdb-json.php script to retrieve the data.

```
00:00:49:mfdb-json.php developer grabber v0.07/2014-02-16
00:00:49:Temp directory for Schedules is /tmp/mfdbo0eoW0
00:00:49:Temp directory for Programs is /tmp/mfdbRHPKAw
00:00:49:Connecting to MythTV database.
00:00:49:Using beta server.
00:00:49:Retrieving list of channels to download.
00:00:49:Logging into Schedules Direct.
00:00:49:Retrieving server status message.
00:00:49:Server: AWS-SD-web.1
00:00:49:Last data refresh: 2014-02-18T20:39:29Z
00:00:49:Account expires: 2014-06-28T05:16:29Z
00:00:49:Max number of lineups for your account: 16
00:00:49:Next suggested connect time: 2014-02-19T08:36:44Z
00:00:49:Sending schedule request.
00:01:19:There are 32263 programIDs in the upcoming schedule.
00:01:19:Retrieving existing MD5 values.
00:01:19:Need to download 247 new or updated programs.
00:01:19:Maximum programs we're downloading per call: 2000
00:01:19:Retrieving chunk 1 of 1.
00:01:20:Performing inserts of JSON data.
00:01:21:100 / 247
00:01:21:200 / 247
00:01:22:Completed local database program updates.
00:01:22:Inserting schedules.
00:01:23:Inserting schedule for chanid:4034 sourceid:4 xmltvid:10021
00:01:23:Inserting schedule for chanid:4050 sourceid:4 xmltvid:10035
00:01:24:Inserting schedule for chanid:4046 sourceid:4 xmltvid:10051
00:01:24:Inserting schedule for chanid:4073 sourceid:4 xmltvid:10057

(etc)

00:04:49:Inserting schedule for chanid:4408 sourceid:4 xmltvid:82541
00:04:49:Inserting schedule for chanid:4237 sourceid:4 xmltvid:82547
00:04:49:Inserting schedule for chanid:4139 sourceid:4 xmltvid:84172
00:04:50:Done inserting schedules.
00:04:50:Status:Successful.
00:04:50:Global. Start Time:2014-02-19 00:00:49
00:04:50:Global. End Time:2014-02-19 00:04:50
00:04:50:4 minutes 1 seconds.
00:04:50:Updating status.
00:04:50:Done.
```

Once the download / update is done, start mythbackend and see if it's happy. 
Try to schedule something to record.  If it works, then excellent!  If not,
let me know what didn't work and I'll take a look.

The scripts will tell you the directories they're using; the raw data files
are going to be plaintext json.
