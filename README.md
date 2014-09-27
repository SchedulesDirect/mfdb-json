#mfdb-json

Grabber for the Schedules Direct JSON service.

v0.26, 2014-09-26

Robert Kulagowski

grabber@schedulesdirect.org

These programs can be used as a replacement to mythfilldatabase and
for downloading data from Schedules Direct using the new JSON format.

This file describes API 20140530.

**NOTE**: You are strongly encouraged to run "git pull" to refresh your client before each use; the software is under
active development and this will ensure that you stay up-to-date.

**NOTE**: Version of the client after 0.12 will now query the server and report if they're not the current version.

#Features:
##MythTV-only
- only downloads programs that have changes. Your first download may be
  40000 programs, but daily downloads after that will be 2-3000 depending on
  how many channels you have.  If the same program is broadcast on multiple
  channels, it's still only downloaded once.

##All
- QAM tuning information.
- program-specific language information. (A program that's being broadcast
  in Mandarin will have a Mandarin tag associated with it)
- Additional downloaded metadata regarding programs relating to content advisories.
- Season and Episode information.
- Logos, fanart, etc.
- Full support for United States, Canada, Great Britain.
- See http://forums.schedulesdirect.org/viewtopic.php?f=8&t=2586 for the
  complete list of countries with data.
- Self-hosted data, so not dependent on Tribune's XML servers.

If the two scripts are called with the **--nomyth** parameter, then certain
functionality changes.  Rather than reading username and password from the
mythconverg database, the scripts will look for a file called "sd.json.conf"

**NOTE**: the password is stored as plaintext.

mfdb-json.php may be called with **--nomyth** as well, in which case you
must specify --schedule and/or --program

The list of stationIDs you wish to retrieve must be in
"sd.json.stations.conf".  One stationID per line.

The list of programs you wish to retrieve must be in
"sd.json.programs.conf".  One programID per line.

This functionality only exists as a demonstration or for developers; the
rest of the documentation assumes that the user is running the programs to
populate MythTV data.

#Installation

Install the prerequisites:

###Ubuntu
sudo apt-get install git php5-cli php5-curl curl

###Fedora
yum -y install git curl curl-devel libcurl libcurl-devel php-common

###Clone from github

git clone https://github.com/SchedulesDirect/mfdb-json.git

cd mfdb-json

git checkout API-20140530

###Install Composer

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
Do not configure Video Sources or Input Connections - you will do that later.

**NOTE**:If you have an Over-the-Air lineup then the instructions are a little more 
involved; take note of the OTA exceptions in the instructions below.

**NOTE**:If you have a QAM lineup then take note of the QAM specifics.

Exit mythtv-setup. You will be prompted that you haven't set your start
channel.  Select "No, I know what I'm doing."

##sd-utility

Run the sd-utility.php script.

This script does some housekeeping functions relating to lineup management
at Schedules Direct.

If called with the **--nomyth** parameter, then MythTV-specific functions will not be used.

The first time you run the script it will create necessary tables in your
MythTV database.

You can run it with **--help** to see the various options.

```
sd-utility.php utility program API:20140530 v0.13/2014-09-26

The following options are available:
--countries     The list of countries that have data.
--debug         Enable debugging. (Default: FALSE)
--dbname=       MySQL database name. (Default: mythconverg)
--dbuser=       Username for database access. (Default: mythtv)
--dbpassword=   Password for database access. (Default: mythtv)
--dbhost=       MySQL database hostname. (Default: localhost)
--extract       Don't do anything but extract data from the table for QAM/ATSC. (Default: FALSE)
--help          (this text)
--host=         IP address of the MythTV backend. (Default: localhost)
--logo=         Directory where channel logos are stored (Default: /home/mythtv/.mythtv/channels)
--nomyth        Don't execute any MythTV specific functions. (Default: FALSE)
--skiplogo      Don't download channel logos.
--username=     Schedules Direct username.
--password=     Schedules Direct password.
--usedb         Use a database to store data, even if you're not running MythTV. (Default: FALSE)
--timezone=     Set the timezone for log file timestamps. See http://www.php.net/manual/en/timezones.php (Default:UTC)
--version       Print version information and exit.
```

If you've never used the Schedules Direct JSON service before, you will be
prompted to enter your username and password; otherwise, your username and
password will be read from the "videosource" table in mythconverg.

MythTV database access information will be read from
**/etc/mythtv/config.xml** and overriden if **~/.mythtv/config.xml** exists. 
If neither file is present then you will need to pass database access
information on the command line if you intend on using a database.

###Add a lineup.

Add your lineup to your Schedules Direct JSON-service account.  (Your
existing Schedules Direct information for the XML service isn't
automatically copied over, and the service is running on different hosts.)

```
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
1 Add a known lineupID to your account at Schedules Direct
2 Search for a lineup to add to your account at Schedules Direct
3 Delete a lineup from your account at Schedules Direct
4 Acknowledge a message
5 Print a channel table for a lineup

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
E to Extract Antenna / QAM / DVB scan from MythTV to send to Schedules Direct
L to Link a videosource to a lineup at Schedules Direct
U to update a videosource by downloading from Schedules Direct
Q to Quit

```
Type in: **2**

Three-character ISO-3166-1 alpha3 country code (? to list available countries):>**USA**

Enter postal code:>**60030**

```
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

Three-character ISO-3166-1 alpha3 country code (? to list available countries):>>**USA**

Enter postal code:>**90210**

```
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

You can encounter the following outside of North America:

Three-character ISO-3166-1 alpha3 country code (? to list available countries):>**GBR**

Enter postal code:>**W2**

```
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

Lineup to add>**USA-IL57303-X**

Repeat this process until you have added the necessary lineups to your
account.

```
Status messages from Schedules Direct:
Server: 20140530.t2.1
Last data refresh: 2014-09-05T17:22:22Z
Account expires: 2015-06-28T05:16:29Z
Max number of headends for your account: 16
Next suggested connect time: 2014-09-06T15:23:59Z
The following lineups are in your account at Schedules Direct:

┌──────┬────────────────────┬────────────────────┬─────────────────────────┬───────┐
│Number│Lineup              │Server modified     │MythTV videosource update│Status │
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│1     │BES-0000044-DEFAULT │2014-09-02T18:34:18Z│                         │Updated│
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│2     │CAN-OTA-L5H3J2      │2014-09-05T16:48:09Z│2014-09-03T14:11:24Z     │Updated│
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│3     │TCA-0003402-X       │2014-09-05T19:24:54Z│                         │Updated│
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│4     │USA-IL57303-X       │2014-09-05T16:47:30Z│2014-08-19T04:19:31Z     │Updated│
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│5     │USA-WA61851-X       │2014-09-05T16:47:30Z│                         │Updated│
└──────┴────────────────────┴────────────────────┴─────────────────────────┴───────┘
Checking for updated lineups from Schedules Direct.
Retrieving lineup BES-0000044-DEFAULT from Schedules Direct.
Retrieving lineup CAN-OTA-L5H3J2 from Schedules Direct.
Retrieving lineup TCA-0003402-X from Schedules Direct.
Retrieving lineup USA-IL57303-X from Schedules Direct.
Retrieving lineup USA-WA61851-X from Schedules Direct.
```

###Add a videosource

```
Schedules Direct functions:
1 Add a known lineupID to your account at Schedules Direct
2 Search for a lineup to add to your account at Schedules Direct
3 Delete a lineup from your account at Schedules Direct
4 Acknowledge a message
5 Print a channel table for a lineup

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
E to Extract Antenna / QAM / DVB scan from MythTV to send to Schedules Direct
L to Link a videosource to a lineup at Schedules Direct
U to update a videosource by downloading from Schedules Direct
Q to Quit

```
Type in: **A**

```
Adding new videosource

Name:>Comcast

Status messages from Schedules Direct:

<status snipped>

MythTV videosource:
sourceid: 1     name: Comcast   Schedules Direct lineup:

```

###Link lineup to MythTV sources

Now that we've got a videosource configured, you have to link it
to the lineup you've added to your Schedules Direct account.

```
Schedules Direct functions:
1 Add a known lineupID to your account at Schedules Direct
2 Search for a lineup to add to your account at Schedules Direct
3 Delete a lineup from your account at Schedules Direct
4 Acknowledge a message
5 Print a channel table for a lineup

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
E to Extract Antenna / QAM / DVB scan from MythTV to send to Schedules Direct
L to Link a videosource to a lineup at Schedules Direct
U to update a videosource by downloading from Schedules Direct
Q to Quit

>
```
Type in: **L**

```
Linking Schedules Direct lineup to sourceid

MythTV sourceid:>1
Schedules Direct lineup:>USA-IL57303-X

(status snipped)

MythTV videosource:
sourceid: 1     name: Comcast   Schedules Direct lineup: USA-IL57303-X
```

###Refresh lineup
####Antenna / Over-the-air and QAM

QAM users have the option of running a scan using mythtv-setup and having
the utility program update the channel table, or skip the scan and just use
the QAM tuning information directly.  If you are using the tuning
information directly you can skip to the next step; you'll be asked which
method you'd like to use in the "R" step.

If you have QAM, but it didn't appear in the list of lineups available in
your headend, please run the scan in mythtv-setup, then see

http://forums.schedulesdirect.org/viewtopic.php?f=3&t=1211

The utility program will extract the scanned QAM information from your
database.  Send the file to

qam-info@schedulesdirect.org

Once we have the scan, we will correlate the station names and create a
"-QAM" lineup for you.

For Antenna and QAM users who will run a scan, once you've added the
appropriate lineup to your Schedules Direct account, added a videosource (in
this document we will assume that you called it "Antenna"), and Linked it,
quit out of sd-utility.php and restart mythtv-setup.

Go to Input Connections, select your hardware device and select the
appropriate Video Source.  In this instance, it's "Antenna", because that's
the name that we created in sd-utility.php for the OTA scan.

Go to "Scan for channels" and use the defaults. Allow the scan to complete.

Restart sd-utility.php and continue to the next step:

####Antenna/QAM lineup post-scan and non-Antenna lineups.

Once the scan is complete, or you're using a device which doesn't require a
scan, you'll need to download the channel mapping with the "U" function. 
This is also done whenever the utility program tells you that there's an
update available to the headend by putting "Updated" in the "Status" column.

Type in: **U**

```
Which lineup:>USA-IL57303-X
Updating channel table for lineup:USA-IL57303-X

Status messages from Schedules Direct:
Server: 20140530.t2.1
Last data refresh: 2014-09-05T17:22:22Z
Account expires: 2015-06-28T05:16:29Z
Max number of headends for your account: 16
Next suggested connect time: 2014-09-06T15:23:59Z
The following lineups are in your account at Schedules Direct:

┌──────┬────────────────────┬────────────────────┬─────────────────────────┬───────┐
│Number│Lineup              │Server modified     │MythTV videosource update│Status │
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│1     │BES-0000044-DEFAULT │2014-09-02T18:34:18Z│                         │Updated│
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│2     │CAN-OTA-L5H3J2      │2014-09-05T16:48:09Z│2014-09-03T14:11:24Z     │Updated│
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│3     │TCA-0003402-X       │2014-09-05T19:24:54Z│                         │Updated│
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│4     │USA-IL57303-X       │2014-09-05T16:47:30Z│2014-08-19T04:19:31Z     │       │
├──────┼────────────────────┼────────────────────┼─────────────────────────┼───────┤
│5     │USA-WA61851-X       │2014-09-05T16:47:30Z│                         │Updated│
└──────┴────────────────────┴────────────────────┴─────────────────────────┴───────┘
Checking for updated lineups from Schedules Direct.
Retrieving lineup BES-0000044-DEFAULT from Schedules Direct.
Retrieving lineup CAN-OTA-L5H3J2 from Schedules Direct.
Retrieving lineup TCA-0003402-X from Schedules Direct.
Retrieving lineup USA-IL57303-X from Schedules Direct.
Retrieving lineup USA-WA61851-X from Schedules Direct.

```

If you're using QAM, you'll see:
```
Which lineup:>USA-AL56879-QAM
Updating channel table for lineup:USA-AL56879-QAM
You can:
1. Exit this program and run a QAM scan using mythtv-setup then use the QAM lineup to populate stationIDs. (Default)
2. Use the QAM lineup information directly.
```

Select "1" if you're just going to update a mythtv-setup scan with stationIDs.
Select "2" if you want the utility program to do everything.

Quit out of the utility program.

If there are channels you don't want, unset the "visible" state in the
channel table, because that's what the script will use to determine what
schedules to download.

##Back to MythTV
###Antenna

If you've configured an Antenna lineup above and it's your only lineup, then
you're actually done at this point.

###All others.

Restart mythtv-setup

Go to "5. Input connections" and associate the hardware inputs with the
video sources you created in the sd-utility script.  (Other than Antenna,
because that's already done at this point.)

You do not need to "Retrieve lineup" or scan for channels.

Exit mythtv-setup

#Manually retrieving data

Run the mfdb-json.php script to download schedule data.

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
00:01:23:Inserting schedule for chanid:4034 sourceid:1 xmltvid:10021
00:01:23:Inserting schedule for chanid:4050 sourceid:1 xmltvid:10035
00:01:24:Inserting schedule for chanid:4046 sourceid:1 xmltvid:10051
00:01:24:Inserting schedule for chanid:4073 sourceid:1 xmltvid:10057

(etc)

00:04:49:Inserting schedule for chanid:4408 sourceid:1 xmltvid:82541
00:04:49:Inserting schedule for chanid:4237 sourceid:1 xmltvid:82547
00:04:49:Inserting schedule for chanid:4139 sourceid:1 xmltvid:84172
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
are going to be plaintext JSON.

**NOTE**: The script does not delete the data files while the scripts are
still being developed.  Make sure that /tmp doesn't get full!

#Automatically retrieving data

One the manual invocation above runs without issue, you can configure MythTV
to automatically run the grabber.

There is a script called "mfdb.sh" which was cloned into this directory. 
Edit and update the script as required for your setup.  The script contains
comments on what needs to be configured in mythtv-setup.

It's recommended that you leave everything in the mfdb-json directory; the
script depends on being able to access the libraries downloaded with
Composer, so moving the script to /var/lib or somewhere else will break
things unless you move the entire directory.
