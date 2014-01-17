mfdb-json
=========

mythfilldatabase grabber for the Schedules Direct JSON service.

v0.02, 2014-01-17

This program runs as a replacement to mythfilldatabase (for now) and
downloads data from Schedules Direct using the new JSON format.

Features:

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
- Full support for United States, Canada, Great Britain. Additional
  countries (Mexico, Latin America, Brazil, Carribean) coming online.

Installation
============
Install the prerequisites:

apt-get install git php-cli 

Start by cloning into a local directory:

git clone https://github.com/SchedulesDirect/mfdb-json.git

Create the required tables with the dbInit.sql script

mysql -umythtv -p < dbInit.sql

Configuration
=============
If you've never run mythtv-setup, configure the following first, otherwise
skip to the next section.

1. General
2. Capture cards
3. Recording Profiles
7. Storage Directories

Do not configure Video Sources or Input Connections.

Exit mythtv-setup. You will be prompted that you haven't set your start
channel.  Select "No, I know what I'm doing."

Next:
-----
Run the sd-utility.php script.

If you've never used Schedules Direct before, you will be prompted to enter
your username and password; otherwise, your username and password will be
read from the "videosource" table in mythconverg.

Add your headend to your Schedules Direct JSON-service account.  (Your
existing Schedules Direct information for the XML service isn't
automatically copied over, and the service is running on different hosts.)

Create a videosource
--------------------

sd-utility.php utility program v0.03/2014-01-17
Attempting to connect to database.
Using beta server.
Logging into Schedules Direct.

Status messages from Schedules Direct:
MessageID: 1234567890123456789012 : 2013-11-15T19:49:53Z : This is a test of the message function.
Server: AWS-micro.1
Last data refresh: 2014-01-17T16:57:11Z
Account expires: 2014-06-28T05:16:29Z
Max number of headends for your account: 3
Next suggested connect time: 2014-01-18T12:34:47Z
The following headends are in your account at Schedules Direct:

IL57303  Last Updated: 2014-01-07T20:54:01Z
PC:60030         Last Updated: 2014-01-07T22:51:18Z

WARNING: *** No videosources configured in MythTV. ***

Schedules Direct functions:
1 Add a headend to account at Schedules Direct
2 Delete a headend from account at Schedules Direct
3 Acknowledge a message
4 Print a channel lineup for a headend

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
L to Link a videosource to a headend at SD
R to refresh a videosource with new lineup information
Q to Quit
>a
Adding new videosource

Name:>SD JSON

=====================================

Now that we've got a videosource configured, you have to link the
videosource to the headend you've added to your account.

Status messages from Schedules Direct:
MessageID: 1234567890123456789012 : 2013-11-15T19:49:53Z : This is a test of the message function.
Server: AWS-micro.1
Last data refresh: 2014-01-17T16:57:11Z
Account expires: 2014-06-28T05:16:29Z
Max number of headends for your account: 3
Next suggested connect time: 2014-01-18T02:24:00Z
The following headends are in your account at Schedules Direct:

IL57303  Last Updated: 2014-01-07T20:54:01Z
PC:60030         Last Updated: 2014-01-07T22:51:18Z

Existing sources in MythTV:
sourceid: 1     name: SD JSON   lineupid:

Schedules Direct functions:
1 Add a headend to account at Schedules Direct
2 Delete a headend from account at Schedules Direct
3 Acknowledge a message
4 Print a channel lineup for a headend

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
L to Link a videosource to a headend at SD
R to refresh a videosource with new lineup information
Q to Quit
>l
Linking Schedules Direct headend to sourceid

sourceid:>1
lineupid:>IL57303
Your headend has only one devicetype: X

=====================================

Once that's done, you'll need to download the channel mapping using the "R"
function.  This is also done whenever the utility program tells you that
there's an update available to the headend.

Status messages from Schedules Direct:
MessageID: 1234567890123456789012 : 2013-11-15T19:49:53Z : This is a test of the message function.
Server: AWS-micro.1
Last data refresh: 2014-01-17T16:57:11Z
Account expires: 2014-06-28T05:16:29Z
Max number of headends for your account: 3
Next suggested connect time: 2014-01-18T09:46:15Z
The following headends are in your account at Schedules Direct:

IL57303  Last Updated: 2014-01-07T20:54:01Z
PC:60030         Last Updated: 2014-01-07T22:51:18Z

Existing sources in MythTV:
sourceid: 1     name: SD JSON   lineupid: IL57303:X

Schedules Direct functions:
1 Add a headend to account at Schedules Direct
2 Delete a headend from account at Schedules Direct
3 Acknowledge a message
4 Print a channel lineup for a headend

MythTV functions:
A to Add a new videosource to MythTV
D to Delete a videosource in MythTV
L to Link a videosource to a headend at SD
R to refresh a videosource with new lineup information
Q to Quit
>r
lineupid:>IL57303
Headend update for IL57303
Apply to sourceid:>1
Updating channel table for sourceid:1

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

20:45:28:mfdb-json.php developer grabber v0.06/2014-01-17
20:45:28:Temp directory for Schedules is /tmp/mfdbN6mfYN
20:45:28:Temp directory for Programs is /tmp/mfdbaziI7w
20:45:28:Connecting to MythTV database.
20:45:28:Using beta server.
20:45:28:Retrieving list of channels to download.
20:45:28:Logging into Schedules Direct.
20:45:29:Retrieving server status message.
20:45:29:MessageID:1234567890123456789012 : 2013-11-15T19:49:53Z : This is a test of the message function.
20:45:29:Server: AWS-micro.1
20:45:29:Last data refresh: 2014-01-17T16:57:11Z
20:45:29:Account expires: 2014-06-28T05:16:29Z
20:45:29:Max number of headends for your account: 3
20:45:29:Next suggested connect time: 2014-01-17T22:24:28Z
20:45:29:Sending schedule request.
20:45:58:Parsing /tmp/mfdbN6mfYN/sched_10021.json.txt
20:45:58:Parsing /tmp/mfdbN6mfYN/sched_10035.json.txt
(etc)

20:46:01:There are 39461 programIDs in the upcoming schedule.
20:46:01:Retrieving existing MD5 values.
20:46:01:Need to download 39461 new or updated programs.
20:46:01:Requesting more than 10000 programs. Please be patient.
20:46:01:Retrieving chunk 1 of 20.
20:46:26:Retrieving chunk 2 of 20.
20:50:46:Retrieving chunk 3 of 20.
20:51:06:Retrieving chunk 4 of 20.
(etc)
20:55:25:Performing inserts of JSON data.
20:58:07:Completed local database program updates.
20:58:07:Inserting schedules.
20:58:08:Inserting schedule for chanid:1001 sourceid:1 xmltvid:74348
20:58:08:Inserting schedule for chanid:1002 sourceid:1 xmltvid:11299
20:58:09:Inserting schedule for chanid:1003 sourceid:1 xmltvid:12475
(etc)

Once the download / update is done, start mythbackend and see if it's happy. 
Try to schedule something to record.  If it works, then excellent!  If not,
let me know what didn't work and I'll take a look.

The scripts will tell you the directories they're using; the raw data files
are going to be plaintext inside a .zip

