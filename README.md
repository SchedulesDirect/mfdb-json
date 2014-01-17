mfdb-json
=========

mythfilldatabase grabber for the Schedules Direct JSON service.

v0.02, 2014-01-17

This program runs as a replacement to mythfilldatabase (for now) and
downloads data from Schedules Direct using the new JSON format.

Features:

- only downloads programs that have changes. Your first download may be
  40000 programs, but daily downloads after that will be 2-3000 depending on
  how many channels you have.
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
Create a source in MythTV called "SD JSON Beta" and associate it with your
tuners, but don't run mythfilldatabase.

Run sd-utility.php first, add your headend to your Schedules Direct
JSON-service account.  Your existing Schedules Direct information for the
XML service isn't automatically copied over.

Link the new headend with the hardware sourceIDs that you have.

Quit out of the utility program.

If there are channels you don't want, unset the "visible" state in the
channel table, because that's what the script will use to determine what
schedules to download.

Run the mfdb-json.php script to retrieve the data.

Once the download / update is done, start mythbackend and see if it's happy. 
Try to schedule something to record.  If it works, then excellent!  If not,
let me know what didn't work and I'll take a look.

The scripts will tell you the directories they're using; the raw data files
are going to be plaintext inside a .zip
