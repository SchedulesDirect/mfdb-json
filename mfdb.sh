#!/bin/sh

# Script originally by Bill Meeks.
# Using mythtv-setup, configure the following:
#   Automatically update program listings: Checked
#   Guide data program: <Full path for this script>
#     Example: /home/mythtv/mfdb-json/mfdb.sh
#   Guide data arguments: <empty>
#   Guide data program execution start: 0
#   Guide data program execution end: 23
#   Run guide data program at time suggested by the grabber: Checked
# Change the log file below to something appropriate for you.

cd ~/mfdb-json
./mfdb-json.php 2>&1 >> /var/log/mythtv/sd-json-beta.log
