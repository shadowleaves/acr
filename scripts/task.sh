#!/bin/bash
source $HOME/.bash_profile
# source activate algo

TS1="$(date +'%Y%m%dT%H.%M.%S')"
TS=`date`
export LOG=$HOME/log/$1_$TS1.log

echo cron task $1 started @ $TS ...
echo writing to $LOG ...

# erasing log file below...
echo >$LOG

echo --------- $1 started on $HOSTNAME @ $TS ---------- >>$LOG
echo >>$LOG
unbuffer bash $HOME/scripts/$1 $2 $3 $4 $5 $6 $7 $8 $9 >> $LOG 2>&1

TS=`date`
echo >>$LOG
echo ========= $1 ended__ @ $TS ========== >>$LOG
echo >>$LOG

export LOG=
