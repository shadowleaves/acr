#!/bin/bash
n=0
DELAY=1
TOTAL=99999
until [ $n -ge $TOTAL ]
do
  TS=`date`
  echo "running $1 @ $TS..."
  "$@"
  echo "failed...retry in $DELAY second(s), $n/$TOTAL retries..."
  n=$[$n+1]
  sleep $DELAY
done

