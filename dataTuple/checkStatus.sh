#!/bin/bash

FILENAME=$1
JOB=$2

#Run condor_q to get status of job
condor_q $JOB > temp_status.txt
sed -i '1,4d' temp_status.txt

#Read condor_q output to fill lists.  
while read line
do
  if [[ `echo $line | awk '{ print $6 }'` == "R" ]]
  then
    echo $FILENAME >> runningList.txt
  elif [[ `echo $line | awk '{ print $6 }'` == "C" ]]
  then
    echo $FILENAME >> runningList.txt
  elif [[ `echo $line | awk '{ print $6 }'` == "I" ]]
  then
    echo $FILENAME >> idleList.txt
  elif [[ `echo $line | awk '{ print $6 }'` == "H" ]]
  then
    echo $FILENAME >> heldList.txt
  fi
done < temp_status.txt

rm temp_status.txt
