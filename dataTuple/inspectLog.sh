#!/bin/bash

BASEPATH=$1

lineAOK=`grep -nr "A-OK" $BASEPATH/dataTuple.log | tail -1 | tr ':' ' ' | awk '{print $1}'`
lineBAD=`grep -nr "No Proxy found" $BASEPATH/dataTuple.log | tail -1 | tr ':' ' ' | awk '{print $1}'`
if [ "$lineAOK" == "" ]; then lineAOK=0; fi
if [ "$lineBAD" == "" ]; then lineBAD=0; fi

#See if it's doing the password thing
if [ "$lineAOK" -lt "$lineBAD" ]
then 
  isFucked="true"
else
  isFucked="false"
fi

#If not fucked, done
if [ "$isFucked" == "false" ]
then
  exit 0
fi

#If it is, check to see if it's already reported
status=`awk 'NR==1' voms_status.txt | awk '{print $1}'`
if [ "$status" == "subject" ]
then
  isReported="no"
elif [ "$status" == "passwordProblem" ] 
then
  isReported="yes"
fi

#If not already reported, report it
if [ "$isReported" == "no" ] 
then
  echo "Error!! $USER doesn't have a proxy!!" | /bin/mail -r "namin@physics.ucsb.edu" -s "[dataTuple] error report" "namin@physics.ucsb.edu, mark.derdzinski@gmail.com" 
  echo "passwordProblem" > voms_status.txt
  echo "reporting"
else
  echo "not reporting"
fi

