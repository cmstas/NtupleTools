#!/bin/bash

if [ ! -d $BASEPATH ]
then
  echo "BASEPATH in GenerateMasterList.sh does not exist!"
fi

> masterList.txt

readarray -t samples < $BASEPATH/input.txt
for i in "${samples[@]}"
do
  # input_from_das=`./das_client.py --query="file dataset= $i site=T2_US_*" --limit=0` #need to use --limit=0 to pick up all files!
  input_from_das=`./das_client.py --query="file dataset=$i" --limit=0` #need to use --limit=0 to pick up all files!
  echo "$input_from_das" | grep "status: fail"
  if [ $? == 0 ]
  then 
    echo "failed" >> nQueryAttempts.txt
    if [ -e nQueryAttempts.txt ] && [ "$(cat nQueryAttempts.txt | wc -l)" -gt "60" ]; then echo "DataTupleWarning! Query attempt has failed many times!" | /bin/mail -r "namin@physics.ucsb.edu" -s "[dataTuple] error report" "namin@physics.ucsb.edu, mark.derdzinski@gmail.com"; fi
  else
    rm nQueryAttempts.txt &>/dev/null
  fi
  echo "$input_from_das" | grep "^/store" >> masterList.txt
done
