#!/bin/bash

if [ ! -d $BASEPATH ]
then
  echo "BASEPATH in GetNumEventsPerFile.sh does not exist!"
fi

> numEventsList.txt

if [ ! -e masterList.txt ]
then
  echo "masterList does not exist, cannot make numEventsList.txt"
else

  readarray -t samples < $BASEPATH/input.txt
  for i in "${samples[@]}"
  do
    input_from_das=`./das_client.py --query="file dataset= $i | grep file.name, file.nevents" --limit=0` #need to use --limit=0 to pick up all files!
    echo "$input_from_das" | grep "status: fail"
    if [ $? == 0 ]
    then 
      echo "failed" >> nQueryAttempts.txt
      if [ -e nQueryAttempts.txt ] && [ "$(cat nQueryAttempts.txt | wc -l)" -gt "60" ]; then echo "DataTupleWarning! Query attempt has failed many times!" | /bin/mail -r "namin@physics.ucsb.edu" -s "[dataTuple] error report" "namin@physics.ucsb.edu, mark.derdzinski@gmail.com"; fi
    else
      rm nQueryAttempts.txt &>/dev/null
    fi
    echo "$input_from_das" | grep "^/store" >> numEventsList.txt
  done

  awk '{print $1}' numEventsList.txt > temp_compare.txt
  comm -13 masterList.txt temp_compare.txt > temp_remove_from_list.txt

  while read line
  do
    remove_filename_escaped=`echo $line | sed 's,/,\\\/,g'`
    sed -i "/$remove_filename_escaped/d" numEventsList.txt
  done < temp_remove_from_list.txt

  rm temp_remove_from_list.txt
  rm temp_compare.txt

fi
