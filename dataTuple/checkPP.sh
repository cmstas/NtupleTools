#checks the status of post-processing for a given task and copies finished files, adds them to the donePP.txt list.

#Takes arguments:
#1) taskname
#2) JOBTYPE

taskname=$1
JOBTYPE=$2
CMS3tagFragment=`echo $3 | tr '_' ' ' | awk '{print $2}'`

if [ "$JOBTYPE" == "cms3" ]
then
    jobDir=dataTuple
elif [ "$JOBTYPE" == "user" ]
then
    jobDir=userjob_test
else
  echo "JOBTYPE not recognized"
  exit 1
fi
mergedDir="/hadoop/cms/store/user/$USER/$jobDir/$taskname/merged"
target="/hadoop/cms/store/group/snt/run2_data/$taskname/merged/$CMS3tagFragment"

if [ ! -d $BASEPATH ]
then
  echo "BASEPATH in checkPP.sh does not exist!"
fi

if [ ! -d $target ]
then
  mkdir -p $target
  chmod a+w $target
fi

#make donelist
if [ ! -e donePP.txt ]
then
  touch donePP.txt
fi

#Run condor_q to get list of running jobs
condor_q $USER > temp_status.txt
sed -i '1,4d' temp_status.txt
sed -i '$d' temp_status.txt
sed -i '$d' temp_status.txt

#Delete old test files
rm heldPPList.txt 2>/dev/null

#Read condor_q output to fill lists.  
while read line
do
  if [ `echo $line | awk '{ print $6 }'` == "H" ]
  then
    echo `echo $line | awk '{ print $1 }'` >> heldPPList.txt
  fi	
done < temp_status.txt    
rm temp_status.txt

#Delete held jobs
if [ -e heldPPList.txt ]
then
  while read line
  do
    condor_rm $line
  done < heldPPList.txt
  rm heldPPList.txt
fi

counter="1"

while [ -e $BASEPATH/mergedLists/$taskname/metaData_$counter.txt ]
do
  # echo "checking $BASEPATH/mergedLists/$taskname/metaData_$counter.txt"
  mergeFile="$mergedDir/merged_ntuple_$counter.root"
  # echo "mergeFile is $mergeFile"
  
  #grep donePP.txt to see if PP already finished and mv'ed to hadoop
  grep "$mergeFile" donePP.txt > /dev/null
  isDone=$?
  if [ $isDone == 0 ]
  then
    counter=$[$counter+1]
    continue
  fi
  isRunning=`condor_q $USER -l | grep $taskname/metaData_$counter.txt &>/dev/null; echo $?`
  if [ $isRunning == 0 ]
  then
    counter=$[$counter+1]
    continue
  fi
  
  #FIXME: May want to check timestamp when submitted and kill if "stuck"
  #grep $mergeFile submitPPList.txt > /dev/null
  #wasSubmitted=$?
  
  if [ -e delayList.txt ]
  then
    grep $mergeFile delayList.txt > /dev/null
    wasDelayed=$?
  else
    wasDelayed=1
  fi

  mergeFileEsc=`echo $mergeFile | sed 's,/,\\\/,g'`
  
  if [ -e $mergeFile ]
  then 
    if [ $wasDelayed == 0 ]
    then 
      sed -i "/$mergeFileEsc/d" delayList.txt
      sed -i "/$mergeFileEsc/d" submitPPList.txt
      root -b -q -l "checkNumMergedEvents.C (\"$BASEPATH/mergedLists/$taskname/merged_list_$counter.txt\",\"$mergeFile\")"
      NumMergedEventsConsistent=$?
      if [ $NumMergedEventsConsistent == 0 ]
      then
        if [ "$JOBTYPE" != "user" ]
        then
          echo "moving $mergeFile to $target"
          echo "I am here: $PWD"
          mv $mergeFile $target
          pushd ../makeJSON
          echo "./makeJSON.sh $mergeFile $BASEPATH" 
          nameAG=`echo $mergeFile | tr '/' ' ' | awk '{print $NF}'`
          ./makeJSON.sh $target/$nameAG $BASEPATH
          popd
        fi
        echo "$mergeFile" >> donePP.txt
      else
        echo "$mergeFile has the wrong number of events. Will delete and resubmit."
        rm $mergeFile
        . submitPPJob.sh $taskName $counter $JOBTYPE
        submitTime=`date +%s`
        echo "/hadoop/cms/store/user/$USER/dataTuple/$taskName/merged/merged_ntuple_$counter.root $submitTime" >> submitPPList.txt
      fi
    else
      echo "$mergeFile exists, but might be copying. Adding to delaylist.txt"
      echo "$mergeFile" >> delayList.txt
    fi
  else
    if [ $wasDelayed == 0 ]
    then 
      sed -i "/$mergeFileEsc/d" delayList.txt
      echo "$mergeFile does not exist! Will resubmit."
      . submitPPJob.sh $taskname $counter $JOBTYPE
      submitTime=`date +%s`
      echo "/hadoop/cms/store/user/$USER/$jobDir/$taskName/merged/merged_ntuple_$counter.root $submitTime" >> submitPPList.txt
    else
      echo "Adding mergeFile to delaylist.txt"
      echo "$mergeFile" >> delayList.txt
    fi
  fi
  counter=$[$counter+1]
done
