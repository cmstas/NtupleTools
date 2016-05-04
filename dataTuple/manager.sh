#!/bin/bash

#This is the manager that calls all the other pieces.  This should itself be called every N minutes.  

echo " "

#Check arguments, set BASEPATH, JOBTYPE
while getopts ":b:t:" opt; do
  case $opt in
    b) BASEPATH="$OPTARG";;
    t)
       if [ $OPTARG == "cms3" ]
       then
         JOBTYPE="$OPTARG"
       elif [ $OPTARG == "user" ]
       then
         JOBTYPE="$OPTARG"
       else
         echo "Invalid argument \"-t $OPTARG\"." >&2
         echo "Acceptable arguments for -t flag are \"cms3\" and \"user\"."
         exit 1
       fi
    ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1;;
    : ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
    * ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
  esac
done

echo "manager running with options:"
echo "JOBTYPE = $JOBTYPE"
echo "BASEPATH = $BASEPATH"

if [ ! -d $BASEPATH ]
then
  mkdir -p $BASEPATH
fi

if [ ! -d $BASEPATH ]
then
  "BASEPATH $BASEPATH does not exist and could not be created."
  exit 1
fi

#Set CMS3 tag to use
CMS3tag=CMS3_V08-00-04_Data

#Set the global tag to use
GTAG=80X_dataRun2_Prompt_v8
#for reminiAOD data, change this to 74X_dataRun2_reMiniAOD_v0

#State the maxmimum number of events
MAX_NEVENTS=-1 #all events

#Don't allow more than one instance to run
if [ -e $BASEPATH/running.pid ] 
then
  echo "An instance of manager is already running"
  exit 1
else
  #store process info in pid file
  echo "Current time is: `date`" > $BASEPATH/running.pid
  echo "manager running on `hostname`" >> $BASEPATH/running.pid
  echo "PID = $$" >> $BASEPATH/running.pid

  #also store info in log file that catches output
  echo "Current time is: `date`"
  echo "manager running on `hostname`"
  echo "PID = $$"
fi

#Set environment
source /code/osgcode/cmssoft/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc530
pushd .
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc530/cms/cmssw/CMSSW_8_0_6
eval `scramv1 runtime -sh`
popd

#Set PATH
if [[ ":$PATH:" != *":$PWD:"* ]]; then
    PATH="${PATH:+"$PATH:"}$PWD"
fi

cd $PWD

if [ $JOBTYPE == "cms3" ]
then
  #Make sure cms3withCondor exists
  if [ ! -d cms3withCondor ] && [ -d ../cms3withCondor ]
  then
    cp -r ../cms3withCondor .
    sed -i s/isDataTupleCMS3flagged=\"false\"/isDataTupleCMS3flagged=\"true\"/g cms3withCondor/submit.sh
  elif [ ! -d cms3withCondor ]
  then
    echo "Cannot find cms3withCondor"
    exit 1
  fi

  cd cms3withCondor

  #Delete files that stageout in home area
  rm *.root 2> /dev/null 

  #Set PATH to run scripts in here
  if [[ ":$PATH:" != *":$PWD:"* ]]; then
      PATH="${PATH:+"$PATH:"}$PWD"
  fi

  cd ..
fi

#Create submit list
if [ -e submitList.txt ] 
then
  touch submitList.txt
fi

#Create completed list
if [ ! -e $BASEPATH/completedList.txt ] 
then
  touch $BASEPATH/completedList.txt
fi

#change permissions on text files
chmod 777 $BASEPATH/* > /dev/null 2>&1 

#Make sure you're not running too many jobs
. nJobsRunning.sh

#Check the cycle number
if [ -s cycleNumber.txt ]
then 
  while read line
  do
    cycleNumber=$line
  done < cycleNumber.txt
else
  cycleNumber=0
fi
rm cycleNumber.txt > /dev/null
echo $(( $cycleNumber+1 )) > cycleNumber.txt

#Set Output Path
if [ "$JOBTYPE" == "cms3" ]
then
  outputPath="/hadoop/cms/store/user/$USER/dataTuple"
else
  outputPath="/hadoop/cms/store/user/$USER/userjob_test"
fi

#0. Check Proxy
. checkProxy.sh
if [ "$?" == 1 ] 
then
  echo "Aborting -- you don't have a proxy"
  rm -f $BASEPATH/running.pid > /dev/null 2>&1 
  exit 1
fi

#1. DBS query to generate masterList with files on input.txt.
echo "Populating masterList.txt with files for datasets in $BASEPATH/input.txt"
. GenerateMasterList.sh
echo "masterList.txt written"

echo "Getting number of events in each input file"
. GetNumEventsPerFile.sh
echo "numEventsList.txt written"

#2. Diff between masterList and completedList to make notDoneList.
echo "Getting list of files that are on masterList but not on completedList.  Output in notDoneList.txt"

sort $BASEPATH/completedList.txt > temp33.txt

sort $PWD/masterList.txt > temp32.txt

comm -13 temp33.txt temp32.txt > notDoneList.txt
echo "done."

rm temp33.txt
rm temp32.txt

#3. Use condor_q to make heldList. Jobs on the heldList are killed.
echo "Using condor_q to get see which jobs are running"
. removeHeldJobs.sh
echo "runningList.txt and heldList.txt written"

#4. Cycle through files on notDoneList. (DONE)
echo "Cycling through notDoneList.txt"
rm filesToSubmit.txt 2> /dev/null
rm runningList.txt 2> /dev/null
rm idleList.txt 2> /dev/null
rm heldList.txt 2> /dev/null
while read line
do
  currentFile=$line

  #a.  See if job is on failure list.  If yes, continue (unless this is a N%5000 = 0 run).  
  echo "step 4a -- $(date)"
  if [ "$(( $cycleNumber%5000 ))" -eq "0" ]
  then 
    . isOnFailureList.sh $currentFile
    if [ "$?" -eq "1" ]; then continue; fi
  fi
 
  #b. See if each job is on submitList. If no, mark the job for submission and on to step 5. (DONE)
  echo "step 4b"
  . isOnSubmitList.sh $currentFile
  isOnSubmitList=$?
  echo "current file: $currentFile"
  if [ $isOnSubmitList != 1 ] 
  then
    echo "Not on submit list, submitting"
    echo $currentFile >> filesToSubmit.txt
    continue
  fi

  #c. Otherwise, it's on the submitList. Get the jobID from there and see if the job is running.
  echo "step 4c -- $(date)"
  echo "job id: $jobid"
  condor_q $jobid > temp_isRunning.txt
  sed -i '1,4d' temp_isRunning.txt
  sed -i '$d' temp_isRunning.txt
  sed -i '$d' temp_isRunning.txt
  if [ -s temp_isRunning.txt ]; then isRunning=true; else isRunning=false; fi
  echo "isRunning: $isRunning"
  rm temp_isRunning.txt
  echo "executing: . checkStatus.sh $currentFile $jobid"
  . checkStatus.sh $currentFile $jobid

  #d. If job is on run list, check time. If has been running for more than 24 hours, kill it, mark for submission, and on to step 5.
  echo "step 4d -- $(date)"
  if [ $isRunning == true ] 
  then
    echo "starttime: $starttime"
    tooMuchTime=$(python checkTime.py $starttime 2>&1)
    if [ $tooMuchTime == true ]
    then
      condor_rm $jobid
      echo "too much time, submitting"
      echo $currentFile >> filesToSubmit.txt
      continue
    fi
  fi

  #set Output path, make sure it exists
  CMS3tagFragment=`echo $CMS3tag | tr '_' ' ' | awk '{print $2}'`
  outputDir=`echo $currentFile | tr '/' ' ' |  awk '{print $3"_"$4"_"$5"_"$6}'`
  outputDir=$outputDir/$CMS3tagFragment
  echo "outputDir: $outputDir"
  if [ ! -d $outputPath/$outputDir ]
  then
    mkdir -p $outputPath/$outputDir
  fi

  #e. If not on run list, check if the output file is present and valid. If not present and valid, mark for submission and on to step 5.
  echo "step 4e -- $(date)"
  if [ $isRunning == false ] 
  then
    fileName=$(python getFileName.py $currentFile 2>&1)
    #Check for file in hadoop
    #If file not in hadoop, allow 20 mins for transfer.
    #If file is in hadoop, check that it is valid
    if [ ! -e $outputPath/$outputDir/$fileName ] 
    then
      #See when job finished
      currentFile_escaped=`echo $currentFile | sed 's,/,\\\/,g'`
      lineNo=`sed -n /$currentFile_escaped/= submitList.txt`
      whenFinish=`awk -v var="$lineNo" 'NR==var {print $NF}' submitList.txt`
      timeSinceEpoch=`date +%s`
      #add finish time to submit list
      if [ "$whenFinish" == "0" ]
      then
      sed -i "${lineNo}s/0$/$timeSinceEpoch/g" submitList.txt 
      #If it's been less than 20 minutes, don't resubmit
      #This allows for delay in transfer of output
      elif [ `echo $(( ($timeSinceEpoch - $whenFinish) < 1200))` == 1 ]
      then
        echo "Job finished within the last 20 mins for $currentFile but output is missing. Waiting."
      else
        echo "No job running in the last 20 mins and no output file for $currentFile"
        echo "Submitting a new job"
        echo `echo $currentFile | awk ' { print $1 }'` >> filesToSubmit.txt
      fi
    else
      if [ ! -e sweepRoot ] 
      then
        pushd ../sweepRoot
        make clean
        make
        popd
        cp ../sweepRoot/sweepRoot .
      fi
      echo "executing: . checkFile.sh $BASEPATH $outputPath/$outputDir/$fileName $currentFile $JOBTYPE"
      . checkFile.sh $BASEPATH $outputPath/$outputDir/$fileName $currentFile $JOBTYPE
      continue
    fi
  fi

done < notDoneList.txt

#5. Submit all the jobs that have been marked for submission
currentTime=`date +%s`
lineno=0
if [ -e filesToSubmit.txt ] 
then 
  while read line
  do
  let "lineno=$lineno+1"
    currentLine=$line
 
    #a. Check number of times submitted
    echo "step 5a -- $(date)"
    . isOnSubmitList.sh $currentLine
    isOnSubmitList=$?
    if [ "$isOnSubmitList" -eq "1"  ] 
    then
      echo "nTries: $nTries" 
      if [ "$nTries" -gt "10" ] && [ "$nTries" -lt "130" ]
      then
        currentLine_escaped=`echo $currentLine | sed 's,/,\\\/,g'`
        sed -i "/$currentLine_escaped/d" submitList.txt
        let "nTries=$nTries+1"
        echo "$currentLine $jobid $currentTime $nTries 0" >> submitList.txt
        continue
      elif [ "$nTries" -eq "135" ] 
      then
        echo "DataTupleError!  File $currentLine has failed many times." | /bin/mail -r "namin@physics.ucsb.edu" -s "[dataTuple] error report" "namin@physics.ucsb.edu, mark.derdzinski@gmail.com" 
        echo "$currentLine" >> failureList.txt
        continue
      fi
    fi

    #5b. Submit them
    echo "step 5b -- $(date)"
    outputName=$(python getFileName.py $currentLine 2>&1)
    CMS3tagFragment=`echo $CMS3tag | tr '_' ' ' | awk '{print $2}'`
    outputDir=`echo $currentLine | tr '/' ' ' |  awk '{print $3"_"$4"_"$5"_"$6}'`
    outputDir=$outputDir/$CMS3tagFragment
    . submitJob.sh filesToSubmit.txt $currentTime $outputPath/$outputDir $outputName $lineno $CMS3tag $MAX_NEVENTS $GTAG
    echo "submitting!  $currentTime $outputPath/$outputDir $outputName $lineno $CMS3tag $MAX_NEVENTS $GTAG"

    #c. Update submitted list
    echo "step 5c -- $(date)"
    . getJobNumber.sh $currentTime
    . isOnSubmitList.sh $currentLine
    if [ $? != 1 ] 
    then
      . getJobNumber.sh $currentTime
      echo "$currentLine $jobid $currentTime 1 0" >> submitList.txt
      continue
    else
      . getJobNumber.sh $currentTime
      currentLine_escaped=`echo $currentLine | sed 's,/,\\\/,g'`
      sed -i "/$currentLine_escaped/d" submitList.txt
      let "nTries=$nTries+1"
      echo "$currentLine $jobid $currentTime $nTries 0" >> submitList.txt
      continue
    fi
  done < filesToSubmit.txt

fi

#6. Check the post-processing status of jobs. Resubmit post-processing jobs without output.
echo "step 6 -- $(date)"
if [ -d $BASEPATH/mergedLists ]; then
    for dir in $( ls -d $BASEPATH/mergedLists/*/ )
    do
	task=`basename $dir`
	echo "checking PP for $task"
	. checkPP.sh $task $JOBTYPE $CMS3tag
	echo "done checking PP for $task"
   done
fi

if [ "$JOBTYPE" == "cms3" ] && [ "$USER" == "namin" ]
then
  pushd DataTuple-backup
  for theUser in nick mark
  do
    cd $theUser
    cp /nfs-7/userdata/dataTuple/$theUser/completedList.txt . 
    cp -r /nfs-7/userdata/dataTuple/$theUser/mergedLists .
    cp -r /nfs-7/userdata/dataTuple/$theUser/fileLists .
    cd ..
  done
  git add nick mark
  git commit -m "dataTuple commit on `date` by $USER"
  git push origin master
  popd
fi

#monitor script
. monitor.sh

rm -f $BASEPATH/running.pid > /dev/null 2>&1 

echo "done!"
