#!/bin/bash

theDir="run2"

#Set the username
USERNAME="$USER"
if [ "$USERNAME" == "dsklein" ]; then USERNAME="dklein"; fi
if [ "$USERNAME" == "iandyckes" ]; then USERNAME="gdyckes"; fi
if [ "$USERNAME" == "mderdzinski" ]; then USERNAME="mderdzin"; fi
if [ "$USERNAME" == "rclsa" ]; then USERNAME="rcoelhol"; fi

#Needed for later
containsElement () {
  local e
  for e in "${@:2}"; do [[ "$e" == "$1" ]] && return 0; done
  return 1
}

#Check for instructions file
if [ $# -eq 0 ] 
  then 
  echo "No arguments, mate!  Need the instructions file....." 
  return 0 
fi

#Check for lumiBlock file
if [ -e LumisPerJob.txt ] 
then
  grep -r "Aborting" LumisPerJob.txt
  if [ "$?" == "0" ] 
  then
    echo "Aborting!  One of your samples is invalid."
    return 0
  fi
fi

#Print status to screen
echo " " 
echo " " 
echo "All quiet, cap'n!  Monitoring has begun....."
echo "Yer can see yer progrress at uaf-7.t2.ucsd.edu/~$USER/AutoTupleHQ.html"

#File
file=$1

#Get Tag, Global Tag
lineno=0
while read line
do
  let "lineno=$lineno+1"
  if [ "$lineno" == "1" ]; then gtag=$line
  elif [ "$lineno" == "2" ]; then tag=$line
  fi
done < $file

#Environment
source /cvmfs/cms.cern.ch/crab3/crab.sh
export PATH=$PATH:`pwd`
eval `scramv1 runtime -sh`

#Make sure public_html directory exists
if [ ! -d ~/public_html ] 
then
  mkdir ~/public_html
fi

#Pirate picture
cp crabPic.png /home/users/$USER/public_html/crabPic.png &>/dev/null
chmod a+r ~/public_html/crabPic.png

#Store WHICHDONE with TRUE when job is finished (so don't keep checking on it)
WHICHDONE=()
NCRABREDO=()
NREDOPP=()
NEVTS=()
while read p 
do
  WHICHDONE+=("false")
  NCRABREDO+=(0)
  NREDOPP+=(0)
  NEVTS+=(0)
done < $file
WHICHDONE[0]="done"
WHICHDONE[1]="done"

#Remove HTML file
if [ -e AutoTupleHQ.html ]; then rm AutoTupleHQ.html; fi

#Make directory for crab status logs
if [ ! -d crab_status_logs ]; then mkdir crab_status_logs; fi

#shouldContinue to go false when all jobs are done
shouldContinue="true"

#use the correct sample directory which we get from twiki.py
if [ -e ../../theDir.txt ]; then theDir=$(cat ../../theDir.txt); fi

#remove old post-processing logs
rm crab_status_logs/pp.txt 2>/dev/null 
rm crab_status_logs/copy.txt 2>/dev/null 

#variable so we do one last loop
last_loop=0

#main loop
while [ "$last_loop" -lt "2"  ] 
do
  #Delete the existing one 
  if [ -e AutoTupleHQ.html ]; then rm AutoTupleHQ.html; fi

  #Date & Time
  dateAG=`date +%D`
  timeAG=`date +%r`

  #Output the header stuff
  touch AutoTupleHQ.html
  echo '<HTML>' > AutoTupleHQ.html
  echo '<meta http-equiv="refresh" content="15">' >> AutoTupleHQ.html
  echo '<body>' >> AutoTupleHQ.html 
  echo '<img src="crabPic.png" width="50%" height="50%"> <BR>' >> AutoTupleHQ.html
  echo "Last updated at $timeAG on $dateAG <BR> <BR>" >> AutoTupleHQ.html

  #Loop over files and do status check 
  fileNumber="0"
  while read line
  do
    #Skip the first two lines
    if [ "$fileNumber" == "0" ]
    then 
      gtag=$line
      let "fileNumber += 1"
      continue 
    elif [ "$fileNumber" == "1" ]
    then
      let "fileNumber += 1"
      CMS3tag=$line
      continue 
    fi

    #Get the variables:
    inputDS=`echo $line | awk '{print $1}'`
    temp2=`echo ${inputDS//\//_} | cut -c 2-`
    filename=${temp2%_*}
    short_filename=`echo $line | awk '{print $1}' | sed 's/\//\ /g' | awk '{print $1}'`
    xsec=`echo $line | awk '{print $2}'`
    kfact=`echo $line | awk '{print $3}'`
    filtEff=`echo $line | awk '{print $4}'`
    isData=`echo $line | awk '{print $5}'`
    sparms=`echo $line | awk '{print $6}'`

    #Calculate directory names
    temp3=`echo ${inputDS//\//_} | cut -c 2-`
    crab_filename=${temp3%_*}
    crab_filename=`echo $crab_filename | cut -c 1-100`
    status_filename="crab_status_logs/${crab_filename}_log.txt"
    tagDir=`echo $CMS3tag | cut -c 6-`

    if   [[ $crab_filename == *"50ns"* ]]; then theDir="run2_50ns"
    elif [[ $crab_filename == *"RunIISpring15MiniAODv2-FastAsympt25ns"* ]] ; then theDir="run2_fastsim"
    elif [[ $crab_filename == *"RunIISpring15FSPremix"* ]] ; then theDir="run2_fastsim"
    elif [[ $crab_filename == *"RunIISpring15MiniAODv2"* ]] ; then theDir="run2_25ns_MiniAODv2"
    elif [[ $crab_filename == *"25ns"* ]]; then theDir="run2_25ns"
    else :
    fi

    #if crab directory doesn't exist (i.e. was added), make and submit crab jobs
    if [ ! -d crab_${crab_filename} ] && [ "${WHICHDONE[$fileNumber]}" != "invalid" ] && [ ! -e crab_status_logs/noCrab_${crab_filename}.txt ]
    then
      echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
      if [ "${NCRABREDO[$fileNumber]}" == "" ] 
      then
        NCRABREDO+=(0)
        NREDOPP+=(0)
        NEVTS+=(0)
      elif [ "${NCRABREDO[$fileNumber]}" -lt "2" ] 
      then
        echo '<font color="red"> &nbsp; &nbsp; <b> New crab task! Submitting..... <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
        ./FindLumisPerJobNoDAS.sh $inputDS > LumisPerJob_temp.txt
        numLumiPerJob=`less LumisPerJob_temp.txt | awk '{print $1}'`
        rm LumisPerJob_temp.txt
        NCRABREDO[$fileNumber]=$(( ${NCRABREDO[$fileNumber]} + 1 ))
        if [ "$numLumiPerJob" == "Aborting." ]
        then
          WHICHDONE[$fileNumber]="invalid"
          isDonePP="invalid"
          echo "$filename invalid" >> crab_status_logs/pp.txt
        elif [ "$sparms" == "" ]
        then
          echo "gonna do this: python makeCrab3Files.py -CMS3cfg skeleton_cfg.py -d $inputDS -t $CMS3tag -gtag $gtag -isData $isData -lumisPerJob $numLumiPerJob"
          python makeCrab3Files.py -CMS3cfg skeleton_cfg.py -d $inputDS -t $CMS3tag -gtag $gtag -isData $isData -lumisPerJob $numLumiPerJob &> /dev/null
        else
          python makeCrab3Files.py -CMS3cfg skeleton_cfg.py -d $inputDS -t $CMS3tag -gtag $gtag -isData $isData -lumisPerJob $numLumiPerJob -sParms $sparms &> /dev/null
        fi
        crab submit -c cfg/$crab_filename.py &> /dev/null
      else
        echo '<font color="red"> &nbsp; &nbsp; <b> Avast!  Blasted Crab Task Failed even after we resubmitted!! Giving up..... <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
      fi
      let "fileNumber += 1"
      continue
    fi

    #If sample is invalid, done
    if [ "${WHICHDONE[$fileNumber]}" == "invalid" ]
    then
      echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
      echo "<font color=\"red\"> &nbsp; &nbsp; <b> Son of a biscuit eater!  THIS SAMPLE IS INVALID!!  <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
      let "fileNumber += 1"
      continue
    fi
    
    #date and time
    if [ -e crab_${crab_filename}/crab.log ] 
    then
      grep -m 1 -r "Looking up detailed status of task" crab_${crab_filename}/crab.log | awk '{print $10}' | cut -c 1-13 > crab_${crab_filename}/jobDateTime.txt
      dateTime=`less crab_${crab_filename}/jobDateTime.txt`
    else
      dateTime=`ls -lthr --ignore=$CMS3tag /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/ | awk '{print $NF}' | tail -1`
    fi

    if [ ! -d crab_${crab_filename} ] && [ -e crab_status_logs/noCrab_${crab_filename}.txt ] && [ ${WHICHDONE[$fileNumber]} == "false" ]
    then
      WHICHDONE[$fileNumber]="true"
      nice -n 10  python process.py $file $fileNumber $dateTime &
      let "fileNumber += 1"
      continue
    fi

    #If already finished......
    if [ "${WHICHDONE[$fileNumber]}" == "done" ] || [ "${WHICHDONE[$fileNumber]}" == "notPP" ]
    then
      if [ "${NEVTS[$fileNumber]}" -gt "0" ]
      then
        nIn="${NEVTS[$fileNumber]}"
        nOut="${NEVTS[$fileNumber]}"
      else
        root -b -l -q numEventsROOT.C\(\"/hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/0000/\"\) > crab_status_logs/temp2.txt 2>&1
        nIn=`grep -r "nEntries" crab_status_logs/temp2.txt | tail -1 | awk '{print $NF}'`
        root -b -l -q numEventsROOT.C\(\"/hadoop/cms/store/group/snt/$theDir/$filename/$tagDir\"\) > crab_status_logs/temp.txt 2>&1
        grep -r "trying to recover" crab_status_logs/temp.txt &> /dev/null
        if [ "$?" == "0" ] 
        then 
          nOut="Error in Copying!" 
          copyProblem="1"
        else
          nOut=`grep -r "nEntries" crab_status_logs/temp.txt | tail -1 | awk '{print $NF}'`
          copyProblem="0"
          if [ "$nIn" == "$nOut" ] 
          then
            NEVTS[$fileNumber]=$nIn
          fi
        fi
      fi
      #check for nOut > nIn
      if [ "$copyProblem" == "0" ] && [ "$nOut" -gt "$nIn" ] 
      then
        echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
        if [ "${NREDOPP[$fileNumber]}" -lt "3" ] 
        then
          echo "<font color=\"red\"> &nbsp; &nbsp; <b> Garrr!  More events out than in!  Post-processing again.... <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
          rm /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir/*.root 2> /dev/null
          NREDOPP[$fileNumber]=$(( ${NREDOPP[$fileNumber]} + 1 ))
          WHICHDONE[$fileNumber]="true" 
        else
          echo "<font color=\"red\"> &nbsp; &nbsp; <b> Garrr!  More events out than in, even after redoing it a few times!  Giving up.... <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
        fi
      fi
    fi
    if [ "${WHICHDONE[$fileNumber]}" == "done" ] 
    then
      if [ -e crab_status_logs/copy.txt ]
      then
        echo "  " >> AutoTupleHQ.html
        if [ "$copyProblem" == "0" ] && [ "$(( 10 * $nOut))" -gt "$nIn" ] 
        then
          echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
          echo "<font color=\"blue\"> &nbsp; &nbsp; <b> This task be finished!!!! nEventsIn: $nIn nEventsOut: $nOut <font color=\"black\"></b><BR>" >> AutoTupleHQ.html
          echo "<font color=\"black\"> &nbsp; &nbsp; <b> Location: /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
          echo "$filename $nIn $nOut" >> crab_status_logs/isdone.txt
          let "fileNumber += 1"
          continue
        elif [ "$copyProblem" != "0" ] 
        then
          echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
          numbers=`grep -r "$filename" crab_status_logs/copy.txt | grep "trying to recover" | awk '{print $5}' | sed 's/\//\ /g' | awk '{print $NF}' | sed 's/_/\ /g' | awk '{print $NF}' | sed 's/\./\ /g' | awk '{print $1}'`
          for i in $numbers
          do
            rm /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir/merged_ntuple_$i.root
          done
          mv /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir/*.root /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/CMS3_$tagDir/merged/
          WHICHDONE[$fileNumber]="true"
          echo "<font color=\"red\"> &nbsp; &nbsp; <b> Shiver me timbers!  There was a problem copying this file.  File $i is corrupt. Fixing..... nEventsIn: $nIn.  <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
          let "fileNumber += 1"
          continue
        fi
      else
        WHICHDONE[$fileNumber]="true"
      fi
    elif [ "${WHICHDONE[$fileNumber]}" == "notPP" ] 
    then
      if [ "$copyProblem" == "0" ] 
      then
        echo "  " >> AutoTupleHQ.html
        echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
        echo "<font color=\"blue\"> &nbsp; &nbsp; <b> This task be finished!!!! Did not post-process; dirrrectory already existed on the cmstas hadoop.... </b> <BR> &nbsp; &nbsp;  If yerr want to postprocess, delete that directory and restart monitoring with . monitor.sh instructions.txt  <font color=\"black\"><BR> &nbsp; &nbsp; nEventsIn: $nIn.  nEventsOut: $nOut. <BR> <BR>" >> AutoTupleHQ.html
        let "fileNumber += 1"
        continue
      elif [ "$copyProblem" != "0" ] 
      then
          echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
          numbers=`grep -r "$filename" crab_status_logs/copy.txt | grep "trying to recover" | awk '{print $5}' | sed 's/\//\ /g' | awk '{print $NF}' | sed 's/_/\ /g' | awk '{print $NF}' | sed 's/\./\ /g' | awk '{print $1}'`
          rm /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir/*.root
          WHICHDONE[$fileNumber]="true"
          echo "<font color=\"red\"> &nbsp; &nbsp; <b> Shiver me timbers!  Did not post-process, but found a corrupt file in the output dirrrectory on the cmstas hadoop.... </b> <BR> Deleting everything and redoing it.  nEventsIn: $nIn.  <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
          nice -n 10 python process.py $file $fileNumber $dateTime &
          let "fileNumber += 1"
          continue
      fi
    fi

    #if crab has finished but post-processing has not, check on post-processing
    if [ "${WHICHDONE[$fileNumber]}" == "true" ] 
    then
      root -b -l -q numEventsROOT.C\(\"/hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/0000/\"\) > crab_status_logs/temp2.txt 2>&1
      nIn=`grep -r "nEntries" crab_status_logs/temp2.txt | tail -1 | awk '{print $NF}'`
      #Print header
      echo "  " >> AutoTupleHQ.html
      echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html
       
      #If there is no pp log, print status message
      if [ ! -e crab_status_logs/pp.txt ]
      then
        echo '<font color="blue"> &nbsp; &nbsp; <b> Post-Processing is underway!  No status available yet....  <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
      #Otherwise, get filename and check log to see if there's any trace of it
      else
        grep -r "$filename" crab_status_logs/pp.txt &> /dev/null
        foundIt="$?"
    
        #Either way, print status message
        if [ "$foundIt" != "0" ] 
        then
          echo '<font color="blue"> &nbsp; &nbsp; <b> Post-Processing is underway!  No status available yet....  <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
        else
          isDonePP=`grep -r "$filename" crab_status_logs/pp.txt | tail -1 | awk '{print $2}'`
          if [ "$isDonePP" == "done" ] 
          then
            nOut=`grep -r "nEntries" crab_status_logs/temp.txt | tail -1 | awk '{print $NF}'`
            echo "<font color=\"blue\"> &nbsp; &nbsp; <b> Post-Processing is finished!  nEventsIn: $nIn  nEventsOut: $nOut  <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
            WHICHDONE[$fileNumber]="done"
          elif [ "$isDonePP" == "alreadyThere" ] 
          then
            if [ ! -e /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir/merged_ntuple_1.root ]
            then
              echo "Alex, here's the problem.  alreadyThere is true, but I don't see anything here: /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir."
            else
              echo "Alex, alreadyThere is true, and I see stuff here: /hadoop/cms/store/group/snt/$theDir/$filename/$tagDir."
              echo "<font color=\"blue\"> &nbsp; &nbsp; <b> Not going to postprocess; already exists on hadoop....  <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
              WHICHDONE[$fileNumber]="notPP"
            fi
          else
            nEntriesIn=`grep -r "$filename" crab_status_logs/pp.txt | tail -1 | awk '{print $2}'`
            nFinished=`grep -r "$filename" crab_status_logs/pp.txt | tail -1 | awk '{print $3}'`
            nTotal=`grep -r "$filename" crab_status_logs/pp.txt | tail -1 |  awk '{print $4}'`
            echo "<font color=\"blue\"> &nbsp; &nbsp; <b> Post-Processing is underway!  nEventsIn: $nIn.  Current progress: $nFinished/$nTotal  <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
          fi
        fi
      fi
 
      #Increment file nmuber, continue
      let "fileNumber += 1"
      continue
    fi

    #Output name with hyperlink to log
    echo "  " >> AutoTupleHQ.html
    echo "<A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/${crab_filename}_log.txt\"> ${crab_filename}</A><BR>" >> AutoTupleHQ.html

    #Do the check, upload result
    crab status crab_$crab_filename --long > $status_filename
    cp $status_filename /home/users/$USER/public_html/${crab_filename}_log.txt >/dev/null

    #If status is done, we're done
    grep -r "Task status" $status_filename | grep "COMPLETED" &>/dev/null
    if [ "$?" == "0" ]
    then 
      #Double check to make sure crab didn't fuck it up
      numberOfRunnings=`grep -r "running" $status_filename | grep "running" | wc -l`
      if [ "$numberOfRunnings" == "0" ] 
      then  
        isDone="1" 
      else
        isDone="0"
      fi
    else 
      isDone="0"
    fi
    if [ "$isDone" == "1" ]
    then
      echo '<font color="blue"> &nbsp; &nbsp; <b> Ready for Post-Processing!!  <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
      WHICHDONE[$fileNumber]="true"
      numDirs=`ls /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/ | wc -l` 
      while [ "$numDirs" -gt "1" ]
      do
        if [ ! -e /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/000$(( $numDirs-1 )) ]; then echo "does not exist, not copying"; numDirs=$(( $numDirs - 1 )); continue; fi
        #First the logs
        mv /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/000$(( $numDirs-1 ))/log/* /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/0000/log/
        rmdir /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/000$(( $numDirs-1 ))/log
        #Then the files
        mv /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/000$(( $numDirs-1 ))/* /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/0000/
        rmdir /hadoop/cms/store/user/$USERNAME/$short_filename/crab_${crab_filename}/$dateTime/000$(( $numDirs-1 ))
        numDirs=$(( $numDirs - 1 )) 
      done
      nice -n 10 python process.py $file $fileNumber $dateTime &
      let "fileNumber += 1"
      continue
    fi
   
    #If status is failed, delete and resubmit
    grep -r "Task status" $status_filename | grep "FAILED" &>/dev/null 
    isFailed="$?"
    #Also failed if no WD and not because already successfully crabbed
    if [ "$isFailed" != "0" ]
    then 
      grep -r "not found" $status_filename | grep "Working directory for task" &>/dev/null 
      result="$?"
      if [ "$result" == "0" ] && [ -e "crab_status_logs/noCrab_${crab_filename}.txt" ] 
      then
        WHICHDONE[$fileNumber]="true"
        nice -n 10 python process.py $file $fileNumber $dateTime &
        let "fileNumber += 1"
        echo "<font color=\"blue\"> &nbsp; &nbsp; <b> Trying to proceed without redoing the crab stuff.  date_time is $dateTime..... <font color=\"black\"></b><BR><BR>" >> AutoTupleHQ.html
        continue
      elif [ "$result" == "0" ] 
      then
        isFailed="$result"
      fi
    fi
#    #Also failed if no requestcache file
#    if [ "$isFailed" != "0" ]
#    then 
#      grep -r "Cannot find .requestcache file inside the working directory for task" $status_filename &>/dev/null 
#      isFailed="$?"
#    fi
#    if [ "$isFailed" == "0" ]
#    then
#      if [ "${NCRABREDO[$fileNumber]}" -lt "2" ] 
#      then
#        echo '<font color="red"> &nbsp; &nbsp; <b> Avast!  Blasted Crab Task Failed!! Resubmitting..... <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
#        rm -rf crab_$crab_filename &> /dev/null
#        ./FindLumisPerJobNoDAS.sh $inputDS > LumisPerJob_temp.txt
#        numLumiPerJob=`less LumisPerJob_temp.txt | awk '{print $1}'`
#        rm LumisPerJob_temp.txt
#        NCRABREDO[$fileNumber]=$(( ${NCRABREDO[$fileNumber]} + 1 ))
#        if [ "$sparms" == "" ]
#        then
#          python makeCrab3Files.py -CMS3cfg skeleton_cfg.py -d $inputDS -t $CMS3tag -gtag $gtag -isData $isData -lumisPerJob $numLumiPerJob &> /dev/null
#        else
#          python makeCrab3Files.py -CMS3cfg skeleton_cfg.py -d $inputDS -t $CMS3tag -gtag $gtag -isData $isData -lumisPerJob $numLumiPerJob -sParms $sparms &> /dev/null
#        fi
#        crab submit -c cfg/$crab_filename.py &> /dev/null
#      else
#        echo '<font color="red"> &nbsp; &nbsp; <b> Avast!  Blasted Crab Task Failed even after we resubmitted!! Giving up..... <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
#      fi
#      let "fileNumber += 1"
#      continue
#    fi

    #If status is queued, we're done
    grep -r "QUEUED" $status_filename &>/dev/null
    isQueued="$?"
    if [ "$isQueued" == "0" ]
    then
      # if jobs queued, write a file that has timestamp for initial queue time
      queued_filename="${status_filename%%.txt}.queued"
      if [ -e $queued_filename ]; then
        # if queued file already exists, then compare it with current timestamp
        timeThen=$(cat $queued_filename)
        timeNow=$(date +%s)
        minsQueued=$(( ($timeNow-$timeThen)/60 ))
        echo '<font color="blue"> &nbsp; &nbsp; <b> Task has been queued for '"$minsQueued"' minutes!! <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
        if [ $minsQueued -gt $((12*60)) ]; then
          echo '<font color="red"> &nbsp; &nbsp; <b> Task has been queued for more than 12 hours. Not going to do anything (commented out automatic resubmission because of general crab ridiculousness)!! <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
          #rm -rf crab_$crab_filename &> /dev/null
          #./FindLumisPerJobNoDAS.sh $inputDS > LumisPerJob_temp.txt
          #numLumiPerJob=`less LumisPerJob_temp.txt | awk '{print $1}'`
          #rm LumisPerJob_temp.txt
          #NCRABREDO[$fileNumber]=$(( ${NCRABREDO[$fileNumber]} + 1 ))
          #if [ "numLumiPerJob" == "Aborting." ]
          #then
          #  WHICHDONE[$fileNumber]="invalid"
          #  isDonePP="invalid"
          #  echo "$filename invalid" >> crab_status_logs/pp.txt
          #elif [ "$sparms" == "" ]
          #then
          #  python makeCrab3Files.py -CMS3cfg skeleton_cfg.py -d $inputDS -t $CMS3tag -gtag $gtag -isData $isData -lumisPerJob $numLumiPerJob &> /dev/null
          #  crab submit -c cfg/$crab_filename.py &> /dev/null
          #else
          #  python makeCrab3Files.py -CMS3cfg skeleton_cfg.py -d $inputDS -t $CMS3tag -gtag $gtag -isData $isData -lumisPerJob $numLumiPerJob -sParms $sparms &> /dev/null
          #  crab submit -c cfg/$crab_filename.py &> /dev/null
          #fi
        fi
      else
        echo '<font color="blue"> &nbsp; &nbsp; <b> Task is Queued!! <font color="black"></b><BR><BR>' >> AutoTupleHQ.html
        echo "$(date +%s)" >> $queued_filename
      fi

      let "fileNumber += 1"
      continue
    fi

    #Otherwise, get distribution
    nUnsubmitted=`grep -r "unsubmitted" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    nIdle=`grep -r "idle" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    nRunning=`grep -r "running" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    nFailed=`grep -r "failed" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    nTransferring=`grep -r "transferring" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    nTransferred=`grep -r "transferred" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    nCooloff=`grep -r "cooloff" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    nFinished=`grep -r "finished" $status_filename | grep "%" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $1}'`
    denominator=`grep -r "%" $status_filename | grep ")" | grep "Jobs status:" | awk '{print $NF}' | sed 's/(//' | sed 's/)//' | sed 's/\//\ /' | awk '{print $2}'`

    #If any are empty, set them to 0
    if [ "$nUnsubmitted"  == "" ]; then nUnsubmitted="0"; fi
    if [ "$nIdle"         == "" ]; then nIdle="0"; fi
    if [ "$nRunning"      == "" ]; then nRunning="0"; fi
    if [ "$nFailed"       == "" ]; then nFailed="0"; fi
    if [ "$nTransferring" == "" ]; then nTransferring="0"; fi
    if [ "$nTransferred"  == "" ]; then nTransferred="0"; fi
    if [ "$nCooloff"      == "" ]; then nCooloff="0"; fi
    if [ "$nFinished"     == "" ]; then nFinished="0"; fi
    if [ "$denominator"   == "" ]; then denominator="0"; fi
  
    #Print the distribution
    echo "&nbsp; &nbsp; unsubmitted: $nUnsubmitted/$denominator <BR>" >> AutoTupleHQ.html
    echo "&nbsp; &nbsp; idle: $nIdle/$denominator <BR>" >> AutoTupleHQ.html
    echo "&nbsp; &nbsp; running: $nRunning/$denominator <BR>" >> AutoTupleHQ.html
    if [ "$nFailed" == "0" ] 
    then
      echo "&nbsp; &nbsp; failed: $nFailed/$denominator <BR>" >> AutoTupleHQ.html
    else
      echo "<font color="red"> &nbsp; &nbsp; <b> failed: $nFailed/$denominator <font color="black"> </b> <BR>" >> AutoTupleHQ.html
    fi
    echo "&nbsp; &nbsp; transferring: $nTransferring/$denominator <BR>" >> AutoTupleHQ.html
    echo "&nbsp; &nbsp; transferred: $nTransferred/$denominator <BR>" >> AutoTupleHQ.html
    echo "&nbsp; &nbsp; cooloff: $nCooloff/$denominator <BR>" >> AutoTupleHQ.html
    echo "&nbsp; &nbsp; <b> finished: $nFinished/$denominator </b> <BR><BR>" >> AutoTupleHQ.html

    #If less than 25% of the jobs have failed, force resubmission
    if [ "$denominator" -gt "$(( 4*$nFailed ))" ] && [ "$nFailed" -gt "0" ] 
    then
      crab resubmit crab_$crab_filename
      #Reset queuetime file
      queued_filename="${status_filename%%.txt}.queued"
      if [ -e $queued_filename ]; then rm $queued_filename; fi
    fi

    #Increase counter
    let "fileNumber += 1"
    
  done < $file 

  #Footer for HTML file
  echo '</body>' >> AutoTupleHQ.html 
  echo '</HTML>' >> AutoTupleHQ.html 

  #Upload HTML file
  cp AutoTupleHQ.html /home/users/$USER/public_html/AutoTupleHQ.html &>/dev/null

  #Check if totally done, sleep and continue looping if not
  containsElement "false" "${WHICHDONE[@]}"
  result1=$?
  containsElement "true" "${WHICHDONE[@]}"
  result2=$?
  if [ "$result1" == "0" ] || [ "$result2" == "0" ]; then shouldContinue="true"; else shouldContinue="false"; fi 
  if [ "$shouldContinue" == "true" ]; then sleep 30; fi
  if [ "$shouldContinue" == "false" ]; then let "last_loop=$last_loop+1"; sleep 5; fi
  
done
