#!/bin/bash

dataSet=$1
CMS3tag=$2
dateTime=$3

if [[ $dataSet == *"50ns"* ]]
then
  thedir="run2_50ns"
  echo "50"
elif [[ $dataSet == *"RunIISpring15MiniAODv2-FastAsympt25ns"* ]] 
then
  thedir="run2_fastsim"
  echo "25_FS"
elif [[ $dataSet == *"RunIISpring15FSPremix"* ]] 
then
  thedir="run2_fastsim"
  echo "25_FS_v1"
elif [[ $dataSet == *"RunIISpring15MiniAODv2"* ]] 
then
  thedir="run2_25ns_MiniAODv2"
  echo "25_redo"
elif [[ $dataSet == *"25ns"* ]]
then
  thedir="run2_25ns"
  echo "25" 
else
  echo "Aborting!  Can't figure out if 25 or 50 ns!!"
  return 1
fi

myDir=`echo $CMS3tag | cut -c 6-`
longName=`echo $dataSet | awk -F '/' '{print $2 "_" $3}'`
shortName=`echo $dataSet | awk -F '/' '{print $2}'`
lName=`echo $longName | cut -c 1-100`

#Set username
USERNAME="$USER"
if [ "$USERNAME" == "dsklein" ]; then USERNAME="dklein"; fi
if [ "$USERNAME" == "iandyckes" ]; then USERNAME="gdyckes"; fi
if [ "$USERNAME" == "mderdzinski" ]; then USERNAME="mderdzin"; fi
if [ "$USERNAME" == "rclsa" ]; then USERNAME="rcoelhol"; fi

#this is where the unmerged files are
unmerged=/hadoop/cms/store/user/$USERNAME/$shortName/crab_$lName/$dateTime/0000/

#check CMS3
nRedo=`grep -r "Too few merged events\!" autoTupleLogs/copy_log_$shortName.log | awk '{print $5}'`
root -b -q checkCMS3.C\(\"/hadoop/cms/store/user/$USERNAME/$shortName/crab_$lName/$CMS3tag/merged/\",\"$unmerged\",0,0,\"$dataSet\"\) > autoTupleLogs/copy_log_$shortName.log
nProblems=`grep -r "Problems found:" autoTupleLogs/copy_log_$shortName.log | awk '{print $3}'`
grep -r  "Too few merged events!" autoTupleLogs/copy_log_$shortName.log &>/dev/null
mergingProblems=$?

if [ "$nProblems" == "0" ]
then
  #Do the copy
  mkdir /hadoop/cms/store/group/snt/$thedir/$longName 2> /dev/null
  mkdir /hadoop/cms/store/group/snt/$thedir/$longName/$myDir 2> /dev/null
  mv /hadoop/cms/store/user/$USERNAME/$shortName/crab_$lName/$CMS3tag/merged/*.root /hadoop/cms/store/group/snt/$thedir/$longName/$myDir/
  echo "mv /hadoop/cms/store/user/$USERNAME/$shortName/crab_$lName/$CMS3tag/merged/*.root /hadoop/cms/store/group/snt/$thedir/$longName/$myDir/"
  cp $unmerged/metadata.txt /hadoop/cms/store/group/snt/$thedir/$longName/$myDir/metadata.txt
  echo "$longName" >> crab_status_logs/copy.txt
  root -b -l -q numEventsROOT.C\(\"/hadoop/cms/store/group/snt/$thedir/$longName/$myDir\"\) >> crab_status_logs/copy.txt 2>&1
elif [ "$nProblems" == "1" ] && [ "$mergingProblems" == "0" ]
then
  #Resubmit merge jobs
  if [ "$nRedo" == "" ]; then nRedo2=0; else nRedo2=$(( $nRedo + 1 )); fi
  sed -i "s/Too few merged events! $nRedo/Too few merged events! $nRedo2/" autoTupleLogs/copy_log_$shortName.log 
  rm /hadoop/cms/store/user/$USERNAME/$shortName/crab_$lName/$CMS3tag/merged/*.root
  #echo "Should resubmit here!! Fix me!!" 
  fileNumber=$(grep -n $dataSet *.txt | grep -v updateTwiki | cut -d ':' -f2)
  fileNumber=$((fileNumber - 1))
  file=$(grep -n $dataSet *.txt | grep -v updateTwiki | cut -d ':' -f1)
  echo "python process.py $file $fileNumber $dateTime &"
  python process.py $file $fileNumber $dateTime &
else
  #Copy to bad dir
  mkdir /hadoop/cms/store/group/snt/$thedir/$longName 2> /dev/null
  mkdir /hadoop/cms/store/group/snt/$thedir/$longName/$myDir 2> /dev/null
  mkdir /hadoop/cms/store/group/snt/$thedir/$longName/$myDir/bad/ 2> /dev/null
  #mv /hadoop/cms/store/user/$USERNAME/$shortName/crab_$lName/$CMS3tag/merged/*.root /hadoop/cms/store/group/snt/$thedir/$longName/$myDir/bad/
  echo "mv /hadoop/cms/store/user/$USERNAME/$shortName/crab_$lName/$CMS3tag/merged/*.root /hadoop/cms/store/group/snt/$thedir/$longName/$myDir/bad/"
  echo "$longName" >> crab_status_logs/copy.txt
  root -b -l -q numEventsROOT.C\(\"/hadoop/cms/store/group/snt/$thedir/$longName/$myDir\"\) >> crab_status_logs/copy.txt 2>&1
  #mv autoTupleLogs/copy_log_$shortName.log /hadoop/cms/store/group/snt/$thedir/$longName/$myDir/bad/
fi
