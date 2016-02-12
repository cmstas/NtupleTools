#!/bin/bash

unmergedFileDir=$1 
mergeLists=$2
xsec=$3
kfact=$4
efact=$5
#/hadoop/cms/store/user/cgeorge/ttHJetToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8/crab_ttHJetToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v/151217_030407/0000/
#mergeLists=/home/users/cgeorge/NtupleTools/AutoTuple/CMSSW_7_4_14/crab/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1

echo $xsec
echo $kfact
echo $efact
echo " "
echo "unmerged files are in $unmergedFileDir"
echo "Here is the correspondence between unmerged and MINIAOD: "

nFiles=`ls $unmergedFileDir/log | wc -l`

for fileNo in `seq 1 $nFiles`
do
  theLog=`tar xf $unmergedFileDir/log/cmsRun_${fileNo}.log.tar.gz -O`
  theParsedLog=`echo $theLog | tr ' ' '\n' | grep "^root:"`
  
  #Make sure there's only one file in each unmerged
  number=`echo $theParsedLog | tr ' ' '\n' | wc -l`
  if [ "$number" != "3" ] ; then echo "Warning! File $fileNo is Not 3" ; continue ; fi 
  
  #If all good, find the name of the file
  echo $fileNo "`echo $theParsedLog | tr ' ' '\n' | tail -1 | sed 's,/, ,3' | awk '{print $2}' | tr '?' ' ' | awk '{print $1}' `"
done

ls $mergeLists/mergeFiles/mergeLists
nMergeLists=`ls $mergeLists/mergeFiles/mergeLists | wc -l`
echo $nMergeLists
echo "Here is the correspondence between merged and unmerged: " 

for file in `seq 1 $nMergeLists`
do
  n=0
  result=""
  for line in `cat $mergeLists/mergeFiles/mergeLists/merged_list_${file}.txt`
  do
    if [ "$n" -gt 1 ] ; then result+=`echo $line | tr '/' ' ' | awk '{print $NF}'  | tr '_.' ' ' | awk '{print $2}'` ; result+=" " ; fi
    n+=1
  done
  echo "merged file $file: $result"
done
