#!/bin/bash

sampleName=$1
unmergedFileDir=$2 
mergeLists=$3
xsec=$4
kfact=$5
efact=$6
cms3tag=$7
gtag=$8
sparms=$9
#/hadoop/cms/store/user/cgeorge/ttHJetToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8/crab_ttHJetToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v/151217_030407/0000/
#mergeLists=/home/users/cgeorge/NtupleTools/AutoTuple/CMSSW_7_4_14/crab/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1

echo "sampleName: $sampleName"
echo "xsec: $xsec"
echo "k-fact: $kfact"
echo "e-fact: $efact"
echo "cms3tag: $cms3tag"
echo "gtag: $gtag"
echo "sparms: $sparms"
echo " "
echo "unmerged files are in: $unmergedFileDir"
echo " "
echo "command used to generate this: ./makeMetadata.sh $@"
echo " "
echo " " 
echo "Here be the correspondence between unmerged and MINIAOD: "

nFiles=`ls $unmergedFileDir/log | wc -l`

for fileNo in `seq 1 $nFiles`
do
  theLog=`tar xf $unmergedFileDir/log/cmsRun_${fileNo}.log.tar.gz -O`
  theParsedLog=`echo $theLog | tr ' ' '\n' | tr '?' '\n' | grep ".root$" | grep -v "[\<\(\"\']" | grep -v "^/"`
  
  #Make sure there's only one file in each unmerged
  check1=`echo $theParsedLog | tr ' ' '\n' | head -1 | awk '{print $NF}' | sed 's,/, ,3' | awk '{print $2}' | tr '?' ' ' | awk '{print $1}' | sed 's,.*/store,/store,' | sed 's,^/,,' `
  check2=`echo $theParsedLog | tr ' ' '\n' | tail -1 | awk '{print $NF}' | sed 's,/, ,3' | awk '{print $2}' | tr '?' ' ' | awk '{print $1}' | sed 's,.*/store,/store,' | sed 's,^/,,' `
  if [ "$check1" != "$check2" ] ; then echo "Argh matey! Suspect Multiple Files: $check1 and $check2" ; continue ; fi 
  
  #If all good, find the name of the file
  echo "unmerged " $fileNo "`echo $theParsedLog | tr ' ' '\n' | tail -1 | sed 's,/, ,3' | awk '{print $2}' | tr '?' ' ' | awk '{print $1}' | sed 's,.*/store,/store,'`"
done

nMergeLists=`ls $mergeLists/mergeFiles/mergeLists | wc -l`
echo " " 
echo "Here be the correspondence between merged and unmerged: " 

for file in `seq 1 $nMergeLists`
do
  n=0
  result=""
  for line in `cat $mergeLists/mergeFiles/mergeLists/merged_list_${file}.txt`
  do
    if [ "$n" -gt 1 ] ; then result+=`echo $line | tr '/' ' ' | awk '{print $NF}'  | tr '_.' ' ' | awk '{print $2}'` ; result+=" " ; fi
    n+=1
  done
  echo "merged file constituents $file : $result"
done

echo " " 
echo "Here be the nEvents in each merged file: " 
for file in `seq 1 $nMergeLists`
do
  n=0
  result=""
  for line in `cat $mergeLists/mergeFiles/mergeLists/merged_list_${file}.txt`
  do
    if [ "$n" == 1 ] ; then echo "merged file nevents $file : $line" ; fi
    n=$(( $n + 1 ))
  done
done
