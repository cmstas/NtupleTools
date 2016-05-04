#!/bin/bash

#Need two arguments
if [ "$#" -lt 2 ]; then echo "GRRR!!!!! I NEED TWO ARGUMENTS, sample name and tag!!!  I quit!!"; return 3; fi

#Arguments
filename=$1
tag=$2
basedir=$3

#Set basedir default
if [ "$basedir" == "" ]; then basedir="/nfs-7/userdata/dataTuple/$shortusername" ; fi

#Short usernames
if [ "$USER" == "cgeorge"     ]; then shortusername="alex" ; fi
if [ "$USER" == "namin"       ]; then shortusername="nick" ; fi
if [ "$USER" == "jgran"       ]; then shortusername="jason"; fi
if [ "$USER" == "mderdzinski" ]; then shortusername="mark" ; fi

#parse filename
run=`echo $filename | tr '/' ' ' | awk '{print $2}' | tr '-' ' ' | awk '{print $1}'`
sample=`echo $filename | tr '/' ' ' | awk '{print $1}'`
format=`echo $filename | tr '/' ' ' | awk '{print $3}'`
era=`echo $filename | tr '/' ' ' | awk '{print $2}' | tr '-' ' ' | awk '{print $2}'`
version=`echo $filename | tr '/' ' ' | awk '{print $2}' | tr '-' ' ' | awk '{print $3}'`

#parse tag
pretag=`echo "$tag" | tr '_' ' ' | awk '{print $1}'`
if [ "$pretag" == "CMS3" ]; then tag=`echo "$tag" | tr '_' ' ' | awk '{print $2}'`; fi
if [ "$tag" == "" ]; then echo "Problem with your tag!"; return 1; fi

#See if label unmerged files exist
unmergedDir="/hadoop/cms/store/user/$USER/dataTuple/${run}_${sample}_${format}_${era}-${version}/$tag/"
if [ ! -d $unmergedDir ]
then 
  echo "No unmerged files found!!  How the hell am I supposed to remerge if there are no fucking unmerged files!!  I quit!!"
  echo "They should be here...."
  echo "/hadoop/cms/store/user/$USER/dataTuple/${run}_${sample}_${format}_${era}-${version}/$tag/"
  return 2;
fi

#Make sure the merged dir exists
mergedDir="/hadoop/cms/store/group/snt/run2_data/${run}_${sample}_${format}_${era}-${version}/merged/$tag/"
if [ ! -d $mergedDir ]
then 
  echo "Your merged dir does not exist!!!  Should be here...."
  echo "$mergedDir"
  return 4;
fi

#Be sure you fucking do this right
read -p "You are about to remerge filename $1.  This will delete all the old merged files.  Are you sure? (y/n)" -n 1 -r
echo 
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  echo "You did not type y, you scaredy cat.  Quitting...."
  return 4;
fi

#Delete the merged files
rm $mergedDir/*.root

#Delete all the old merged lists
mergedListsDir="$basedir/mergedLists/${run}_${sample}_${format}_${era}-${version}/"
if [ ! -d $mergedListsDir ]
then 
  echo "Your merged dir does not exist!!!  Should be here...."
  echo "$mergedListsDir"
  return 5;
else
  rm $mergedListsDir/merged_list_*.txt
  rm $mergedListsDir/metaData_*.txt
fi

#Take all these files off the donePP.txt
sed -i "/\/hadoop\/cms\/store\/user\/$USER\/dataTuple\/${run}_${sample}_${format}_${era}-${version}\/merged\/merged_ntuple/d" testAlex.txt

#Now loop over the unmerged and call checkfile on them
for file in $( ls -l $unmergedDir/ | awk '{print $9}' )
do
  if [ -d $unmergedDir/$file ]; then continue; fi 
  stuff=`echo $file | tr '_' ' ' | awk '{printf "%s/%s/%s/%s/%s",$4,$5,$6,$7,$8}'`
  currentFile=/store/data/$run/$sample/$format/${era}-${version}/$stuff
  outputDir=`echo $currentFile | tr '/' ' ' |  awk '{print $3"_"$4"_"$5"_"$6}'`
  . checkFile.sh $basedir /hadoop/cms/store/user/$USER/dataTuple/$outputDir/$tag/$file $currentFile cms3
done
