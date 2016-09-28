#! /bin/bash

##check metadata to find CMSSW and scramarch. Might do this later.
#input merged list
inputList=$1
#input metadata
mData=$2
#output
outputDir=$3

#This step prevents the ntuples from staging out in your home directory.
mkdir tempdir
cd tempdir
cp ../* .

# shows files you have to work with.
echo "current files in directory:"
echo `ls -l`

# This sets up some variables for later.
inputList=`basename $inputList`
echo "Attempting to merge files from $inputList"
mData=`basename $mData`
echo "using metadata file: $mData"

#Debugging
nFilesAG=`wc -l < $inputList`
echo "The list contains the following $nFilesAG files:"
less $inputList

#If the input list starts with nEvents:, remove that line
firstLine=`awk 'NR==1 {print $1}' $inputList`
if [ "$firstLine" == "nEntries:" ] 
then
  nEvents=`awk 'NR==1 {print $2}' $inputList` 
  sed -i '1d' $inputList
fi

# Environment
#export CMS_PATH=/cvmfs/cms.cern.ch
#export SCRAM_ARCH=slc6_amd64_gcc491
#source /cvmfs/cms.cern.ch/cmsset_default.sh
#source /cvmfs/cms.cern.ch/slc6_amd64_gcc491/lcg/root/6.02.00-eccfad2/bin/thisroot.sh
#export LD_LIBRARY_PATH=/cvmfs/cms.cern.ch/slc6_amd64_gcc491/lcg/root/6.02.00-eccfad2/lib:/cvmfs/cms.cern.ch/slc6_amd64_gcc491/external/gcc/4.9.1/lib:/home/users/cgeorge:/cvmfs/cms.cern.ch/crab3/slc6_amd64_gcc491/external/gcc/4.9.1-cms/lib64:/cvmfs/cms.cern.ch/slc6_amd64_gcc491/cms/cmssw/CMSSW_7_4_1/external/slc6_amd64_gcc491/lib
#export PATH=$ROOTSYS/bin:$PATH:${_CONDOR_SCRATCH_DIR}
#export PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH

#Use enviroment setting from MT2Analysis
CMSSW_VERSION=CMSSW_7_4_1
echo "[wrapper] setting env"
export SCRAM_ARCH=slc6_amd64_gcc491
source /cvmfs/cms.cern.ch/cmsset_default.sh
OLDDIR=`pwd`
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc491/cms/cmssw/$CMSSW_VERSION/src
#cmsenv
eval `scramv1 runtime -sh`
cd $OLDDIR

#export CMS_PATH=/cvmfs/cms.cern.ch
#export SCRAM_ARCH=slc6_amd64_gcc491
#source /cvmfs/cms.cern.ch/cmsset_default.sh
#scramv1 p -n CMSSW_7_4_1 CMSSW CMSSW_7_4_1
#cd CMSSW_7_4_1/src
#cmsenv
#cd ../../

echo "scramarch is $SCRAM_ARCH"

#Debugging
echo "host is: " 
hostname

echo "slc6 v. slc5: "
cat /etc/redhat-release

#sets variable for stageout later
localdir=`pwd`

# check that sweepRoot exists
if [ ! -e "sweepRoot.tar.gz" ]; then
	echo "Error: sweepRoot doesn't exist, exiting.."
	exit 4
fi

#make sweepRoot
mkdir sweepRootStuff
mv sweepRoot.tar.gz sweepRootStuff/
cd sweepRootStuff
tar -xzvf sweepRoot.tar.gz
make
cd ..
mv sweepRootStuff/sweepRoot . 

#runs merging script, and if it fails tries one more time.
root -b -q -l "mergeScript.C (\"$inputList\",\"merged_ntuple.root\")" > temp_merging.log
./sweepRoot -o "Events" -t "Events" merged_ntuple.root 
didMerge=$?

if [ $didMerge != 0 ]; then

	echo "Error in merging process, attempting to merge one more time.."

	if [ -e "merged_ntuple.root" ]; then

		rm merged_ntuple.root
	fi

	root -b -q -l "mergeScript.C (\"$inputList\",\"merged_ntuple.root\")" > temp_merging.log
	./sweepRoot -o "Events" -t "Events" merged_ntuple.root
	didMerge=$?	
fi
	
if [ $didMerge != 0 ]; then

	echo "Error in second merging attempt. Exiting."

	if [ -e "merged_ntuple.root" ]; then
		
		rm merged_ntuple.root
		
	fi
	
	exit 2	
fi

if [ -e temp_merging.log ]; then
    nEventsMerged=`grep "Merged Entries:" temp_merging.log | awk '{print $3}'`
    retrievedEventsMerged=$?
    rm temp_merging.log
fi

echo "retrievedEventsMerged is $retrievedEventsMerged"
echo "nEventsMerged is $nEventsMerged"
echo "nEvents is $nEvents"

#check that number of merged events is same as input from mergelist
#only do this if grep was successful and nEvents exists
if [ $retrievedEventsMerged == 0 ] && [ $nEvents != "" ]; then
   if [ $nEvents != $nEventsMerged ]; then
	echo "Error in merging. Events in does not equal events out. Exiting."
	if [ -e "merged_ntuple.root" ]; then		
	    rm merged_ntuple.root
	fi
	exit 4
   fi
fi

#Makes the name of the output ntuple depending on the name of the file list
outFileName=`echo $inputList | sed 's/list/ntuple/g' | sed 's/txt/root/g'`
outFileName=`basename $outFileName`
echo "outfile name = $outFileName"

#Adds cms2 branches to the file. If it fails, it tries again.
root -b -q -l "addBranches.C (\"$mData\",\"merged_ntuple.root\",\"$outFileName\")"
./sweepRoot -o "Events" -t "Events" `pwd`/$outFileName
didAddBranches=$?

if [ $didAddBranches != 0 ]; then

	echo "Error in adding cms2 branches. Attempting one more time.."

	if [ -e $outFileName ]; then
		
		rm $outFileName
		
	fi

	root -b -q -l "addBranches.C (\"$mData\",\"merged_ntuple.root\",\"$outFileName\")"
	./sweepRoot -o "Events" -t "Events" `pwd`/$outFileName
	didAddBranches=$?

fi


if [ $didAddBranches != 0 ]; then
	
	echo "Error in second attempt to add cms2 branches. Exiting."
	
	if [ -e $outFileName ]; then
		
		rm $outFileName
		
	fi
	
	exit 3
fi

# Rigorous sweeproot which checks ALL branches for ALL events.
# If GetEntry() returns -1, then there was an I/O problem, so we will delete it
cat > rigorousSweepRoot.sh << EOL
import ROOT as r
import os

f1 = r.TFile("$outFileName")
t = f1.Get("Events")
print "[RSR] ntuple has %i events" % t.GetEntries()

foundBad = False
for i in range(0,t.GetEntries(),1):
    if t.GetEntry(i) < 0:
        foundBad = True
        print "[RSR] found bad event %i" % i
        break

if foundBad:
    print "[RSR] removing merged_ntuple.root because it does not deserve to live"
    os.system("rm $outFileName")
else:
    print "[RSR] passed the rigorous sweeproot"
EOL

date +%s
python rigorousSweepRoot.sh
date +%s

echo $CMSSW_RELEASE_BASE
#use lcgcp to stageout
echo "filename = $outFileName"
echo "Outputting file to $outputDir from worker node."
localFile=`pwd`/${outFileName}

if [ $didAddBranches == 0 ]; then

	echo -e "copying file now from : \n$localFile \nto:\n$outputDir/$outFileName"
	gfal-copy -p -f -t 4200 --verbose file:`pwd`/${outFileName} srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=$outputDir/${outFileName}
	stageout_error=$?

	if [ $stageout_error != 0 ]; then

		echo "Error merging files in $inputList. Job exit code $stageout_error. Stageout with lcg-cp failed."

	fi

	if [ $stageout_error == 0 ]; then

		echo "Merging of files in $inputList successful. Job exit code $stageout_error." # Error occurred while running makeSkime.C."

	fi
fi


echo "Cleaning up."
rm $outFileName merged_ntuple.root *.txt
echo "End of Merging."
#add white space.
