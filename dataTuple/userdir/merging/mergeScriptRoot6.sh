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

#runs merging script, and if it fails tries one more time.
root -b -q -l "mergeScript.C (\"$inputList\",\"merged_ntuple.root\")"

#Makes the name of the output ntuple depending on the name of the file list
outFileName=`echo $inputList | sed 's/list/ntuple/g' | sed 's/txt/root/g'`
outFileName=`basename $outFileName`
echo "outfile name = $outFileName"

echo $CMSSW_BASE
#use lcgcp to stageout
echo "filename = $outFileName"
echo "Outputting file to $outputDir from worker node."
localFile=`pwd`/${outFileName}

echo -e "copying file now from : \n$(pwd)/merged_ntuple.root \nto:\n$outputDir/$outFileName"
gfal-copy -p -f -t 4200 file:`pwd`/merged_ntuple.root srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=$outputDir/${outFileName}
stageout_error=$?

if [ $stageout_error != 0 ]; then

    echo "Error merging files in $inputList. Job exit code $stageout_error. Stageout with lcg-cp failed."

fi

if [ $stageout_error == 0 ]; then

    echo "Merging of files in $inputList successful. Job exit code $stageout_error." # Error occurred while running makeSkime.C."

fi

echo "Cleaning up."
rm $outFileName merged_ntuple.root *.txt
echo "End of Merging."
#add white space.
