#!/bin/bash

if (( $# != 7 )); then
    echo "Illegal number of arguments."
    exit 1
fi

configFile=$1
libCMSSW=$2
GLOBAL_TAG=$3
INPUT_FILE_NAME=$4
OUTPUT_DIR=$5
OUTPUT_FILE_NAME=$6
MAX_NEVENTS=$7

#Tell us where we're running
echo "host: `hostname`" 

#untar tarball containing CMS3 libraries and python files
echo "libCMSSW = $libCMSSW"
if [ -e $libCMSSW ]
then
  tar -xzvf $libCMSSW
else
  echo "libCMSSW missing!"
  exit 1
fi

#Set environment
export CMS_PATH=/cvmfs/cms.cern.ch
export SCRAM_ARCH=slc6_amd64_gcc491
source /cvmfs/cms.cern.ch/cmsset_default.sh

pushd .
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc491/cms/cmssw-patch/CMSSW_7_4_4_patch2/src/
eval `scramv1 runtime -sh`
popd

#The lib and python directories come from the libCMSSW tarball
export LD_LIBRARY_PATH=$PWD/lib/slc6_amd64_gcc491:$LD_LIBRARY_PATH
export PATH=$PWD:$PATH
export PYTHONPATH=$PWD/python:$PYTHONPATH

sed -i "s/SUPPLY_GLOBAL_TAG/${GLOBAL_TAG}/g" $configFile
sed -i "s,SUPPLY_INPUT_FILE_NAME,${INPUT_FILE_NAME},g" $configFile
sed -i "s/SUPPLY_OUTPUT_FILE_NAME/${OUTPUT_FILE_NAME}/g" $configFile
sed -i "s/SUPPLY_MAX_NEVENTS/${MAX_NEVENTS}/g" $configFile

echo "ls -lrth"
ls -lrth

#Run it
cmsRun $configFile

echo "ls -lrth"
ls -lrth

#Copy the output
gfal-copy -p -f -t 4200 file://`pwd`/${OUTPUT_FILE_NAME} srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=${OUTPUT_DIR}/${OUTPUT_FILE_NAME}
