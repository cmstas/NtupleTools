#!/bin/bash

if (( $# != 9 )); then
    echo "Illegal number of arguments."
    echo "No.of args:  $#"
    echo "args: $@"
    exit 1
fi

HOME=`pwd`

configFile=$1
libCMS3=$2
GLOBAL_TAG=$3
INPUT_FILE_NAME=$4
OUTPUT_DIR=$5
OUTPUT_FILE_NAME=$6
MAX_NEVENTS=$7
CMS3_TAG=$8
DATASETNAME=$9

#Tell us where we're running
echo "host: `hostname`" 

source /cvmfs/cms.cern.ch/cmsset_default.sh

#The lib and python directories come from the libCMS3 tarball
export LD_LIBRARY_PATH=$PWD/lib/slc6_amd64_gcc530:$LD_LIBRARY_PATH
export PATH=$PWD:$PATH
export PYTHONPATH=$PWD/python:$PYTHONPATH:python/

#Set environment
# note, non-patches are in a different folder, run `scram list -a CMSSW | grep 8_0_14` to check
# pushd /cvmfs/cms.cern.ch/slc6_amd64_gcc530/cms/cmssw/CMSSW_8_0_12/src/ 
# pushd /cvmfs/cms.cern.ch/slc6_amd64_gcc530/cms/cmssw/CMSSW_8_0_20/src
pushd /cvmfs/cms.cern.ch/slc6_amd64_gcc530/cms/cmssw-patch/CMSSW_8_0_26_patch1/src/
eval `scramv1 runtime -sh`
echo "should be in cvmfs: $PWD"
popd

#untar tarball containing CMS3 libraries and python files
echo "libCMS3 = $libCMS3"

if [ -e $libCMS3 ]
then
  scramv1 project CMSSW CMSSW_8_0_26_patch1
  mv $libCMS3 CMSSW_8_0_26_patch1/
  cd CMSSW_8_0_26_patch1
  tar -xzvf $libCMS3
  scram b -j 8
  eval `scramv1 runtime -sh`
  cd ..
else
  echo "libCMS3 missing!"
  exit 1
fi

which root

#INPUT_FILE_NAME_ESCAPED=`echo $INPUT_FILE_NAME | sed 's,/,\\\/,g'

sed -i "s/SUPPLY_GLOBAL_TAG/${GLOBAL_TAG}/g" $configFile
sed -i "s,SUPPLY_INPUT_FILE_NAME,${INPUT_FILE_NAME},g" $configFile
sed -i "s/SUPPLY_OUTPUT_FILE_NAME/${OUTPUT_FILE_NAME}/g" $configFile
sed -i "s/SUPPLY_MAX_NEVENTS/${MAX_NEVENTS}/g" $configFile
sed -i "s/SUPPLY_CMS3_TAG/${CMS3_TAG}/g" $configFile
sed -i "s,SUPPLY_DATASETNAME,${DATASETNAME},g" $configFile

echo "ls -lrth"
ls -lrth

#copy JEC files to location where we run the pset
mv $CMSSW_BASE/*.db .

#remove .root for FrameworkJobReport name
FJR_NAME="FJR_"
FJR_NAME+=`echo $OUTPUT_FILE_NAME | sed s/\.root//g`
FJR_NAME+=".xml"

#Run it
cmsRun --jobreport $FJR_NAME $configFile
exit_code=$?


echo "ls -lrth"
ls -lrth

#Copy the output
if [ $exit_code == 0 ]
then
  if [ -e $OUTPUT_FILE_NAME ]
  then
    echo "Sending output file $OUTPUT_FILE_NAME"
    gfal-copy -p -f -t 4200 --verbose file://`pwd`/${OUTPUT_FILE_NAME} srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=${OUTPUT_DIR}/${OUTPUT_FILE_NAME}
  else
    echo "Output file $OUTPUT_FILE_NAME does not exist!"
  fi
else
  echo "cmsRun exited with error code $exit_code"
fi

#Copy the FrameworkJobReport
if [ -e $FJR_NAME ]
then
  echo "Sending FrameworkJobReport $FJR_NAME.tar.gz"
  tar -czvf $FJR_NAME.tar.gz $FJR_NAME
  gfal-copy -p -f -t 4200 --verbose file://`pwd`/$FJR_NAME.tar.gz srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=${OUTPUT_DIR}/FJR/$FJR_NAME.tar.gz
else
  echo "No FrameworkJobReport created!"
fi

#clean up
echo "cleaning up"
for FILE in `find . -not -name "*stderr" -not -name "*stdout"`; do rm -rf $FILE; done
echo "ls -lrth"
ls -lrth

