#!/bin/bash

OUTPUTDIR=$1
OUTPUTNAME=$2
INPUTFILENAME=$3
IFILE=$4
PSET=$5
CMSSWVERSION=$6

INPUTFILENAME=$(echo $INPUTFILENAME | sed 's|/hadoop/cms||')
INPUTFILENAME="root://xrootd.unl.edu/${INPUTFILENAME}"

echo "OUTPUTDIR: $OUTPUTDIR"
echo "OUTPUTNAME: $OUTPUTNAME"
echo "INPUTFILENAME: $INPUTFILENAME"
echo "IFILE: $IFILE"
echo "PSET: $PSET"
echo "CMSSWVERSION: $CMSSWVERSION"

#Tell us where we're running
echo "host: `hostname`" 

source /cvmfs/cms.cern.ch/cmsset_default.sh

export SCRAM_ARCH=slc6_amd64_gcc530

eval `scramv1 project CMSSW $CMSSWVERSION`
cd $CMSSWVERSION
eval `scramv1 runtime -sh`
mv ../$PSET pset.py


# echo "process.source.fileNames = cms.untracked.vstring([\"file:${INPUTFILENAME}\"])" >> pset.py
echo "process.source.fileNames = cms.untracked.vstring([\"${INPUTFILENAME}\"])" >> pset.py
echo "process.maxEvents.input = cms.untracked.int32(-1)" >> pset.py

echo "ls -lrth"
ls -lrth 

cmsRun pset.py

echo "ls -lrth"
ls -lrth

echo "Sending output file $OUTPUTNAME.root"
gfal-copy -p -f -t 4200 --verbose file://`pwd`/${OUTPUTNAME}.root srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=${OUTPUTDIR}/${OUTPUTNAME}_${IFILE}.root

# cd ../
# echo "cleaning up"
# rm -rf CMSSW*

