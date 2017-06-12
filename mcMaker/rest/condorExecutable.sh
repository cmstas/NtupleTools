#!/bin/bash

OUTPUTDIR=$1
OUTPUTNAME=$2
INPUTFILENAMES=$3
IFILE=$4
PSET=$5
CMSSWVERSION=$6
SCRAMARCH=$7
NEVTS=$8
FIRSTEVT=$9


echo "OUTPUTDIR: $OUTPUTDIR"
echo "OUTPUTNAME: $OUTPUTNAME"
echo "INPUTFILENAMES: $INPUTFILENAMES"
echo "IFILE: $IFILE"
echo "PSET: $PSET"
echo "CMSSWVERSION: $CMSSWVERSION"

#Tell us where we're running
echo "host: `hostname`" 

source /cvmfs/cms.cern.ch/cmsset_default.sh

export SCRAM_ARCH=${SCRAMARCH}

eval `scramv1 project CMSSW $CMSSWVERSION`
cd $CMSSWVERSION
eval `scramv1 runtime -sh`
mv ../$PSET pset.py


echo "process.maxEvents.input = cms.untracked.int32(${NEVTS})" >> pset.py
echo "process.source.fileNames = cms.untracked.vstring([" >> pset.py
for INPUTFILENAME in $(echo "$INPUTFILENAMES" | sed -n 1'p' | tr ',' '\n'); do
    INPUTFILENAME=$(echo $INPUTFILENAME | sed 's|/hadoop/cms||')
    INPUTFILENAME="root://xrootd.unl.edu/${INPUTFILENAME}"
    echo "\"${INPUTFILENAME}\"," >> pset.py
done
echo "])" >> pset.py
if [ "$FIRSTEVT" -ge 1 ]; then
    echo "process.source.skipEvents = cms.untracked.uint32(${FIRSTEVT})" >> pset.py
fi

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

