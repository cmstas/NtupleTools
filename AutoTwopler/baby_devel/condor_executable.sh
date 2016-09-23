# Environment

# Patch1 not visible on condor nodes, so use nominal CMSSW release
if [ "$CMSSW_VER" == "CMSSW_8_0_5_patch1" ]; then CMSSW_VER=CMSSW_8_0_5; fi
export SCRAM_ARCH=$SCRAM_VER
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /cvmfs/cms.cern.ch/$SCRAM_VER/cms/cmssw/$CMSSW_VER/src/
eval `scramv1 runtime -sh`
cd -

# Need this for .so files
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD

# Split Output files line into an array
OUTPUT_NAMES=(`echo $OUTPUT_NAMES | sed s/,/" "/g`)
echo "OUTPUT_NAMES=${OUTPUT_NAMES[*]}"

# Split Extra Args into an array 
EXE_ARGS=(`echo $EXE_ARGS | sed s/,/" "/g`)
echo "EXE_ARGS=${EXE_ARGS[*]}"


#
# Place for User Code
#

if [ -z "$DATASET" ]; then 
    DATASET=/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM
    FILENAME=/hadoop/cms/store/group/snt/test/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/merged_ntuple_1.root
fi

./main.exe $FILENAME $OUTNAME $NEVENTS
