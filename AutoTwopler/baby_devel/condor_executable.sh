if [ -z "$DATASET" ]; then 
    DATASET=/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM
    FILENAME=/hadoop/cms/store/group/snt/test/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/merged_ntuple_1.root
fi

OUTNAME="output.root"
NEVENTS=-1
if [ -z "$EXTRA1" ]; then NEVENTS=$EXTRA1; fi

# Set CMSSW environment
export SCRAM_ARCH=slc6_amd64_gcc491
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc491/cms/cmssw/CMSSW_7_4_12/src/
eval `scramv1 runtime -sh`
cd -

# Need this for .so files
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD

tar -xzf package.tar.gz

./main.exe $FILENAME $OUTNAME $NEVENTS
