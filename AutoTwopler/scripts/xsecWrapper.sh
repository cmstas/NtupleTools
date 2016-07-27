#!/bin/bash

inFile=$1          # "/hadoop/cms/store/user/namin/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/crab_ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/160220_235603/0000/merged_ntuple_1.root"
nevents=$2           # 25000
nevents_effective=$3 # 21000
xsec=$4              # 0.0123
kfactor=$5           # 1.10
filtEff=$6           # 1.0

# inFile=/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/ZJetsToNuNu_HT-800To1200_13TeV-madgraph_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/merged_ntuple_16.root
# nevents=5345383
# nevents_effective=5345383
# xsec=3.279
# kfactor=1.0
# filtEff=1.0

mergedIndex=$( echo $inFile  | rev | cut -d '_' -f1 | cut -d '.' -f2 | rev )
inDir=$( echo $inFile  | rev | cut -d '/' -f2- | rev )
echo "mergedIndex: $mergedIndex, inDir: $inDir"

# inFile="merged_ntuple.root"
outFile=merged_ntuple_${mergedIndex}.root
outDir=$inDir/new_xsec/
echo "outFile: $outFile, outDir: $outDir"

# prevents the ntuples from staging out in your home directory.
mkdir tempdir; cd tempdir; cp ../* .

# setup environment
CMSSW_VERSION=CMSSW_7_4_14
echo "[merge wrapper] setting env"
export SCRAM_ARCH=slc6_amd64_gcc491
source /cvmfs/cms.cern.ch/cmsset_default.sh
OLDDIR=$(pwd)
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc491/cms/cmssw/$CMSSW_VERSION/src
eval `scramv1 runtime -sh`
cd $OLDDIR

# debugging printouts 
echo "[merge wrapper] pwd: $(pwd)"
echo "[merge wrapper] scramarch: $SCRAM_ARCH"
echo "[merge wrapper] host: $(hostname)" 
echo "[merge wrapper] slc6 vs slc5: $(cat /etc/redhat-release)"
echo "[merge wrapper] current files in directory:"
echo `ls -l`

echo "[merge wrapper] t before addBranches.C: $(date +%s)"
root -b -q -l "addBranches.C (\"$inFile\",\"$outFile\",$nevents,$nevents_effective,$xsec,$kfactor,$filtEff)"
echo "[merge wrapper] t after addBranches.C: $(date +%s)"

# stageout
localFile=$(pwd)/$outFile
echo "[merge wrapper] copying file from $localFile to $outDir/$outFile"
gfal-copy -p -f -t 4200 --verbose file:$localFile srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=$outDir/$outFile
echo "[merge wrapper] t after lcg-cp: $(date +%s)"

# cleanup
echo "[merge wrapper] cleaning up."
rm $outFile merged_ntuple.root *.txt

