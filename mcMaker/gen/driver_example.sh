#!/bin/bash
LHEFILE="/{path to file}.lhe"   # Edit me!

source  /afs/cern.ch/cms/cmsset_default.sh
export SCRAM_ARCH=slc6_amd64_gcc481
if [ -r CMSSW_7_1_20_patch3/src ] ; then 
 echo release CMSSW_7_1_20_patch3 already exists
else
scram p CMSSW CMSSW_7_1_20_patch3
fi
cd CMSSW_7_1_20_patch3/src
eval `scram runtime -sh`

export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
curl -s --insecure https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_fragment/SUS-RunIISummer15GS-00059 --retry 2 --create-dirs -o Configuration/GenProduction/python/SUS-RunIISummer15GS-00059-fragment.py 
[ -s Configuration/GenProduction/python/SUS-RunIISummer15GS-00059-fragment.py ];

scram b
cd ../../
cmsDriver.py Configuration/GenProduction/python/SUS-RunIISummer15GS-00059-fragment.py --filein "file:${LHEFILE}" --fileout file:GENSIM.root --mc --eventcontent RAWSIM --customise SLHCUpgradeSimulations/Configuration/postLS1Customs.customisePostLS1,Configuration/DataProcessing/Utils.addMonitoring --datatier GEN-SIM --conditions MCRUN2_71_V1::All --beamspot Realistic50ns13TeVCollision --step GEN,SIM --magField 38T_PostLS1 --python_filename pset_gensim.py --no_exec -n 240;

# cmsDriver.py step2 --filein file:SUS-RunIISummer15GS-00059_genvalid.root --conditions MCRUN2_71_V1::All --mc -s HARVESTING:genHarvesting --harvesting AtJobEnd --python_filename SUS-RunIISummer15GS-00059_genvalid_harvesting.py --no_exec;
