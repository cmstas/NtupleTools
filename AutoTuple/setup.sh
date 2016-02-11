#!/bin/bash
if [ $# -eq 0 ] 
  then 
  echo "No arguments!" 
  return
fi
source /code/osgcode/cmssoft/cms/cmsset_default.sh

gtag=`sed -n '1p' $1`
tag=`sed -n '2p' $1`
CMSSW_VER=CMSSW_7_4_14
# CMSSW_VER=CMSSW_7_4_1_patch1
export PATH=$PATH:`pwd`
source /cvmfs/cms.cern.ch/crab3/crab.sh
export SCRAM_ARCH=slc6_amd64_gcc491
scramv1 p -n ${CMSSW_VER} CMSSW $CMSSW_VER
pushd ${CMSSW_VER}
cmsenv
popd
pushd ../sweepRoot
make
if [ ! -e sweepRoot.o ]
then
  echo "Could not make sweepRoot!"
  return 1 
fi
popd
if [ ! -e /nfs-7/userdata/libCMS3/lib_$tag.tar.gz ]
then
  echo "Trying to make this on the fly.  Hopefully this works......"
  source ../cms3withCondor/make_libCMS3.sh $tag $CMSSW_VER
  mv lib_$tag.tar.gz /nfs-7/userdata/libCMS3/lib_$tag.tar.gz
  cd $CMSSW_BASE
else
  cd ${CMSSW_VER}
  cmsenv
  cp /nfs-7/userdata/libCMS3/lib_$tag.tar.gz . 
  tar -xzvf lib_$tag.tar.gz
  scram b -j 10
fi
mkdir crab
cd crab
mkdir autoTupleLogs
cp ../../../sweepRoot/sweepRoot ${CMSSW_BASE}/crab/
cp -r ../../../condorMergingTools/* ${CMSSW_BASE}/crab/
if [ -e "${CMSSW_BASE}/src/CMS3/NtupleMaker/test/MCProduction2015_NoFilter_cfg.py" ] 
then
  cp ${CMSSW_BASE}/src/CMS3/NtupleMaker/test/MCProduction2015_NoFilter_cfg.py skeleton_cfg.py
  sed -i s/process.GlobalTag.globaltag\ =\ .*/process.GlobalTag.globaltag\ =\ \"$gtag\"/ skeleton_cfg.py
fi
if [ -e "${CMSSW_BASE}/src/CMS3/NtupleMaker/test/MCProduction2015_FastSim_NoFilter_cfg.py" ] 
then
  cp ${CMSSW_BASE}/src/CMS3/NtupleMaker/test/MCProduction2015_FastSim_NoFilter_cfg.py skeleton_fsim_cfg.py
  sed -i s/process.GlobalTag.globaltag\ =\ .*/process.GlobalTag.globaltag\ =\ \"$gtag\"/ skeleton_fsim_cfg.py
fi
cp ../../submitMergeJobs.sh .
cp ../../submit_crab_jobs.py  .
cp ../../$1 .
cp ../../monitor.sh . 
cp ../../process.py .
cp ../../pirate.txt .
cp ../../theDir.txt .
cp ../../FindLumisPerJob.sh . 
cp ../../maxAG.sh . 
chmod a+x maxAG.sh
cp ../../FindLumisPerJobNoDAS.sh . 
cp ../../FindLumisPerJob.py . 
cp ../../das_client.py . 
cp ../../crabPic.png .
cp ../../copy.sh .
cp ../../numEventsROOT.C .
cp ../../../checkCMS3/checkCMS3.C . 
cp ../../../checkCMS3/das_client.py .
cp $CMSSW_BASE/*.db .
cp ../../checkProxy.sh . 
cp ../../makeMetaData.sh . 
mkdir crab_status_logs
. checkProxy.sh 
python submit_crab_jobs.py $1
. monitor.sh $1 
