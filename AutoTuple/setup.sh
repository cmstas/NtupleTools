#!/bin/bash
if [ $# -eq 0 ] 
  then 
  echo "No arguments!" 
  return
fi
source /code/osgcode/cmssoft/cms/cmsset_default.sh

era=`less $1 | head -2 | tail -1 | tr '_-' ' ' | awk '{print $2 $3'} | tr '0' ' ' | awk '{print $2 $3}'`
source "config${era}X.sh"

gtag=`sed -n '1p' $1`
tag=`sed -n '2p' $1`
export PATH=$PATH:`pwd`
source /cvmfs/cms.cern.ch/crab3/crab.sh
scramv1 p -n ${CMSSW_VER} CMSSW $CMSSW_VER
export SCRAM_ARCH=$SCRAMARCHAG
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
ln -s ../../submit_crab_jobs.py  .
cp ../../$1 .
ln -s ../../monitor.sh . 
ln -s ../../process.py .
cp ../../pirate.txt .
cp ../../theDir.txt .
ln -s ../../FindLumisPerJob.sh . 
cp ../../maxAG.sh . 
chmod a+x maxAG.sh
cp ../../FindLumisPerJobNoDAS.sh . 
cp ../../FindLumisPerJob.py . 
cp ../../das_client.py . 
cp ../../crabPic.png .
ln -s ../../copy.sh .
cp ../../numEventsROOT.C .
cp ../../../checkCMS3/checkCMS3.C . 
cp ../../../checkCMS3/das_client.py .
cp $CMSSW_BASE/*.db .
cp ../../checkProxy.sh . 
ln -s ../../makeMetaData.sh . 
chmod a+x makeMetaData.sh
mkdir crab_status_logs
. checkProxy.sh 
python submit_crab_jobs.py $1
. monitor.sh $1 
