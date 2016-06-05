cmsrel CMSSW_7_4_12
cd CMSSW_7_4_12/
cmsenv
cd ../
git clone https://github.com/cmstas/CORE
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD
