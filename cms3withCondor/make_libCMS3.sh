#!/bin/bash

if [ "$#" -lt 1 ]; then 
  echo "Illegal number of arguments."
  echo "Must provide CMS3 tag then (optionally) CMSSW tag"
  exit 1
fi

echo "Checkout and build CMS3:"

THE_CMS3_TAG=$1

curl https://raw.githubusercontent.com/cmstas/NtupleMaker/${THE_CMS3_TAG}/install.sh > install.sh
sed -i "4s/.*/CMS3Tag=${THE_CMS3_TAG}/g" install.sh
if [ "$#" -gt 1 ]; then 
  CMSSW_RELEASE=$2
  sed -i "5s/.*/CMSSW_release=${CMSSW_RELEASE}/g" install.sh
fi

DIR=$PWD

source install.sh
cd $CMSSW_BASE
cp /nfs-7/userdata/JECs/*.db . 
echo "Making the tarball..."
stuff1=`find src/ -name "data"`
stuff2=`find src/ -name "interface"`
stuff3=`find src/ -name "python"`
jettoolbox="src/JMEAnalysis"
tar -chzvf lib_$THE_CMS3_TAG.tar.gz *.db lib/ python/ $stuff1 $stuff2 $stuff3 $jettolbox src/CMS3/NtupleMaker/test/*_cfg.py

mv lib_$THE_CMS3_TAG.tar.gz $DIR/lib_$THE_CMS3_TAG.tar.gz
cd $DIR
echo "Your tarball is lib_$THE_CMS3_TAG.tar.gz"
