#!/bin/bash

if (( $# != 1 )); then
  echo "Illegal number of arguments."
  echo "Must provide CMS3 tag"
  exit 1
else
  THE_CMS3_TAG=$1
fi

echo "Checkout and build CMS3:"

curl https://raw.githubusercontent.com/cmstas/NtupleMaker/${THE_CMS3_TAG}/install.sh > install.sh

sed -i "s/CMS3Tag=master/CMS3Tag=${THE_CMS3_TAG}/g" install.sh

DIR=$PWD

source install.sh
cd $CMSSW_BASE
echo "Making the tarball..."
stuff1=`find src/ -name "data"`
stuff2=`find src/ -name "interface"`
stuff3=`find src/ -name "python"`
jettoolbox="src/JMEAnalysis"
tar -chzvf lib_$THE_CMS3_TAG.tar.gz lib/ python/ $stuff1 $stuff2 $stuff3 $jettolbox src/CMS3/NtupleMaker/test/*_cfg.py


mv lib_$THE_CMS3_TAG.tar.gz $DIR/lib_$THE_CMS3_TAG.tar.gz
cd $DIR
echo "Your tarball is lib_$THE_CMS3_TAG.tar.gz"
