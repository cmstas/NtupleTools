#!/bin/bash


HADOOPDIR=/hadoop/cms/store/user/namin/condor/ss_babies_Aug25/
OUTPUTDIR=/nfs-7/userdata/ss2015/ssBabies/v8.04_trigsafe_v4/

echo $HADOOPDIR
echo $OUTPUTDIR

# mkdir -p $OUTPUTDIR

# echo root -b -q mergeHadoopFiles.C\(\"${HADOOPDIR}/$1\",\"${OUTPUTDIR}/$2.root\"\)
# nice -n 19 root -b -q mergeHadoopFiles.C\(\"${HADOOPDIR}/$1\",\"${OUTPUTDIR}/$2.root\"\) >& log_merge_$2.txt
