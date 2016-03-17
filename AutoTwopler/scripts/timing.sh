
# TODO find these:
# [merge] Merged Entries: 169320
user=namin

samples="
TTJets_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
ZJetsToNuNu_HT-100To200_13TeV-madgraph_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
ZJetsToNuNu_HT-200To400_13TeV-madgraph_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
ZJetsToNuNu_HT-400To600_13TeV-madgraph_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
ZJetsToNuNu_HT-600ToInf_13TeV-madgraph_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
GJets_HT-40To100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v3
GJets_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
GJets_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
GJets_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
GJets_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1
"

  # 1 [merge wrapper] setting env
  # 2 [merge wrapper] pwd: /data1/condor_local/execute/dir_1768073/glide_5tQlaT/execute/dir_1786199/tempdir
  # 3 [merge wrapper] scramarch: slc6_amd64_gcc491
  # 4 [merge wrapper] host: cabinet-0-0-0.t2.ucsd.edu
  # 5 [merge wrapper] slc6 vs slc5: CentOS release 6.7 (Final)
  # 6 [merge wrapper] current files in directory:
  # 7 [merge wrapper] t before addBranches.C: 1458140856
  # 8 [merge wrapper] t after addBranches.C: 1458140965
  # 9 [merge wrapper] copying file from /data1/condor_local/execute/dir_1768073/glide_5tQlaT/execute/dir_1786199/tempdir/merged_ntuple_1.root to /hadoop/cms/store/user/namin/GJets_HT-100To200_TuneCUETP8M1_13TeV-ma
 # 10 [merge wrapper] t after lcg-cp: 1458141082
 # 11 [merge wrapper] cleaning up.

echo "# user sample t_before_merge t_before_addbranch t_after_addbranch t_after_lcgcp nevents"
for sample in $samples; do
    # echo $sample

    basedir=/data/tmp/${user}/${sample}/std_logs/
    for file in $(ls -1 $basedir/*.out); do

        # outlog=1e.335850.0.out
        # file=/data/tmp/${user}/${sample}/std_logs/${outlog}

        tvals=$(grep "\[merge wrapper\] t " -H $file | cut -d ':' -f3) 
        nevents=$(grep "\[merge\] Merged Entries: " -H $file | cut -d ':' -f3)

        # user sample t_before_merge t_before_addbranch t_after_addbranch t_after_lcgcp nevents
        line=$(echo $user $sample $tvals $nevents)
        line=$(echo $line | sed 's/ \{1,\}/,/g')
        echo $line

    done
done
