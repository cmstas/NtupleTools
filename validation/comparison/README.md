#Compare Ntuples for CMS3

This is a "core dump" of every branch in CMS3 for **TWO RELEASES**.  If you want one release by itself, you are looking for cmstas/NtupleTools/validation/branchDumper.  

###Instructions:
1. In compareNtuples.C, put the OLD and NEW samples on line 118-119. 
2. "root -b do.C"
3. ". process.sh" 
4. Now overview.pdf has everything you need. 

###Sample:
For run-2, we have been using the following sample for validation:
 - fileNames = cms.untracked.vstring('/store/mc/RunIISpring15DR74/TTJets_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/MINIAODSIM/Asympt50ns_MCRUN2_74_V9A-v1/00000/20AD8065-31FD-E411-9D75-00259073E2F2.root')
