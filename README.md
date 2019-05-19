# NtupleTools
Old readme is left below, but basically all of the old functionality is contained within
* [https://github.com/aminnj/ProjectMetis](https://github.com/aminnj/ProjectMetis)
* [https://github.com/aminnj/dis](https://github.com/aminnj/dis)
* [MetisScripts](MetisScripts)

# Old readme below

#NtupleTools

This repository is used to facilitate running the NtupleMaker directory at scale.  This allows CMS3 Ntuples to be quickly made.  

###AutoTupler
  - Uses crab to run the NtupleMaker.  Need to know the published dataset name

###AutoTwopler
  - Uses crab to run the NtupleMaker. MkII of the AutoTupler (hence the "Two" in AutoTwopler. Get it?).

###CMS3withCondor
  - Uses condor to run the NtupleMaker.  Need to know the name and location of the MINIAOD file (check DAS), starting with /store....
  - Necessary if data has not been published (produced centrally)

###dataTuple 
  - Runs the NtupleMaker on files as the files become available.  Uses CMS3withCondor to do this 

###checkCMS3
  - Called by the AutoTupler or independently, this runs a few checks on finished CMS3 files to make sure all is OK

###condorMergingTools
  - Merges and does the post-processing for CMS3 jobs.  Called by all three methods.  

###sampleParser
  - This is just a cheat sheet that allows us to parse the huge e-mails announcing new samples and returns only the potentially useful samples.  

###subscribedDatasets
  - checks which samples are valid but not present at T2_US_UCSD

###sweepRoot
  - runs various checks on one CMS3 file for validity.  Called by many of the other directories.

###validation
  - allows the user to make before-and-after comparisons (or single release plot dumps) to verify that our NtupleMaker is not buggy
