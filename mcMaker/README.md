
## Making MC

General notes:
* Places to edit example files and currently existing scripts are marked with `EDIT ME` in the files.
* As of right now, the example files are usable to make Moriond campaign MC.

### LHE
* Can maybe steal cards from [genproductions](https://github.com/cms-sw/genproductions/tree/master/python/ThirteenTeV)
* If making SUSY, check out <put SUSY SMS scan github link here>
* qcut values can be stolen from [here](https://docs.google.com/spreadsheets/d/1fsHXGf6s7sIm_8PWaoVermlN1Q9mEtCM-1mTxqz4X7k/edit#gid=0)
* Put your LHE on hadoop somewhere when you're done with it

### GENSIM
* Inside the `gen/` folder, you'll find an example cmsDriver command, crab config, and pset
* Use [DIS](http://uaf-10.t2.ucsd.edu/~namin/makers/test_disMaker/?query=%2FTTZToLL_M-1to10_TuneCUETP8M1_13TeV-madgraphMLM-pythia8%2FRunIISummer16MiniAODv2-80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1%2FMINIAODSIM&type=driver&short=short) to get a driver for a similar sample
* The driver command will set up a CMSSW environment and will also make a pset

### GENSIM->RAWRECO->AODSIM->MINIAODSIM
* Go into `rest/`
* For subsequent steps, one can make driver files and execute them to get psets
* Use DIS with the `parent` option for a miniaod file to get a list of parents
* Enter each one of those (including the original miniaod dataset name) into the `driver` option and make sure to suffix the dataset name in the query with `,this` to prevent the `driver` option from recursing up the parent tree for you (as it does by default)
* The pset names should be `pset_raw.py`, `pset_aod.py`, `pset_miniaod.py`
* Edit submit.py appropriately and `python submit.py`. Note that you are the looping mechanism, so you have to do `for i in $(seq 1 100); do python submit.py ; sleep 60m; done` or whatever.
* Eventually, files will show up in your configured directory in the `<tag>_MINIAOD` folder.


### MINIAODSIM->CMS3
Use [AutoTwopler](https://github.com/cmstas/NtupleTools/tree/master/AutoTwopler)

