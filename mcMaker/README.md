
## Making MC

General notes:
* Places to edit example files and currently existing scripts are marked with `EDIT ME` in the files.
* As of right now, the example files are usable to make Moriond campaign MC.

### LHE
* Can maybe steal cards from [genproductions](https://github.com/cms-sw/genproductions/tree/master/python/ThirteenTeV)
* If making SUSY, check out [put SUSY SMS scan github link here]
* qcut values can be stolen from [here](https://docs.google.com/spreadsheets/d/1fsHXGf6s7sIm_8PWaoVermlN1Q9mEtCM-1mTxqz4X7k/edit#gid=0)
* Put your LHE on hadoop somewhere when you're done with it
* If able to find some undecayed LHE files, can use the script `gen/parse_lhe.py` to modify the LHE with desired decay tables.

### GENSIM
* Use [DIS](http://uaf-10.t2.ucsd.edu/~namin/makers/test_disMaker/?query=%2FTTZToLL_M-1to10_TuneCUETP8M1_13TeV-madgraphMLM-pythia8%2FRunIISummer16MiniAODv2-80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1%2FMINIAODSIM&type=driver&short=short) to get a driver for a similar sample

Inside the `gen/` folder, you'll find an example cmsDriver command, crab config, and pset
1. Change the `LHEFILE` variable in `driver_sample.sh` to create the the pset for the LHE file, and do 
``` bash
. driver.sh
```
this will set up a CMSSW environment and a pset `pset_gensim.py` will be created.

2. In `pset_gensim.py`, compared with the given pset example, modify lines 
   - `process.source.fileNames = [...]`: This enables doing multiple LHE files (for a single point)
   - `SLHA:minMassSM = 1000.`: A discription can be found [here](http://freyr.phys.nd.edu/~karmgard/htmldoc/SUSYLesHouchesAccord.html) If you need to set the mass of some SM particle (the Higgses), it would be useful to make sure this value is below the desired mass.
   - `JetMatching:qCut`: Use the qcut vlue from the link in the previous section.
   - `JetMatching:nJetMax = 2`: Make sure that this match the max num of jets with the Madgraph process lines
   - `fileName = cms.untracked.string('file:gensim.root'),`: Make sure this is in lower case
then do 
``` bash
cmsRun pset_gensim.py
```
for a test run. Allow for a few events to be generated and can stop the program, check the particles are properly decayed before moving on. 
For an example command would be to open the generated `gensim.root` with `root`, and do
``` c++
Events->Scan("recoGenParticles_genParticles__SIM.obj.pdgId():recoGenParticles_genParticles__SIM.obj.mother().pdgId()","recoGenParticles_genParticles__SIM.obj.mother().pdgId() > 1000000")
```
3. After the quality of the gensim is checked, can move on to using CRAB to do the rest. Note that the LHE files has to be in hadoop in order for xrootd to reach them.
For each point, do
``` bash
crab submit -c crab_example.py
```
> Now GENSIM can also be made with condor, go to `rest/submit.py` to find out more

### GENSIM->RAWRECO->AODSIM->MINIAODSIM
* Go into `rest/`
* Now the CMSSW environment doesn't need to be the same of the previous step. A good example would be `CMSSW_8_0_21`.
#### Generate new psets 
In principle one can just use the given psets out of the box, and go directly to the running steps below. In case errors occur, the following steps are on how to generate the psets.

* For subsequent steps, one can make driver files and execute them to get psets. Example driver commands can be read off from the comment line on top of the existing psets.
* Use DIS with the `parent` option for a miniaod file to get a list of parents, and use the `driver` option to get the driver for generating them.
* Enter each one of those (including the original miniaod dataset name) into the `driver` option and make sure to suffix the dataset name in the query with `,this` to prevent the `driver` option from recursing up the parent tree for you (as it does by default)
* The pset names should be `pset_raw.py`, `pset_aod.py`, `pset_miniaod.py`

#### Using existing psets
* The psets don't need to have input files inside, as `submit.py` will override them correctly

#### Running the rest steps with condor
* Edit `submit.py` appropriately and do `python submit.py`. 
* Note that you are the looping mechanism, for example, in a `screen`, do `for i in $(seq 1 100); do python submit.py ; sleep 60m; done` This will loop over the directories every hour and submit any completed ones from the previous step.
* Eventually, files will show up in your configured directory in the `<tag>_MINIAOD` folder.

### MINIAODSIM->CMS3
* Use [AutoTwopler](https://github.com/cmstas/NtupleTools/tree/master/AutoTwopler)

