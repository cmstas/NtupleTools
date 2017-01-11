# AutoTwopler
## Instructions
0. Fill in a Twiki page with the canonical sample format: cross-section, k-factor, filter efficiency, dataset name, sparms, assigned name, etc.
1. Make a `~/.twikipw` file in your home directory that contains only your Twiki password. For security, do `chmod 600 ~/.twikipw`.
2. Modify/add JECs, CMSSW release, pset names, etc. inside `params.py`
3. Start a `screen`
4. `source setup.sh` and go to the dashboard URL printed out
5. Use the dashboard to create an `instructions.txt` file (or you can skip this and make your own, following the `instructions_test.txt` template)
  1. Click "twiki" at the top
  2. Fill in your assigned name (or "all" for everyone's samples), Twiki username, and select the appropriate Twiki (if not there, select "Other" and paste in the twiki link)
  3. You also have the option to only fetch the unmade samples (i.e., those without a filled-in final directory on the Twiki)
  4. Click "Fetch from Twiki" to put the `instructions.txt` content into the box. Feel free to edit this as needed.
  5. Click `Add to instructions.txt` to automatically take the text in the box and put it in the `instructions.txt` file in your AutoTwople directory
6. Now, back in the terminal, enter the same `screen` and do `source setup.sh` (maybe a couple of times until it doesn't complain)
7. Do `python run.py instructions.txt`
8. Sit quack and relax
9. When you see done samples on the dashboard, you can enter your username in the Twiki section, select the appropriate Twiki, and hit "Update Twiki with done samples" to automagically fill in the Twiki entries

### Basic API usage
```python
import run
import params as p
p.merging_scripts = [] # or modify it however you want
run.main(instructions="instructions.txt", params=p)
```

## Babymaking
Babies can be made using the AutoTwopler as well. These instructions will be updated (and the procedure streamlined) in the future. The inputs to the 
babymaking process are an executable script, a package tar file, a dataset name, an analysis name ("SS", "MT2", etc.), and a baby tag version ("v0.1", "v1.2-fix", etc.).

### Package and executable
For examples of these, visit `http://uaf-6.t2.ucsd.edu/~namin/dump/baby_ducks.sh` and `http://uaf-6.t2.ucsd.edu/~namin/dump/package.tar.gz`. The package file will be untarred
once on the compute node, so make sure it has all your dependences including JECs, SFs, etc. The executable is just a shell script that will be fed with basic information like the dataset, filename, tags, merged file number, and others (see example for variable names).
The content between the BEGIN and END markers is provided by the user and must properly handle the input that is injected by the AutoTwopler before and after it. In order to be compatible with the content injected afterwards (the `lcg-cp` command),
the script _MUST_ make sure the output root file is named `output.root`! At a minimum, the bare user-provided executable must take a variable `$FILENAME` containing the CMS3 file to run on and produce an output ROOT file named `output.root`.

### SweepRoot
If a shell script for sweepRoot is specified inside of `params.py`, then this script will be copied over into the AutoTwopler directory and executed. It will be given one parameter: the filename. It must end in `exit 0` to indicate a good file, or anything else for a bad file (to be deleted automatically). If no script is specified (empty string), then all files will pass the "sweepRoot" step. Note that because this parameter is a list, you may specify additional files that are also needed for sweepRooting (the first file must be the executable).

### Merging
Same story for merging as with sweepRoot. Parameters will be fed as in the example script in `baby_devel/mergeScript.sh`. Actually, you might as well just use this script since it already uses the main merging function for SNT and handles multiple output file types. Note that the final merged directory for babies must be specified inside `params.py` following the instructions there.

### Instructions syntax
In the instructions file, an example line might look like
```
BABY FA v1.01 /ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM /home/users/namin/duck_80x/NtupleTools/AutoTwopler/baby/test/package.tar.gz /home/users/namin/duck_80x/NtupleTools/AutoTwopler/baby/test/condor_executable.sh
```

Note here that the first token must be `BABY` to tell the AutoTwopler the type of job. The next token is the analysis code (FA stands for Fake Analysis, but 'SS' would go here, for example).
The next token is simply a user-constructed tag for the baby-making campaign. Next is the dataset. Finally, you must provide the full path to the package tarfile and executable script.

With these input parameters, the output baby files will be located in `/hadoop/cms/store/user/namin/AutoTwopler_babies/FA_v1.01/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/`.

### Helpful commands
Inside the `scripts/` directory, there is a `dis_client.py` script that makes querying the Twiki easy. For example, in order to get a list of datasets associated with a particular tag, you can do
`python dis_client.py -t snt "*,cms3tag=CMS3_V08-00-01 | grep dataset_name"`. 

Or, for a particular Twiki page, you can do `python dis_client.py -t snt "*,twiki_name=Run2Samples25ns80X | grep dataset_name".` 


Finally, using the `location` key (which gives the hadoop directory of the merged files for a given CMS3 sample), we can find out how many merged files
exist for a given campaign. If we run baby making scripts on merged files 1-to-1, then this number is how many jobs we will farm out. Do
`for i in $(dis_client.py -t snt "*,twiki_name=Run2Samples25ns80X | grep location"); do ls -1 $i/*.root | wc -l; done | awk '{s+=$1} END {print s}'`.

## Misc
### Ntupling private samples (assuming they are accessible via xrootd)
- Prepare a text file (must end with `.txt`) with the list of files. There are two required parts: a "fake dataset" name which will follow the same parsing of regular datasets, and (of course) the list of files. An optional third piece of information lets you control the file splitting in case there are many files with few events. Structure the file as follows
```bash
$ cat filelist.txt
dataset: /SMS-T2tt_TEST/SMS_T2tt_mStop-170_mLSP-1_Private74X-TEST-v2/USER
files_per_job: 4
/store/user/namin/ana_MC/SMST2tt/SMS-T2tt_mStop-170/SMS-T2tt_mStop-170_mLSP-1_madgraphMLM-pythia8_RunIISpring15MiniAODv2-FastAsympt25ns_74X_MINIAODSIM_b0.root
/store/user/namin/ana_MC/SMST2tt/SMS-T2tt_mStop-170/SMS-T2tt_mStop-170_mLSP-1_madgraphMLM-pythia8_RunIISpring15MiniAODv2-FastAsympt25ns_74X_MINIAODSIM_b100.root
/store/user/namin/ana_MC/SMST2tt/SMS-T2tt_mStop-170/SMS-T2tt_mStop-170_mLSP-1_madgraphMLM-pythia8_RunIISpring15MiniAODv2-FastAsympt25ns_74X_MINIAODSIM_b101.root
```
- Next, get the full path to this filelist and put it in your instructions.txt in place of the dataset name, i.e., `/home/users/namin/forFrank/NtupleTools/AutoTwopler/filelist.txt 74X_mcRun2_asymptotic_v2 1 1 1 mStop,mLSP`
- Now everything else follows as usual.

### Ntupling multiple campaigns at once
- You can't. However, since everything in the AutoTwopler directory is self contained, you can copy the directory and change the campaign in `params.py` (as well as `dashboard_name` in `params.py`, so that running multiple instances will have different dashboard URLs)

### CRAB fails to submit. What do I do?
- Did you submit a lot of jobs recently? It could be that storage space on schedds is clogged. See https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#User_quota_in_the_CRAB_scheduler. In this case, you can use the "get_crabcache_info()" code block in `scripts/misc.py` to purge old crab task metadata if you're sure you won't ever need it again
- "SUBMITFAILED: Impossible to retrieve proxy...":
   * First things first: since the job didn't actually submit, you will need to delete the appropriate crab task folder inside `crab/` before you're able to resubmit.
   * Delete `~/.globus/proxy_for_${USER}.file` and rerun `run.py`. This time you will be prompted for a password. Submission should go smoothly from there. If problem persists, proceed to next bullet point.
   * Edit `params.py` and set `FORSAKE_HEAVENLY_PROXY = True`. If problem persists, proceed to next bullet point.
   * Do `voms-proxy-destroy` to kill your proxy and let the scripts automatically make one for you. If problem persists, proceed to next bullet point.
   * If you have the same issue again, try to follow https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_command_fails_with_Impossib. Again, if you get the same thing, try doing the `voms-proxy-destroy` again.  Repeat this alternating technique until you get lucky enough to pass the CRAB trials and tribulations. If problem persists, then I don't know what the hell to do.

### Changing xsec, kfactor, filteff
- Has your sample started postprocessing yet? No? Good. Simply update the instructions file to have your new values and the AutoTwopler will pick up the new information
- If the sample is done already, you can run `scripts/new_xsec.py` (after modifying the bottom part to have the right directories and xsecs). This will submit jobs to change branches via condor, and put the output
into [input directory]/new_xsec. It is up to the user to verify that the xsec was done properly and event count remains the same. Then you can just delete the merged files and move in the new ones. Note that running the script multiple times will re-submit failed jobs, so re-run until all are done.

### Doctor, doctor, help!
- `python doctor.py` checks some basic things like writing to T2_US_UCSD using crab, xrootd, CMS3 branches, etc.
- Ensure that the first sample in the `instructions.txt` file is a FullSim MC sample

## TODO:
- [ ] Make tester class
