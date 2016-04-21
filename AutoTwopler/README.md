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

### CRAB doesn't submit. What do I do?
- Did you submit a lot of jobs recently? It could be that storage space on schedds is clogged. See https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#User_quota_in_the_CRAB_scheduler. In this case, you can use the "get_crabcache_info()" code block in `scripts/misc.py` to purge old crab task metadata if you're sure you won't ever need it again
- Did you not use your GRID certificate recently? I think proxies can go stale (whatever the hell that means) and CRAB will complain with something like "Impossible to retrieve proxy..." and say "SUBMITFAILED". Try to do `voms-proxy-destroy` to kill your proxy and let the scripts automatically make one for you. If you have the same issue again, try to follow https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#crab_command_fails_with_Impossib . Again, if you get the same thing, try doing the `voms-proxy-destroy` again. Repeat this alternating technique until you get lucky enough to pass the CRAB trials and tribulations.


## TODO:
- [ ] Support for CMS3 to babies
- [ ] Integrate DIS querying
- [x] Check that nothing happened to the files after copying (don't need to do full blown checkCMS3, just check event counts or something)
- [x] Parse checkCMS3 output and remake stuff appropriately
- [x] Be able to change xsec, kfact, efact before post-processing (through an updated instructions.txt)
- [x] Copy metadata (AND json) to backup directory (right now, it's copied only to the final directory)
- [x] If merged files are already in the final directory, either warn users or mark job as done
- [ ] Be able to nuke and resubmit job from dashboard
- [ ] Be able to instantaneously refresh statuses (i.e., interrupt sleeping for 10m)
- [x] Resubmit crab task if been bootstrapped or some other thing for longer than x minutes
- [x] Don't wait on last x% of MC samples to finish up in crab (put a mask on the job number)
- [ ] Have Condor submission possibility for certain jobs that misbehave
- [x] Make postprocessing part of sample show all jobs done when in done state
- [x] If user enters full twiki name into the dashboard, automatically strip the beginning part to get only the end
- [x] Re-read instructions.txt file every iteration to pull in new information/samples
- [x] If crab status hasn't run yet, we'll have 0 running / 0 total, so monitor page shows NaN%. Force this to 0%.
- [ ] Make tester class
- [x] Add example instructions.txt
- [x] Grep condor logfiles to get timing statistics for merging/branchadding/copying
- [x] Handle odd hadoop mappings
- [x] Use time_stats key in data to make plot of crab jobs over time
- [ ] Deal with CRAB HTTP exceptions elegantly
- [x] Don't rely on CRAB status codes. Use our own. In particular. isComplete = (nComplete == nTot) is all one needs.
- [ ] Put crab output into a folder within hadoop so that not all folders are directly in hadoop, maybe use "80X" or "76X" as the folder, so /hadoop/cms/..../namin/80X/crab_TTJets.....
- [ ] Parallelize sweepRooting (sweepRooting goes at 0.3Hz whereas finding miniaod is at 10Hz)
