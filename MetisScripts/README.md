# Metis submission scripts for 2016,2017,2018 data and MC
Current as of May 19, 2019

### Setup
`git clone https://github.com/aminnj/ProjectMetis` and get environment (`cd ProjectMetis ; source setup.sh`)

### Submission

Main submission script is `bigly.py` because it submits **bigly**. Of course read the script source and modify it appropriately before blindly executing things.

### Misc scripts
* `misc/checkminiaodv3.py` -- paste in MINIAODSIM names of 2016 MiniAODv3 MC samples, and then query past SNT samples for xsec information. Spit out the format needed in the other metis submission scripts.
* `misc/to_backup.py` -- has functions to print out directories containing samples we care about (i.e., latest CMS4 tags, not ones scheduled for deletion, etc)
* `misc/find_corruptions.py` -- uses `misc/to_backup.py` and `hdfs fsck` to find corrupted files that we care to remake
* `misc/find_older_cms4.py` -- example script to find outdated CMS4 according to DIS
* `misc/find_older_cms4_general.py` -- similar to above, but just using `ls`, and uses python pandas if you're into that
* `misc/get_local_commands.py` -- example script that prints out cmsRun commands to run CMS4 metis jobs locally
* `misc/useful_condor_commands.sh` -- bash script with useful aliases/functions for condor
* `misc/check_corrupt_fast.py` -- super fast (and super loose) check for corruptions in a folder with uproot

