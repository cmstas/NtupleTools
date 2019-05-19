# Metis submission scripts for 2016,2017,2018 data and MC
Current as of May 19, 2019

### Setup
`git clone https://github.com/aminnj/ProjectMetis` and get environment (`cd ProjectMetis ; source setup.sh`)

### Submission

Main submission script is `bigly.py` because it subits **bigly**. Of course read the script source and modify it appropriately before blindly executing things.

### Misc scripts
* `misc/checkminiaodv3.py` -- paste in MINIAODSIM names of 2016 MiniAODv3 MC samples, and then query past SNT samples for xsec information. Spit out the format needed in the other metis submission scripts.
* `misc/to_backup.py` -- has functions to print out directories containing samples we care about (i.e., latest CMS4 tags, not ones scheduled for deletion, etc)
* `misc/find_corruptions.py` -- uses `misc/to_backup.py` and `hdfs fsck` to find corrupted files that we care to remake

