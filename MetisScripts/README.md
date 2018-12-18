# Metis submission scripts for 2016,2017,2018 data and MC
Current as of Dec 18, 2018

### Setup
`git clone https://github.com/aminnj/ProjectMetis` and get environment (`cd ProjectMetis ; source setup.sh`)

### Edit
Read scripts before submitting.
`triple.py` is the driver script. It loads tasks from the other python files and processes them.
To select which datasets to process, edit the corresponding script. Comment out various years in `triple.py`
to speed up the loop over tasks, and comment out PDs/datasets/etc in the other scripts to also avoid
instantiating too many tasks. Of course don't comment them out if they are not done.


### Submission
`python triple.py`


### Misc scripts
* `misc/checkminiaodv3.py` -- paste in MINIAODSIM names of 2016 MiniAODv3 MC samples, and then query past SNT samples for xsec information. Spit out the format needed in the other metis submission scripts.

