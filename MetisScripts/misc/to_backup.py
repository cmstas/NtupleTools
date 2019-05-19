import os
import sys
import commands
import glob
from itertools import groupby

def retain_latest_tag(dirs):
    """
    Takes a list of files like ["/hadoop/blahblahblah/blah_CMS4_V10-02-04/", ...],
    finds duplicates (up to the _CMS4), and then only keeps the ones with the latest tag after _CMS4 (alphabetically)
    """
    d = {}
    for dirname in dirs:
        base,tag = dirname.rsplit("_CMS4",1)
        if base not in d: d[base] = []
        d[base].append(dirname)
    return list(map(lambda x: sorted(x)[-1],d.values()))

def get_folders(maindir,
        exclude_todelete=True,
        exclude_fastsim=True,
        exclude_oldtags=True,
        exclude_prompt2018=True,
        verbose=False,
        ):
    """
    Given a main SNT directory like `run2_data2018`, get a list of folders to back up
    `exclude_todelete`: Exclude folders that we've manually marked for deletion/deprecation that end with _todelete
    `exclude_fastsim`: Exclude SMS or fastsim
    `exclude_oldtags`: See `retain_latest_tag()`
    `exclude_prompt2018`: See comment below.
    """
    dirs = glob.glob("/hadoop/cms/store/group/snt/{0}/*/".format(maindir))
    if verbose: print("Found {0} dirs".format(len(dirs)))
    if exclude_todelete:
        dirs = [x for x in dirs if "_todelete" not in x]
        if verbose: print("Down to {0} dirs after dropping _todelete".format(len(dirs)))
    if exclude_fastsim:
        dirs = [x for x in dirs if ("Fast_" not in x and "SMS-T" not in x)]
        if verbose: print("Down to {0} dirs after dropping Fast_".format(len(dirs)))
    if exclude_oldtags:
        dirs = retain_latest_tag(dirs)
        if verbose: print("Down to {0} dirs after dropping old tags".format(len(dirs)))
    if exclude_prompt2018:
        # Drop 2018ABC prompt (because there is the 17Sep2018 rereco).
        # And for EGamma Dv2, drop the prompt because there is a special 22Jan2019 (RePrompt actually! to recover missing files)
        def keep(x):
            if "run2_data2018" not in x: return True
            if "Run2018A-PromptReco" in x: return False
            if "Run2018B-PromptReco" in x: return False
            if "Run2018C-PromptReco" in x: return False
            if "EGamma_Run2018D-PromptReco" in x: return False
            return True
        dirs = filter(keep,dirs)
        if verbose: print("Down to {0} dirs after dropping prompt 2018".format(len(dirs)))
    # Sanity check. If it's MC, make sure RunII is in the name
    dirs = [x for x in dirs if ("run2_data" in x or "RunII" in x)]
    dirs = sorted(dirs)
    return dirs

def get_size_gb(dirs):
    """
    Given list a hadoop directories, return physical file size in GB
    """
    cmd = "hadoop fs -du -s {0}".format(" ".join([d.replace("/hadoop","") for d in dirs]))
    stat,out = commands.getstatusoutput(cmd)
    sizes = {}
    for line in out.strip().splitlines():
        try:
            sizes[line.split()[1]] = int(line.split()[0]) 
        except:
            pass
    ngb = sum(sizes.values())*1.0e-9
    return ngb

if __name__ == "__main__":

    dirs = []

    # # ~90 TB
    # dirs += get_folders("run2_mc2016_94x")
    # dirs += get_folders("run2_mc2017")
    # dirs += get_folders("run2_mc2018")

    # # ~80 TB
    dirs += get_folders("run2_data2016_94x") # ~25TB
    dirs += get_folders("run2_data2017") # ~27TB
    dirs += get_folders("run2_data2018") # ~28TB

    print len(dirs)
    print get_size_gb(dirs)

    print "\n".join(sorted(dirs))

