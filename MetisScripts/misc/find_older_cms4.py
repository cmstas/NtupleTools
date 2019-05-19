import scripts.dis_client as dis
import os
import sys
import glob

def num_missing_files(location):
    rfiles = glob.glob("{}/*.root".format(location))
    indices = set(map(lambda x: int(x.rsplit("_",1)[-1].split(".",1)[0]), rfiles))
    if not len(indices): return 999
    missing_indices = set(range(1,max(indices)+1))-indices
    return len(missing_indices)

def get_cmd(location):
    return "dir={}; mv $dir ${{dir}}_todelete".format(location.rstrip("/"))

def print_cmds(old, new, label="Something"):
    dsold, dsnew = set(old), set(new)
    print "################### {} ###################".format(label)
    print "# Folders that exist in newer tag now:"
    for ds in sorted(dsnew & dsold, key=old.get):
        oldloc = old[ds]
        newloc = new[ds]
        if not os.path.exists(oldloc): continue
        nmiss = num_missing_files(newloc)
        if nmiss > 0:
            print "# Can't delete {} because there's {} missing files still".format(oldloc,nmiss)
            continue
        print get_cmd(oldloc)
    print


if __name__ == "__main__":

    # 2017 MC
    old = {x["dataset_name"]:x["location"] for x in dis.query("*Fall17MiniAOD*,cms3tag=CMS4_V10-02-04 | grep dataset_name,location",typ="snt")["response"]["payload"]}
    new = {x["dataset_name"]:x["location"] for x in dis.query("*Fall17MiniAOD*,cms3tag=CMS4_V10-02-05 | grep dataset_name,location",typ="snt")["response"]["payload"]}
    dsold, dsnew = set(old), set(new)
    print_cmds(old,new,label="2017 MC")

    # 2017 Data
    old = {x["dataset_name"]:x["location"] for x in dis.query("/*/*Run2017*/MINIAOD,cms3tag=CMS4_V10-02-04 | grep dataset_name,location",typ="snt")["response"]["payload"]}
    new = {x["dataset_name"]:x["location"] for x in dis.query("/*/*Run2017*/MINIAOD,cms3tag=CMS4_V10-02-05 | grep dataset_name,location",typ="snt")["response"]["payload"]}
    dsold, dsnew = set(old), set(new)
    print_cmds(old,new,label="2017 data")

    # 2016 Data
    old = {x["dataset_name"]:x["location"] for x in dis.query("/*/*Run2016*/MINIAOD,cms3tag=CMS4_V10-02-04 | grep dataset_name,location",typ="snt")["response"]["payload"]}
    new = {x["dataset_name"]:x["location"] for x in dis.query("/*/*Run2016*/MINIAOD,cms3tag=CMS4_V10-02-05 | grep dataset_name,location",typ="snt")["response"]["payload"]}
    dsold, dsnew = set(old), set(new)
    print_cmds(old,new,label="2016 94X data")
