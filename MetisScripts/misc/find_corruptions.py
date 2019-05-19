import os
from to_backup import get_folders

if __name__ == "__main__":

    good_folders = sum([
        get_folders("run2_data2016_94x"), # ~25TB
        get_folders("run2_data2017"), # ~27TB
        get_folders("run2_data2018"), # ~28TB
        get_folders("run2_mc2016_94x"),
        get_folders("run2_mc2017"),
        get_folders("run2_mc2018"),
        ], [])
    print "# Found {} folders housing the latest/greatest ntuples".format(len(good_folders))

    # True if we care about the sample enough to remake corruptions
    def do_care(x):
        if "CMS4_V10" not in x: return False
        return any(x.startswith(gf) for gf in good_folders)

    # Run fsck to get list of corruptions to parse
    os.system("hdfs fsck /cms/store/group/snt/ > tmp.txt")
    fnames = []
    with open("tmp.txt","r") as fh:
        for line in fh:
            if not line.startswith("/cms/store/group/snt"): continue
            if "CORRUPT" not in line: continue
            hfname = line.split(":",1)[0].strip()
            fname = "/hadoop{}".format(hfname)
            fnames.append(fname)
    fnames = sorted(list(set(fnames)))
    print "# Found {} corruptions".format(len(fnames))

    fnames = filter(do_care,fnames)
    print "# Of those, we only care about {} files".format(len(fnames))
    print "# So delete the below and resubmit with bigly".format(len(fnames))
    print "\n".join(fnames)



