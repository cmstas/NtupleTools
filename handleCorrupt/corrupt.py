import datetime
import re
import glob

def find_corrupt():
    basedir = "/nfs-7/userdata/corruptCheck/"
    log_yesterday = "%s/corrupt-%s.txt" % (basedir, (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%a"))
    log_today = "%s/corrupt-%s.txt" % (basedir, datetime.datetime.now().strftime("%a"))

    with open(log_yesterday, "r") as fhin1:
        fileset_yesterday = set(["/hadoop"+line.split(":")[0].strip() for line in fhin1.readlines() if ".root" in line and "CORRUPT" in line])
    with open(log_today, "r") as fhin2:
        fileset_today = set(["/hadoop"+line.split(":")[0].strip() for line in fhin2.readlines() if ".root" in line and "CORRUPT" in line])

    # take coincidence of corrupted files over 2 days
    corrupt_files = list(fileset_yesterday & fileset_today)

    # prune files we don't care about
    corrupt_files_new = []
    for cf in corrupt_files :
        if cf.endswith(".bad"): continue
        if "_old" in cf or "_wrong" in cf or "wrong_" in cf: continue
        if "run2_25ns" not in cf and "run2_data" not in cf and "run2_fastsim" not in cf: continue
        corrupt_files_new.append(cf)

    d_corrupt = {cf:1 for cf in corrupt_files_new}

    corrupt_files = []
    for cf in d_corrupt.keys(): 
        # V07-04-11
        reg = re.compile("[V]?[0-9]{2}-[0-9]{2}-[0-9]{2}")
        search = reg.search(cf)
        if search is not None:
            match = reg.search(cf).group()
            versions = glob.glob(cf.replace(match,"*"))

            # make list of pairs of (filename, tag), then sort by tag so newest is last
            versions = map(lambda x: (x[0],x[1].group()), [m for m in map(lambda x: (x,reg.search(x)), versions) if m[0]])
            versions = sorted(versions, key=lambda x: x[1].replace("V",""))

            # if we ended up with more than 1 tag and the latest 2 files don't have
            # the same tag since one might be "V07-04-11" and another
            # "V07-04-11_oldxsec". don't bother parsing, just remake anyways
            if len(versions) > 1 and versions[-1][1] != versions[-2][1]:
                newestfile = versions[-1][0]
                # if the newest file is not corrupted, we don't care about remaking the old one
                if newestfile not in d_corrupt: continue

        corrupt_files.append(cf)

    return corrupt_files

if __name__=='__main__':
    corrupt_files = find_corrupt()
    print "Found %i corrupt files" % len(corrupt_files)
    for cf in corrupt_files: print cf
