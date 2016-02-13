import os, sys, commands
import datetime, re, glob
from pprint import pprint

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

def parse(fname, merged_file_num):
    with open(fname,"r") as fh: lines = fh.readlines()

    d_merged_nevents = {}
    d_unmerged_indices = {}
    for line in lines:
        if line.startswith("sampleName"): sample_name = line.split(":")[1].strip()
        elif line.startswith("xsec"): xsec = float(line.split(":")[1].strip())
        elif line.startswith("k-fact"): kfact = float(line.split(":")[1].strip())
        elif line.startswith("e-fact"): efact = float(line.split(":")[1].strip())
        elif line.startswith("cms3tag"): cms3tag = line.split(":")[1].strip()
        elif line.startswith("gtag"): gtag = line.split(":")[1].strip()
        elif line.startswith("sparms"): sparms = line.split(":")[1].replace("_","").strip()
        elif line.startswith("unmerged files are in:"): unmerged_dir = line.split(":")[1].strip()
        elif line.startswith("merged file constituents"):
            ifile, constituents = line.replace("merged file constituents","").split(":")
            ifile = int(ifile)
            constituents = constituents.strip()
            if " " in constituents: constituents = map(int, constituents.split(" "))
            else: constituents = int(constituents)
            d_unmerged_indices[ifile] = constituents
        elif line.startswith("merged file nevents"):
            ifile, nevents = map(int,line.replace("merged file nevents","").split(":"))
            d_merged_nevents[ifile] = nevents

    merged_nevents = d_merged_nevents[merged_file_num]
    unmerged_indices = d_unmerged_indices[merged_file_num]
    d_parsed = {"sample_name":sample_name, "xsec":xsec, "kfact":kfact, "efact":efact, "cms3tag":cms3tag, \
                "gtag":gtag, "sparms":sparms, "unmerged_dir":unmerged_dir, "merged_nevents":merged_nevents, "unmerged_indices":unmerged_indices}
    pprint(d_parsed)
    return d_parsed

def get_events(unmerged_indices, unmerged_dir):
    tot_events, tot_effevents = 0, 0
    for ifile in unmerged_indices:
        status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%sntuple_%i.root\",false,true)' | grep nevents" % (unmerged_dir, ifile))
        events = int(macroData.split("=")[-1])
        status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%sntuple_%i.root\",true,true)' | grep nevents" % (unmerged_dir, ifile))
        effevents = int(macroData.split("=")[-1])
        tot_events += events
        tot_effevents += effevents
    return tot_events, tot_effevents

def all_unmerged_exists(unmerged_indices):
    have_unmerged = False
    for ifile in unmerged_indices:
        unmerged_filename = "%s/ntuple_%i.root" % (d_parsed["unmerged_dir"], ifile)
        if not os.path.exists(unmerged_filename): break # if we're missing a single unmerged, break
    else: have_unmerged = True # an 'else' with a for loop gets executed if we complete the whole for loop without breaking
    return have_unmerged

def make_mergelist(shortname, output_dir, mergemetadata_fname):
    with open("%s/cfg.sh" % (shortname),"w") as f_cfg:
        f_cfg.write("export inputListDirectory=%s/mergeLists/\n" %(shortname))
        f_cfg.write("export mData=%s\n" %(mergemetadata_fname))
        f_cfg.write("export outputDir=%s\n" %(output_dir))
        f_cfg.write("export dataSet=%s\n" %(shortname))
        f_cfg.write("export workingDirectory=%s\n" %(os.getcwd()))
        if (os.environ["SCRAM_ARCH"]=='slc6_amd64_gcc491'): 
            f_cfg.write("export executableScript=%s/libsh/mergeScriptRoot6.sh\n" %(os.getcwd()))
        else:    
            f_cfg.write("export executableScript=%s/libsh/mergeScript.sh\n" %(os.getcwd()))

def submit_mergejobs(shortname):
    os.system("./submitMergeJobs.sh {0}/cfg.sh >& {0}/submit.log".format(shortname))


if __name__ == '__main__':
    # corrupt_files = find_corrupt() # FIXME uncomment when finished testing
    corrupt_files = ["/hadoop/cms/store/group/snt/run2_25ns_MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/V07-04-12/merged_ntuple_2.root"]
    for cf in corrupt_files:
        pfx = "[%s]" % (cf.split("/")[7].split("-")[0])
        metadata_fname = cf.split("/merged_")[0] + "/metadata.txt"
        metadata_fname = "tempMetaData.txt" # FIXME want to use actual file, not testing, so delete this
        if not os.path.exists(metadata_fname): continue # no metadata, so useless to try to figure out stuff

        merged_file_num = int(cf.split("merged_ntuple_")[1].split(".root")[0])
        d_parsed = parse(metadata_fname, merged_file_num)

        have_unmerged = all_unmerged_exists(d_parsed["unmerged_indices"])
        # if not have_unmerged: REMAKE unmerged WITH CRAB # FIXME implement a way to resubmit them with crab or condor

        print pfx, "found all unmerged files"

        shortname = d_parsed["unmerged_dir"].split("/crab_")[1].split("/")[0]
        os.system("mkdir -p %s/mergeLists/" % shortname)
        output_dir = "/hadoop/cms/store/user/%s/corruption/merged/%s/" % (os.getenv("USER"), shortname)

        print pfx, "getting event counts"
        # tot_events, tot_effevents = get_events(d_parsed["unmerged_indices"], d_parsed["unmerged_dir"]) # FIXME uncomment when finished testing
        tot_events, tot_effevents = 999, 999

        files_to_merge = ["%s/ntuple_%i.root" % (d_parsed["unmerged_dir"],ifile) for ifile in d_parsed["unmerged_indices"]]
        mergelist_fname = shortname+"/mergeLists/merged_list_%i.txt" % merged_file_num
        mergemetadata_fname = shortname+"/merge_metaData.txt"
        with open(mergelist_fname, "w") as fh:
            fh.write("nEntries: %i\n%s" % (tot_events, "\n".join(files_to_merge)))

        with open(mergemetadata_fname, "w") as fh:
            fh.write("n: %i\neffN: %i\nk: %f\nf: %f\nx: %f\nfile: merged_list_%i.txt" \
                    % (tot_events, tot_effevents, d_parsed["kfact"], d_parsed["efact"], d_parsed["xsec"], merged_file_num))

        make_mergelist(shortname, output_dir, mergemetadata_fname)
        print pfx,"made mergelist"

        # submit_mergejobs(shortname) # FIXME uncomment when finished testing
        print pfx,"submitted merge jobs"

