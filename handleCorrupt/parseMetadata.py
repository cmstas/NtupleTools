import os, sys, commands, json
import datetime, re, glob
from pprint import pprint

def find_corrupt():
    basedir = "/nfs-7/userdata/corruptCheck/"
    log_daym2 = "%s/corrupt-%s.txt" % (basedir, (datetime.datetime.now()-datetime.timedelta(days=2)).strftime("%a")) # day minus 2
    log_daym1 = "%s/corrupt-%s.txt" % (basedir, (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%a")) # day minus 1
    log_today = "%s/corrupt-%s.txt" % (basedir, datetime.datetime.now().strftime("%a"))
    files_done = "files_done.txt"
    if not os.path.exists(files_done): os.system("touch %s" % files_done)

    with open(log_daym2, "r") as fhin1:
        fileset_daym2 = set(["/hadoop"+line.split(":")[0].strip() for line in fhin1.readlines() if ".root" in line and "CORRUPT" in line])
    with open(log_daym1, "r") as fhin2:
        fileset_daym1 = set(["/hadoop"+line.split(":")[0].strip() for line in fhin2.readlines() if ".root" in line and "CORRUPT" in line])
    with open(log_today, "r") as fhin3:
        fileset_today = set(["/hadoop"+line.split(":")[0].strip() for line in fhin3.readlines() if ".root" in line and "CORRUPT" in line])
    with open(files_done, "r") as fhdone:
        files = set(map(lambda x: x.strip(), fhdone.readlines()))

    # take triple coincidence of corrupted files over 3 days
    corrupt_files = fileset_daym2 & fileset_daym1 & fileset_today
    corrupt_files -= files # disregard files we already processed in case they are still in corrupt list
    corrupt_files = list(corrupt_files)

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

def parse_metadata_old(fname, merged_file_num):
    with open(fname,"r") as fh: lines = fh.readlines()

    d_merged_nevents = {}
    d_unmerged_indices = {}
    d_indices_to_miniaod = {}
    pattern = re.compile(r'^unmerged[ ]+[0-9]+ /')
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
        elif pattern.search(line) is not None:
            idx, miniaod = line.strip().split(" ")[-2:]
            d_indices_to_miniaod[int(idx)] = miniaod

    merged_nevents = d_merged_nevents[merged_file_num]
    unmerged_indices = d_unmerged_indices[merged_file_num]
    miniaod = [(umidx,d_indices_to_miniaod[umidx]) for umidx in unmerged_indices]
    d_parsed = {"sample_name":sample_name, "xsec":xsec, "kfact":kfact, "efact":efact, "cms3tag":cms3tag, \
                "gtag":gtag, "sparms":sparms, "unmerged_dir":unmerged_dir, "merged_nevents":merged_nevents, \
                "unmerged_indices":unmerged_indices, "miniaod":miniaod}
    return d_parsed

def parse_metadata(fname, merged_file_num):
    data = {}
    with open(fname,"r") as fh: data = json.load(fh)

    d_parsed = {}
    d_parsed["sample_name"] = data["dataset"]
    d_parsed["xsec"] = data["xsec"]
    d_parsed["kfact"] = data["kfact"]
    d_parsed["efact"] = data["efact"]
    d_parsed["cms3tag"] = data["cms3tag"]
    d_parsed["gtag"] = data["gtag"]
    d_parsed["sparms"] = data["sparms"]

    d_parsed["unmerged_dir"] = data["crab"]["outputdir"]
    d_parsed["merged_nevents"] = data["nevents_merged"]

    d_parsed["unmerged_indices"] = data["imerged_to_ijob"][str(merged_file_num)]
    d_parsed["miniaod"] = []
    ijob_to_miniaod = data["ijob_to_miniaod"]
    if len(data["ijob_to_miniaod"].keys()) == 1: # then it's buggy (so do workaround): see /hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/W3JetsToLNu_NuPt-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/metadata.json
        ijob_to_miniaod = data["ijob_to_miniaod"][data["ijob_to_miniaod"].keys()[0]]

    for idx in data["imerged_to_ijob"][str(merged_file_num)]:
        if str(idx) in ijob_to_miniaod:
            d_parsed["miniaod"].append( (int(idx),ijob_to_miniaod[str(idx)][0]) )

    return d_parsed

def get_events(unmerged_indices, unmerged_dir):
    tot_events, tot_effevents = 0, 0
    for ifile in unmerged_indices:
        status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%s\",false)' | grep nevents" % (unmerged_dir, ifile))
        events = int(macroData.split("=")[-1])
        status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%s\",true)' | grep nevents" % (unmerged_dir, ifile))
        effevents = int(macroData.split("=")[-1])
        tot_events += events
        tot_effevents += effevents
    return tot_events, tot_effevents

def all_unmerged_exists(unmerged_indices, output_dir):
    have_unmerged, where = False, None
    for ifile in unmerged_indices:
        unmerged_filename = "%s/ntuple_%i.root" % (d_parsed["unmerged_dir"], ifile)
        if not os.path.exists(unmerged_filename): break # if we're missing a single unmerged, break
    else: 
        have_unmerged = True # an 'else' with a for loop gets executed if we complete the whole for loop without breaking
        where = "original"

    # if we didn't find unmerged in original user directory, look for it in remade directory
    if not have_unmerged:
        for ifile in unmerged_indices:
            unmerged_filename = "%s/unmerged/ntuple_%i.root" % (output_dir, ifile)
            if not os.path.exists(unmerged_filename): break
        else: 
            have_unmerged = True
            where = "remade"

    return have_unmerged, where

def have_made_merged(merged_file_num, output_dir):
    remade_merged_filename = "%s/merged/merged_ntuple_%i.root" % (output_dir, merged_file_num)
    print remade_merged_filename
    return os.path.exists(remade_merged_filename)

def make_mergelist(shortname, output_dir, mergemetadata_fname):
    with open("%s/cfg.sh" % (shortname),"w") as f_cfg:
        f_cfg.write("export inputListDirectory=%s/mergeLists/\n" %(shortname))
        f_cfg.write("export mData=%s\n" %(mergemetadata_fname))
        f_cfg.write("export outputDir=%s/merged/\n" %(output_dir))
        f_cfg.write("export dataSet=%s\n" %(shortname))
        f_cfg.write("export workingDirectory=%s\n" %(os.getcwd()))
        if (os.environ["SCRAM_ARCH"].startswith('slc6_amd64_gcc49')):
            f_cfg.write("export executableScript=%s/libsh/mergeScriptRoot6.sh\n" %(os.getcwd()))
        else:    
            f_cfg.write("export executableScript=%s/libsh/mergeScript.sh\n" %(os.getcwd()))

def submit_condor_jobs(d_parsed, merged_file_num, output_dir):
        sparms = d_parsed["sparms"]
        if len(sparms) == 0: pset = "pset_mc.py"
        elif len(sparms.split(",")) == 2: pset = "pset_mc_2sparms.py"
        elif len(sparms.split(",")) == 3: pset = "pset_mc_3sparms.py"

        status, condorq = commands.getstatusoutput("condor_q %s -l | grep Args" % os.getenv("USER"))

        # only resubmit jobs that aren't in condor_q and don't exist in the output area
        miniaod_tosubmit = []
        for umidx, filename in d_parsed["miniaod"]:
            if os.path.isfile("%s/unmerged/ntuple_%i.root" % (output_dir,umidx)): continue
            if filename in condorq: continue

            with open("files.txt", "w") as fh: fh.write(filename + "\n")

            cmd = "./submit.sh files.txt  {0} {1} {2} -1 false {3} {4} {5} {6}".format(
                    datetime.datetime.now().strftime("%s"),
                    output_dir + "/unmerged/",
                    d_parsed["cms3tag"],
                    "ntuple_%i.root" % umidx,
                    d_parsed["gtag"],
                    d_parsed["sample_name"],
                    pset
                    )

            os.system(cmd)

def submit_mergejobs(shortname, merged_file_num):
    status, condorq = commands.getstatusoutput("condor_q %s -l | grep Args" % os.getenv("USER"))
    if shortname in condorq and "merged_list_%i.txt" % merged_file_num in condorq: return 0
    os.system("./submitMergeJobs.sh {0}/cfg.sh >& {0}/submit.log".format(shortname))
    return 1


if __name__ == '__main__':
    # corrupt_files = find_corrupt() # FIXME uncomment when finished testing
    corrupt_files = [
            "/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/W3JetsToLNu_NuPt-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/merged_ntuple_44.root",
             "/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/W3JetsToLNu_NuPt-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/merged_ntuple_9.root",
             ]

    for cf in corrupt_files:
        pfx = "[%s...]" % (cf.split("/")[7][:25])
        metadata_fname = cf.split("/merged_")[0] + "/metadata.json"
        # metadata_fname = "tempMetaData.txt" # FIXME want to use actual file, not testing, so delete this
        if not os.path.exists(metadata_fname): continue # no metadata, so useless to try to figure out stuff

        print pfx, "found corrupt and metadata for", cf

        merged_file_num = int(cf.split("merged_ntuple_")[1].split(".root")[0])
        d_parsed = parse_metadata(metadata_fname, merged_file_num)

        shortname = d_parsed["unmerged_dir"].split("/crab_")[1].split("/")[0]
        output_dir = "/hadoop/cms/store/user/%s/corruption/%s/" % (os.getenv("USER"), shortname)

        have_unmerged, unmerged_location = all_unmerged_exists(d_parsed["unmerged_indices"], output_dir)

        if not have_unmerged:
            print pfx, "submitting condor jobs to remake unmerged"
            submit_condor_jobs(d_parsed, merged_file_num, output_dir) # FIXME uncomment so we submit condor jobs if unmerged don't exist
            continue

        print pfx, "found all unmerged files in %s dir" % unmerged_location

        have_new_unmerged = have_made_merged(merged_file_num, output_dir)
        if have_new_unmerged:
            # copy over the merged to "cf"
            print pfx, "copying over new merged file to", cf # FIXME do it

            with open("files_done.txt", "a") as fhdone:
                fhdone.write(cf + "\n")

            continue

        os.system("mkdir -p %s/mergeLists/" % shortname)

        print pfx, "getting event counts"
        tot_events, tot_effevents = get_events(d_parsed["unmerged_indices"], d_parsed["unmerged_dir"]) # FIXME uncomment when finished testing
        # tot_events, tot_effevents = 999, 999

        if unmerged_location == "original":
            files_to_merge = ["%s/ntuple_%i.root" % (d_parsed["unmerged_dir"],ifile) for ifile in d_parsed["unmerged_indices"]]
        elif unmerged_location == "remade":
            files_to_merge = ["%s/unmerged/ntuple_%i.root" % (output_dir,ifile) for ifile in d_parsed["unmerged_indices"]]
        mergelist_fname = shortname+"/mergeLists/merged_list_%i.txt" % merged_file_num
        mergemetadata_fname = shortname+"/merge_metaData.txt"
        with open(mergelist_fname, "w") as fh:
            fh.write("%s" % "\n".join(files_to_merge))

        with open(mergemetadata_fname, "w") as fh:
            fh.write("n: %i\neffN: %i\nk: %f\nf: %f\nx: %f\nfile: merged_list_%i.txt" \
                    % (tot_events, tot_effevents, d_parsed["kfact"], d_parsed["efact"], d_parsed["xsec"], merged_file_num))

        make_mergelist(shortname, output_dir, mergemetadata_fname)
        print pfx,"made mergelist"

        did_submit = submit_mergejobs(shortname, merged_file_num) # FIXME uncomment when finished testing
        if did_submit:
            print pfx,"submitted merge jobs"


