import os, sys, commands

def parse(fname, merged_file_num):
    with open(fname,"r") as fh: lines = fh.readlines()

    d_merged_nevents = {}
    d_merged_constituents = {}
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
            d_merged_constituents[ifile] = constituents
        elif line.startswith("merged file nevents"):
            ifile, nevents = map(int,line.replace("merged file nevents","").split(":"))
            d_merged_nevents[ifile] = nevents

    merged_nevents = d_merged_nevents[merged_file_num]
    merged_constituents = d_merged_constituents[merged_file_num]
    return {"sample_name":sample_name, "xsec":xsec, "kfact":kfact, "efact":efact, "cms3tag":cms3tag, \
            "gtag":gtag, "sparms":sparms, "unmerged_dir":unmerged_dir, "merged_nevents":merged_nevents, "merged_constituents":merged_constituents}

corrupt_file = "/hadoop/cms/store/group/snt/run2_25ns_MiniAODv2/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/V07-04-12/merged_ntuple_2.root"
metadata_fname = corrupt_file.split("/merged_")[0] + "/metadata.txt"
metadata_exists = os.path.exists(metadata_fname)
# if not metadata_exists: continue # FIXME want to uncomment
merged_file_num = int(corrupt_file.split("merged_ntuple_")[1].split(".root")[0])
metadata_fname = "tempMetaData.txt" # FIXME want to use actual file, not testing, so delete this
d_parsed = parse(metadata_fname, merged_file_num)
print d_parsed
have_unmerged = False
for ifile in d_parsed["merged_constituents"]:
    unmerged_filename = "%s/ntuple_%i.root" % (d_parsed["unmerged_dir"], ifile)
    if not os.path.exists(unmerged_filename): break # if we're missing a single unmerged, break
else:
    have_unmerged = True # an 'else' with a for loop gets executed if we complete the whole for loop without breaking

print have_unmerged
# if not have_unmerged: REMAKE unmerged WITH CRAB # FIXME implement a way to resubmit them with crab or condor

shortname = d_parsed["unmerged_dir"].split("/crab_")[1].split("/")[0]
os.system("mkdir -p %s/mergeLists/" % shortname)
output_dir = "/hadoop/cms/store/user/%s/remerge/%s/" % (os.getenv("USER"), shortname)

tot_events, tot_effevents = 0, 0
print "Counting events"
for ifile in d_parsed["merged_constituents"]:
    status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%sntuple_%i.root\",false,true)' | grep nevents" % (d_parsed["unmerged_dir"], ifile))
    events = int(macroData.split("=")[-1])
    status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%sntuple_%i.root\",true,true)' | grep nevents" % (d_parsed["unmerged_dir"], ifile))
    effevents = int(macroData.split("=")[-1])
    tot_events += events
    tot_effevents += effevents

files_to_merge = [d_parsed["unmerged_dir"] + "ntuple_%i.root" % ifile for ifile in d_parsed["merged_constituents"]]

mergelist_fname = shortname+"/mergeLists/merged_list_%i.txt" % merged_file_num
metadata_fname = shortname+"/merge_metaData.txt"
with open(mergelist_fname, "w") as fh:
    fh.write("nEntries: %i\n%s" % (tot_events, "\n".join(files_to_merge)))

with open(metadata_fname, "w") as fh:
    fh.write("n: %i\neffN: %i\nk: %f\nf: %f\nx: %f\nfile: merged_list_%i.txt" \
            % (tot_events, tot_effevents, d_parsed["kfact"], d_parsed["efact"], d_parsed["xsec"], merged_file_num))

with open("%s/cfg.sh" % (shortname),"w") as f_cfg:
    f_cfg.write("export inputListDirectory=%s/mergeLists/\n" %(shortname))
    f_cfg.write("export mData=%s\n" %(metadata_fname))
    f_cfg.write("export outputDir=%s\n" %(output_dir))
    f_cfg.write("export dataSet=%s\n" %(shortname))
    f_cfg.write("export workingDirectory=%s\n" %(os.getcwd()))
    if (os.environ["SCRAM_ARCH"]=='slc6_amd64_gcc491'): 
        f_cfg.write("export executableScript=%s/libsh/mergeScriptRoot6.sh\n" %(os.getcwd()))
    else:    
        f_cfg.write("export executableScript=%s/libsh/mergeScript.sh\n" %(os.getcwd()))

