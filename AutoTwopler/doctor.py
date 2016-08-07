import os, sys

try:
    from WMCore.Configuration import Configuration
    from CRABAPI.RawCommand import crabCommand
    from CRABClient.UserUtilities import setConsoleLogLevel
    from CRABClient.ClientUtilities import LOGLEVEL_MUTE
    # I recommend putting `Root.ErrorIgnoreLevel: Error` in your .rootrc file
    from ROOT import TFile, TH1F, TChain
except:
    print ">>> Make sure to source setup.sh first!"
    sys.exit()

import params
import utils as u
import scripts.dis_client as dis

BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ENDC = '\033[0m'

# setConsoleLogLevel(LOGLEVEL_MUTE)

# Pick the proxy
proxy_file_dict = {}
if not params.FORSAKE_HEAVENLY_PROXY: proxy_file_dict = {"proxy": u.get_proxy_file()}
else: print ">>> You have chosen to forsake your heavenly proxy. Be wary of prompts for your password."

# Check write permissions
print BLUE,"Checking write permissions to UCSD...",ENDC
out = crabCommand('checkwrite', site="T2_US_UCSD", **proxy_file_dict)
print "Done. Status: %s" % out["status"]
print


# Take first dataset name in instructions.txt
print BLUE, "Taking the first sample in instructions.txt. If it's not a FullSim MC sample, then you're going to have a bad time!", ENDC
sample = u.read_samples()[0]
dataset_name = sample["dataset"]
gtag = sample["gtag"]
print "  --> %s" % dataset_name
print


# Find the smallest MINIAOD file
filelist = dis.query(dataset_name, detail=True, typ="files")
filelist = filelist["response"]["payload"]
filelist = sorted(filelist, key=lambda x: x.get("sizeGB", 999.0))
smallest_filename = filelist[0]["name"]
print BLUE, "Smallest file", ENDC
print "  --> %s" % smallest_filename
print 


# Use xrootd to get that file
ntuple_name = "input.root"
print BLUE, "Using xrootd to download the file", ENDC
os.system("xrdcp -f root://xrootd.unl.edu/%s %s" % (smallest_filename, ntuple_name))
if os.path.isfile(ntuple_name): print "Success!"
else: print "ERROR: failed to download using xrootd"
print


# Make pset for this file
pset_in_fname = params.cmssw_ver+"/src/CMS3/NtupleMaker/test/MCProduction2015_NoFilter_cfg.py"
pset_out_fname = "test_pset.py"
newlines = []
with open(pset_in_fname, "r") as fhin:
    lines = fhin.readlines()
    for iline, line in enumerate(lines):
        if line.strip().startswith("fileName") and "process.out" in lines[iline-1]: line = line.split("(")[0]+"('ntuple.root'),\n"
        elif ".GlobalTag." in line: line = line.split("=")[0]+" = '"+gtag+"'\n"
        elif "maxEvents" in line: line = line.split("int32(")[0] + "int32(" + str(100) + ") )\n"
        elif "noEventSort" in line: newlines.append("process.source.fileNames=cms.untracked.vstring(\"file:%s\")\n" % ntuple_name)
        newlines.append(line)
with open(pset_out_fname, "w") as fhout:
    fhout.write( "".join(newlines) )

# Now do cmsRun
print BLUE,"Now doing cmsRun",ENDC
os.system("cmsRun %s" % pset_out_fname)
print


# If output file exists, then check some basic branches
if os.path.isfile("ntuple.root"): 
    print BLUE,"Found output file, so assuming CMS3 succeeded. Now checking some branches.",ENDC
    f = TFile.Open("ntuple.root","READ")
    tree = f.Get("Events")

    def get_mean_of_branch(branch):
        h = TH1F("h"+branch, "h"+branch, 100, -1000, 1000);
        tree.Draw("{0} >> h{0}".format(branch), "", "goff")
        avg = h.GetMean()
        return avg

    for branch in ["evt_pfmet", "hlt_trigNames", "mus_mcidx", "evt_nvtxs"]:
        mean = get_mean_of_branch(branch)
        print "  --> avg %s: %.2f" % (branch, mean)

    f.Close()
