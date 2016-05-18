import sys
sys.path.insert(0, "../AutoTwopler/dashboard/")
import twiki, dis_client
import os, time, re

#############
# USER PARAMS
#############
twiki_username = "FrankGolf"
if os.getenv("USER") == "namin": twiki_username = "namin"
old_twiki = "Run2Samples25ns80X"
new_twiki = "Run2Samples25ns80XminiAODv2"
campaign_string = "RunIISpring16MiniAODv2"
new_gtag = "80X_mcRun2_asymptotic_2016_miniAODv2_v0"
new_cms3tag = "CMS3_V08-00-05"
new_assigned = "Frank"
do_xsec_check = True # MAKE SURE ALL XSEC MATCH OR ELSE VERY BAD
#################
# END USER PARAMS
#################

def print_good(text): print '\033[92m'+text+'\033[0m'
def print_warn(text): print '\033[93m'+text+'\033[0m'
def print_bad(text): print '\033[91m'+text+'\033[0m'

def get_xsec(ds):
    xsec = -1
    try: xsec = dis_client.query(ds, typ="mcm")["response"]["payload"]["cross_section"]
    except: pass
    return xsec

# OLD samples: on old twiki
# NEW samples: new campaign from DAS
# ALREADY NEW samples: samples that have already been put on the new campaign twiki

# make map from old dataset name --> old twiki line
#               old dataset name --> sample type ("QCD", "SMS", etc.)
old_raw = twiki.get_raw(username=twiki_username, page=old_twiki)
d_samples_to_type = {}
d_dataset_to_twikiline = {}
typ = None
types = []
for line in old_raw.split("\n"):
    if "---+++" in line:
        typ = line.split("---+++")[1]
        types.append(typ)
    if line.count("|") is not 13 or "Dataset*" in line: continue
    dataset = line.split("|")[1].strip()
    if typ:
        d_samples_to_type[dataset] = typ
        d_dataset_to_twikiline[dataset] = line

old_samples = twiki.get_samples(username=twiki_username, page=old_twiki, get_unmade=False, assigned_to="all")
already_new_samples = twiki.get_samples(username=twiki_username, page=new_twiki, get_unmade=False, assigned_to="all")

old_datasets = [s["dataset"] for s in old_samples if "dataset" in s]
new_datasets = dis_client.query("/*/*%s*/MINIAODSIM" % campaign_string)["response"]["payload"]
already_new_datasets = [s["dataset"] for s in already_new_samples if "dataset" in s]

# for each old, find new sample that has same content between first two slashes (the Primary Dataset) and ext number matches
# map old dataset name --> new dataset name
# map new dataset name --> old dataset name
d_old_to_new = {}
for old in old_datasets:
    matches = []
    for new in new_datasets:
        if old.split("/")[1] != new.split("/")[1]: continue

        ext = None
        match_old = re.search("_ext([0-9]{1,2})", old)
        match_new = re.search("_ext([0-9]{1,2})", new)
        if match_old:
            # if old is _ext, then make sure new matches, or else skip
            if not match_new: continue
            if match_new and match_new.group(0) != match_old.group(0): continue
        else:
            # if old is not ext, then don't match a new ext to it
            if match_new: continue


        matches.append(new)

    if len(matches) != 1: continue
    d_old_to_new[old] = matches[0]

# invert dict can't use dict comprehension in python 2.6 wtf
d_new_to_old = {}
for k,v in d_old_to_new.items(): d_new_to_old[v] = k

# for each sample type, find all new campaign samples that don't already exist on the new twiki
# and modify the old twiki line to have new quantities
for typ in types:
    samples = [d_old_to_new[old] for old in d_samples_to_type if d_samples_to_type[old] == typ and old in d_old_to_new and d_old_to_new[old] not in already_new_datasets]
    if len(samples) == 0: print_good(typ + " (%i)" % len(samples))
    else: print_warn(typ + " (%i)" % len(samples))
    for new in samples:

        twikiline = d_dataset_to_twikiline[ d_new_to_old[new] ]
        parts = twikiline.split("|")

        # | /SMS-T1bbbb_mGluino-1500_mLSP-100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM | !NoFilter | 52465 | 52465 | 0.0141903 | 1 | 1 | 80X_mcRun2_asymptotic_2016_v3 | CMS3_V08-00-01 | /hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/SMS-T1bbbb_mGluino-1500_mLSP-100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/ | Sicheng | sParms: mGluino, mLSP |

        xsec = float(parts[5])

        if do_xsec_check:
            new_xsec = get_xsec(new)
            if abs(xsec - new_xsec)/xsec > 0.03:
                print_bad( "===> OLD and NEW xsecs (%f, %f) do not match for OLD and NEW datasets %s, %s" % (xsec, new_xsec, d_new_to_old[new], new) )

                if abs(new_xsec-1) < 0.0001:
                    print_bad(" ===> New xsec is 1, so let's trust the OLD (SNT) xsec" )
                    print_bad( "===> Will use OLD xsec, but please check manually" )
                    xsec = xsec
                else:
                    print_bad( "===> Will use NEW xsec, but please check manually" )
                    xsec = new_xsec
                # continue

        parts[1] = new
        parts[3] = ""
        parts[4] = ""
        parts[5] = str(xsec)
        parts[8] = new_gtag
        parts[9] = new_cms3tag
        parts[10] = ""
        parts[11] = new_assigned
        twikiline = " | ".join(parts)
        print twikiline

    print

