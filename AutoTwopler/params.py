### CMS3
DO_TEST = False # if True, put the final samples in a  /snt/test/ dir so we don't screw anything up
DO_SKIP_TAIL = True # if True, skip crab jobs that are taking too long (doesn't apply to babies)
EMAIL_WHEN_DONE = False # if True, send email to user when each sample finishes (must have email in git config)
FORSAKE_HEAVENLY_PROXY = False # if True, don't use autorenewable proxy, so user may need to put in password every now and then (hopefully not often...this is a backup option)
dashboard_name = "AutoTwopler"
log_file = "duck.log"

### BABIES
open_datasets = False # for babies, if we want the AutoTwopler to keep pulling in new merged files as they show up in hadoop, set this to True (this is useful for data, but if you're forcing a golden json, then there's kind of no point in setting this to True)
sweepRoot_scripts = [] # put absolute path to a bash script which takes 1 argument (filename) and has "exit 0" for good file, and something else for bad files. leaving this as an empty list will skip sweepRooting. Since it is a list, you can put multiple files to copy over for use with sweepRooting (first element must still be the executable)
merging_scripts = [] # same story as above for merging_scripts
baby_merged_dir = "/nfs-7/userdata/${USER}/tupler_babies/merged/" # where final merged babies will end up - useless if merging is disabled (empty list above)

### ANALYSIS-SPECIFIC STUFF FOR BABIES
# Prepare a file like same_sign.py in this directory with parameters that override the above defaults (default = no merging, no sweeprooting)
# Note that you *must* provide a function called dataset_to_shortname that has below example behaviour (takes one dataset as a string and returns a shortname as a string for the merged ntuple's name)
# Well, you could just put all these new parameters and functions in here, but it's probably better to factor out analysis-specific stuff and just do a single `from blah import *`
"""
def dataset_to_shortname(ds):
    if ds=="/TestCMS3/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAOD":
        return "Test_ntuple"
sweepRoot_scripts = ["/home/users/namin/duck_80x/NtupleTools/AutoTwopler/baby_devel/sweepRoot.sh", "/home/users/namin/duck_80x/NtupleTools/AutoTwopler/baby_devel/sweepRoot.C"]
merging_scripts = ["/home/users/namin/duck_80x/NtupleTools/AutoTwopler/baby_devel/mergeScript.sh", "/home/users/namin/duck_80x/NtupleTools/AutoTwopler/baby_devel/mergeHadoopFiles.C"]
baby_merged_dir = "/nfs-7/userdata/${USER}/tupler_babies/merged/"
"""
try: from same_sign import *
except: pass

### CMS3
pset_data = "DataProduction2015_NoFilter_PAT_cfg.py"
pset_mc_fastsim = "MCProduction2015_FastSim_NoFilter_cfg.py"
pset_mc = "MCProduction2015_NoFilter_cfg.py"


campaign = "80X"

if campaign == "74X":
    scram_arch="slc6_amd64_gcc491"
    cms3tag="CMS3_V07-04-12"
    cmssw_ver="CMSSW_7_4_14"
    jecs = "Summer15_25nsV5_MC.db"

elif campaign == "76X":
    scram_arch="slc6_amd64_gcc493"
    cms3tag="CMS3_V07-06-03_MC"
    cmssw_ver="CMSSW_7_6_3"
    jecs = "Summer15_25nsV5_MC.db"

elif campaign == "80X":
    scram_arch="slc6_amd64_gcc493"
    cms3tag="CMS3_V08-00-01"
    cmssw_ver="CMSSW_8_0_3_patch1"
    jecs = 'Spring16_25nsV1_MC.db'

elif campaign == "80X_miniaodv2":
    scram_arch="slc6_amd64_gcc530"
    cms3tag="CMS3_V08-00-05"
    cmssw_ver="CMSSW_8_0_5_patch1"
    jecs = 'Spring16_25nsV1_MC.db'

elif campaign == "80X_stopBabyMaker":
    scram_arch="slc6_amd64_gcc530"
    cms3tag="CMS3_V08-00-05"
    cmssw_ver="CMSSW_8_0_5"
    jecs = 'Spring16_25nsV1_MC.db'

