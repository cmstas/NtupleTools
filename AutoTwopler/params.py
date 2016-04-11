DO_TEST = False # if True, put the final samples in a  /snt/test/ dir so we don't screw anything up
DO_SKIP_TAIL = False # if True, skip crab jobs that are taking too long
dashboard_name = "AutoTwopler"
log_file = "duck.log"

pset_data = "DataProduction2015_NoFilter_PAT_cfg.py"
pset_mc_fastsim = "MCProduction2015_FastSim_NoFilter_cfg.py"
pset_mc = "MCProduction2015_NoFilter_cfg.py"

campaign = "74X"

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

