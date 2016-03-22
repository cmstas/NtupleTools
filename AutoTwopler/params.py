# DO_TEST = True # if True, put the final samples in a  /snt/test/ dir so we don't screw anything up
# dashboard_name = "AutoTwopler_debug"
DO_TEST = False # if True, put the final samples in a  /snt/test/ dir so we don't screw anything up
dashboard_name = "AutoTwopler"
log_file = "duck.log"
pset_data = "DataProduction2015_NoFilter_PAT_cfg.py"
pset_mc_fastsim = "MCProduction2015_FastSim_NoFilter_cfg.py"
pset_mc = "MCProduction2015_NoFilter_cfg.py"

jecs = ['Summer15_25nsV5_MC.db']

campaign = "76X"

if campaign == "74X":
    scram_arch="slc6_amd64_gcc491"
    cms3tag="CMS3_V07-04-12"
    cmssw_ver="CMSSW_7_4_14"

elif campaign == "76X":
    scram_arch="slc6_amd64_gcc493"
    cms3tag="CMS3_V07-06-03_MC"
    cmssw_ver="CMSSW_7_6_3"

