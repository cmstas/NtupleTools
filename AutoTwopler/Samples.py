import os, sys, glob, select
import datetime, tarfile, pprint
import time
import pickle, json, logging
import multiprocessing
import re, copy
import random

try:
    from WMCore.Configuration import Configuration
    from CRABAPI.RawCommand import crabCommand
    from CRABClient.UserUtilities import setConsoleLogLevel, getUsernameFromSiteDB
    from CRABClient.ClientUtilities import LOGLEVEL_MUTE
    # I recommend putting `Root.ErrorIgnoreLevel: Error` in your .rootrc file
    from ROOT import TFile, TH1F, TChain
except:
    print ">>> Make sure to source setup.sh first!"
    sys.exit()

import utils as u
import scripts.dis_client as dis


# FAKE_BABY_NJOBS = 5 # how many fake merged ntuples
# FAKE_BABY_MERGED = True # fake the initial merged ntuples to run over
# FAKE_BABY_SUBMIT = True # fake submission and checking on condor to see if they're running
# FAKE_BABY_MAKEPERITERATION = 5 # every run.py iteration, how many babies should we "make" by putting fake files into the output directory?

FAKE_BABY_NJOBS = 0 # how many fake merged ntuples
FAKE_BABY_MERGED = False # fake the initial merged ntuples to run over
FAKE_BABY_SUBMIT = False # fake submission and checking on condor to see if they're running
FAKE_BABY_MAKEPERITERATION = 0 # every run.py iteration, how many babies should we "make" by putting fake files into the output directory?

class Sample:

    def __init__(self, type="CMS3", dataset=None, gtag=None,  \
            kfact=None, efact=None,xsec=None,  \
            executable=None,package=None,analysis=None,baby_tag=None, \
            sparms=[], extra={}, params=None):

        self.params = params
        if not params: 
            self.params = __import__('params')

        setConsoleLogLevel(LOGLEVEL_MUTE)

        self.specialdir_test = self.params.DO_TEST
        self.do_skip_tail = self.params.DO_SKIP_TAIL

        self.do_filelist = "filelist" in extra
        self.extra = extra

        self.baby = type == "BABY"

        if self.params.DO_TEST: print ">>> You have specified DO_TEST, so final samples will end up in snt/test/!"

        self.proxy_file_dict = {}
        if not self.params.FORSAKE_HEAVENLY_PROXY:
            self.proxy_file_dict = {"proxy": u.get_proxy_file()}
        else:
            print ">>> You have chosen to forsake your heavenly proxy. Be wary of prompts for your password."


        self.misc = {}
        self.misc["pfx_pset"] = 'pset' # where to hold the psets
        self.misc["pfx_crab"] = 'crab' # where to keep all crab tasks
        self.misc["pfx_babies"] = 'babies' # where to keep all babymaking misc files
        self.misc["crab_config"] = None
        self.misc["handled_more_than_1k"] = False
        self.misc["rootfiles"] = []
        self.misc["logfiles"] = []
        self.misc["last_saved"] = None # when was the last time we backed up this sample data
        self.misc["can_skip_tail"] = False
        self.misc["email_when_done"] = self.params.EMAIL_WHEN_DONE
        self.misc["update_dis_when_done"] = True

        self.sample = {
                "type": type,
                "basedir" : os.getcwd()+"/",
                "dataset" : dataset,
                "shortname": u.get_shortname_from_dataset(dataset),
                "user" : u.get_hadoop_name(),
                "cms3tag" : self.params.cms3tag,
                "cmsswver" : self.params.cmssw_ver,
                "gtag" : gtag,
                "kfact" : kfact,
                "efact" : efact,
                "xsec" : xsec,
                "sparms": sparms, # always keep as list. e.g., ["mlsp","mstop"]
                "isdata": False, # by default, MC
                "pset": "", # *_cfg.py pset location
                "specialdir": "", # /hadoop/cms/store/group/snt/{specialdir}/ (e.g., run2_25ns, run2_fastsim)
                "finaldir": "", # where final files will live
                "status" : "new", # general sample status
                "crab": { }, # crab task information here
                "postprocessing": { }, # postprocessing counts for monitor
                "checks": { }, # checkCMS3 info for monitor
                "ijob_to_miniaod": { }, # map from ijob to list of miniaod
                "imerged_to_ijob": { }, # map from imerged to iunmerged
                "ijob_to_nevents": { }, # map from ijob to (nevents, nevents_eff)
                "nevents_DAS": 0,
                "nevents_unmerged": 0,
                "nevents_merged": 0,
                }

        self.sample["crab"]["requestname"] = self.sample["shortname"][:99] # damn crab has size limit for name
        if self.baby:
            self.pfx = "BABY_"+self.sample["shortname"][:20] + "..."
        else:
            self.pfx = self.sample["shortname"][:25] + "..."

        # since extensions are at the end of the dataset name, the [:99] crab limit will give us duplicate requestnames
        # so tack on _ext[0-9]{1,2} at the end of the crab requestname for distinction
        ext = None
        match = re.search("_ext([0-9]{1,2})", self.sample["dataset"])
        if match: ext = match.group(0)
        if ext: self.sample["crab"]["requestname"] = "%s%s" % (self.sample["crab"]["requestname"][:-6], str(ext))

        self.sample["crab"]["outputdir"] = None
        self.sample["crab"]["taskdir"] = self.misc["pfx_crab"]+"/crab_"+self.sample["crab"]["requestname"]
        self.sample["crab"]["datetime"] = None # "160220_151313" from crab request name
        self.sample["crab"]["resubmissions"] = 0 # number of times we've "successfully" resubmitted a crab job
        self.sample["crab"]["jobs_left"] = [] # keep track of job ids that are not done
        self.sample["crab"]["jobs_left_tail"] = [] # keep track of job ids that are taking forever (in the tail)

        # HANDLE BABY STUFF
        if self.baby:
            self.sample["type"] = "BABY"
            self.sample["baby"] = {
                "cmsswver" : self.params.cmssw_ver,
                "scramarch" : self.params.scram_arch,
                "baby_tag": baby_tag,
                "analysis": analysis,
                "dataset" : dataset,
                "user_package": package,
                "user_executable": executable,
                "outputdir_pattern": "/hadoop/cms/store/user/%s/AutoTwopler_babies/${ANALYSIS}_${BABY_TAG}/${SHORTNAME}/" % os.getenv("USER"),
                "have_set_inputs": False,
                "package": "%s/%s/package.tar.gz" % (self.sample["basedir"], self.misc["pfx_babies"]),
                "executable_script": "%s/%s/baby_ducks.sh" % (self.sample["basedir"], self.misc["pfx_babies"]),
                "sweepRoot_script": None,
                "merging_script": None,
                "input_filenames": [],
                "imerged": [],
                "exe_args": [],
                "imerged_swept": [], # indices of jobs that we have sweepRooted
                "running": 0,
                "idle": 0,
                "condor_done": 0,
                "sweepRooted": 0,
            }
            self.sample["crab"]["taskdir"] = self.misc["pfx_babies"]+"/babies_"+self.sample["crab"]["requestname"]+"_"+self.sample["baby"]["baby_tag"]
            self.sample["baby"]["finaldir"] = self.sample["baby"]["outputdir_pattern"].replace("${ANALYSIS}", analysis).replace("${BABY_TAG}", baby_tag).replace("${SHORTNAME}", self.sample["shortname"]) 


        self.logger = logging.getLogger(self.params.log_file.replace(".","_"))

        self.crab_status_res = { }


        if not self.baby:
            self.set_sample_specifics()

        self.load() # load backup of this sample when we instantiate it


    def __getitem__(self, i):
        return self.sample[i]


    def __setitem__(self, k, v):
        self.sample[k] = v
    

    def __eq__(self, other):
        if self.sample["type"] == "CMS3":
            return "dataset" in other and other["dataset"] == self.sample["dataset"] \
               and "type" in other and other["type"] == self.sample["type"]
        else:
            return "dataset" in other and other["dataset"] == self.sample["dataset"] \
               and "type" in other and other["type"] == self.sample["type"] \
               and "baby" in self.sample and "baby_tag" in self.sample["baby"] \
               and other["baby_tag"] == self.sample["baby"]["baby_tag"]


    def __str__(self):
        buff  = "[%s] %s: %s\n" % (self.pfx, self.sample["status"], self.sample["dataset"])
        buff += "[%s]   cms3tag, gtag = %s, %s\n" % (self.pfx, self.sample["cms3tag"], self.sample["gtag"])
        buff += "[%s]   xsec, kfactor, eff = %.4f, %.2f, %.2f\n" % (self.pfx, self.sample["xsec"], self.sample["kfact"], self.sample["efact"])
        buff += "[%s]   shortname = %s\n" % (self.pfx, self.sample["shortname"])
        buff += "[%s]   requestname = %s\n" % (self.pfx, self.sample["crab"]["requestname"])
        buff += "[%s]   pset = %s\n" % (self.pfx, self.sample["pset"])

        if "status" in self.sample["crab"]:
            buff += "[%s]   CRAB status %s for %i jobs\n" \
                    % (self.pfx, self.sample["crab"]["status"], self.sample["crab"]["njobs"])
            buff += "[%s]   Output dir: %s\n" % (self.pfx, self.sample["crab"]["outputdir"])
            for cstat, num in self.sample["crab"]["breakdown"].items():
                if num == 0: continue
                buff += "[%s]     %s: %i\n" % (self.pfx, cstat, num)
        return buff


    def get_slimmed_dict(self):
        new_dict = copy.deepcopy(self.sample)
        del new_dict["imerged_to_ijob"]
        del new_dict["ijob_to_miniaod"]
        del new_dict["ijob_to_nevents"]
        if "jobs_left" in new_dict["crab"]:
            del new_dict["crab"]["jobs_left"]

        if self.sample["type"] == "BABY":
            for key in ["xsec", "specialdir", "sparms", "pset", "postprocessing", "nevents_unmerged", "nevents_merged", "nevents_DAS", \
                        "kfact", "isdata", "gtag", "finaldir", "efact", "cmsswver", "cms3tag", "checks", "output_names_nosplit"]:
                if key in new_dict: del new_dict[key]
            for key in ["datetime", "jobs_left_tail", "outputdir", "resubmissions"]:
                if key in new_dict["crab"]: del new_dict["crab"][key]
            for key in ["have_set_inputs", "imerged", "imerged_swept", "input_filenames", "outputdir_pattern"]:
                if key in new_dict["baby"]: del new_dict["baby"][key]
        return new_dict


    def get_status(self):
        return self.sample["status"]


    def get_type(self):
        return self.sample["type"]


    def set_status(self, status):
        self.sample["status"] = status


    def do_log(self, text, typ='info'):
        # toprint = "[%s] [%s] %s" % (datetime.datetime.now().strftime("%H:%M:%S"), self.pfx, text)
        toprint = "[%s] %s" % (self.pfx, text)
        # print toprint
        if typ == 'info':
            self.logger.info(toprint)
        elif typ == 'debug':
            self.logger.debug(toprint)


    def save(self):
        backup_file = self.sample["crab"]["taskdir"]+"/backup.pkl"
        try:
            self.misc["last_saved"] = u.get_timestamp()
            d_tot = {"sample": self.sample, "misc": self.misc}
            with open(backup_file,"w") as fhout:
                pickle.dump(d_tot, fhout)
        except:
            self.do_log("couldn't save %s" % backup_file)
       # self.do_log("successfully backed up to %s" % backup_file)
        # self.do_log("successfully backed up")

    def load(self):
        backup_file = self.sample["crab"]["taskdir"]+"/backup.pkl"
        if os.path.isfile(backup_file):
            with open(backup_file,"r") as fhin:
                d_tot = pickle.load(fhin)

            for key in d_tot["sample"].keys():

                # if one of these values is already in the init'ed sample, then don't
                # take the old value from the loaded dict. also should parallel update_params()
                if key in ["xsec", "kfact", "efact", "sparms"] and key in d_tot["sample"]:
                    if d_tot["sample"][key] and not(d_tot["sample"][key] == self.sample[key]):
                        self.do_log("found a new value of %s: %s. (old value: %s). updating." \
                                % (key, self.sample[key], d_tot["sample"][key]) )
                        continue

                self.sample[key] = d_tot["sample"][key]

            for key in d_tot["misc"].keys(): self.misc[key] = d_tot["misc"][key]
            last_saved = self.misc["last_saved"]
            if last_saved:
                min_ago = round((u.get_timestamp() - last_saved) / 60.0)
                # self.do_log("successfully loaded %s which was last saved %i minutes ago" % (backup_file, min_ago))
                self.do_log("successfully loaded pickle backup (last saved %i minutes ago)" % min_ago)
            else:
                self.do_log("successfully loaded %s" % (backup_file))
        else:
            self.do_log("backup doesn't exist. you probably just put in new samples, so ignore this if so.")


    def set_sample_specifics(self):
        ds = self.sample["dataset"]

        # figure out pset automatically
        if ds.endswith("SIM"): self.sample["pset"] = self.params.pset_mc
        # if len(self.sample["sparms"]) > 0: self.sample["pset"] = self.params.pset_mc_fastsim
        if "FSPremix" in ds: self.sample["pset"] = self.params.pset_mc_fastsim
        if "FastAsympt" in ds: self.sample["pset"] = self.params.pset_mc_fastsim
        if "/Run2015" in ds: 
            self.sample["isdata"] = True
            self.sample["pset"] = self.params.pset_data
        if "/Run2016" in ds:
            self.sample["isdata"] = True
            self.sample["pset"] = self.params.pset_data
        if self.sample["isdata"]: 
            self.do_log("recognized that this is data")
            self.do_skip_tail = False
            self.sample["pset"] = self.params.pset_data

        # figure out specialdir automatically
        if "50ns" in ds: self.sample["specialdir"] = "run2_50ns"
        elif "RunIISpring15MiniAODv2-FastAsympt25ns" in ds:
            self.sample["pset"] = self.params.pset_mc_fastsim
            self.sample["specialdir"] = "run2_fastsim"
        elif "Spring16Fast" in ds:
            self.sample["pset"] = self.params.pset_mc_fastsim
            # self.sample["specialdir"] = "run2_25ns_80MiniAODv2_fastsim"
            self.sample["specialdir"] = "run2_moriond17_fastsim"
        elif "RunIISpring15FSPremix" in ds:
            self.sample["pset"] = self.params.pset_mc_fastsim
            self.sample["specialdir"] = "run2_fastsim"
        elif "Private74X" in ds:
            self.sample["pset"] = self.params.pset_mc_fastsim
            self.sample["specialdir"] = "run2_fastsim_private"
        elif "T2ttZH_" in ds or "T5qqqqWH_" in ds or "Private80Xv2" in ds:
            self.sample["pset"] = self.params.pset_mc
            self.sample["specialdir"] = "run2_25ns_80Private"

        elif "RunIISpring15MiniAODv2" in ds: self.sample["specialdir"] = "run2_25ns_MiniAODv2"
        elif "RunIISpring16MiniAODv1" in ds: self.sample["specialdir"] = "run2_25ns_80MiniAODv1"
        elif "RunIISpring16MiniAODv2" in ds:
            self.sample["pset"] = self.params.pset_mc
            self.sample["specialdir"] = "run2_25ns_80MiniAODv2"
        elif "RunIISummer16MiniAODv2" in ds:
            self.sample["pset"] = self.params.pset_mc
            self.sample["specialdir"] = "run2_moriond17"
        elif "25ns" in ds: self.sample["specialdir"] = "run2_25ns"
        elif self.sample["isdata"]: self.sample["specialdir"] = "run2_data_test"
        else:
            self.do_log("can't match patterns in dataset name to figure out where in .../snt/ to put it. using /snt/run2/. move it later")
            self.sample["specialdir"] = "run2"


        if "76X_mcRun2_" in ds: self.sample["specialdir"] = "run2_25ns_76MiniAODv2"
        if "SSDL2016" in ds:
            self.sample["pset"] = self.params.pset_mc
            self.sample["specialdir"] = "run2_ss_synch"

        if self.specialdir_test or "/Good" in ds:
            self.do_log("I think this is for a corrupted file, so will put in snt/test!")
            self.sample["specialdir"] = "test"
            self.sample["pset"] = self.params.pset_mc

        self.sample["finaldir"] = "/hadoop/cms/store/group/snt/%s/%s/%s/" \
                % (self.sample["specialdir"], self.sample["shortname"], self.sample["cms3tag"].split("_",1)[1])


    def set_baby_inputs(self):


        if self.sample["baby"]["have_set_inputs"]: return

        if not os.path.isdir(self.misc["pfx_babies"]): os.makedirs(self.misc["pfx_babies"])
        taskdir = self.sample["crab"]["taskdir"]
        if not os.path.isdir(taskdir): os.makedirs(taskdir)

        user_executable = self.sample["baby"]["user_executable"]
        user_package = self.sample["baby"]["user_package"]

        # extra_vals at the moment will be a list of the arguments given after the first 6 things for babies in instructions.txt
        extra_vals = self.extra or []
        nevts, output_names, exe_args = -1, "output.root", ""
        if len(extra_vals) == 2:
            nevts, output_names = extra_vals
        if len(extra_vals) == 3:
            nevts, output_names, exe_args = extra_vals
            
        self.sample["baby"]["output_names_nosplit"] = output_names
        self.sample["baby"]["exe_args_nosplit"] = exe_args

        nevts = int(nevts)
        output_names = output_names.split(",")
        exe_args = exe_args.split(",")
           
        self.sample["baby"]["nevents"] = nevts
        self.sample["baby"]["output_names"] = output_names
        self.sample["baby"]["exe_args"] = exe_args

        # copy the user package and move it here. at this point, we can just force the name to be package.tar.gz 
        # since the user isn't the one extracting it on the WN
        u.cmd( "cp %s %s/%s/package.tar.gz" % (user_package, self.sample["basedir"], self.misc["pfx_babies"]) )
        u.cmd( "cp %s %s/%s/executable.sh" % (user_executable, self.sample["basedir"], self.misc["pfx_babies"]) )

        # if the user specified a sweepRoot file in the params.py, then copy it over
        if(len(self.params.sweepRoot_scripts) > 0) and os.path.isfile(self.params.sweepRoot_scripts[0]): 
            new_dir = "%s/%s/" % (self.sample["basedir"], self.misc["pfx_babies"])
            self.sample["baby"]["sweepRoot_script"] = new_dir+"sweepRoot.sh"
            for fname in self.params.sweepRoot_scripts:
                u.cmd( "cp %s %s" % (fname, new_dir))
            u.cmd( "cp %s %s/sweepRoot.sh" % (self.params.sweepRoot_scripts[0], new_dir))
            u.cmd( "chmod u+x %s/sweepRoot.sh" % (new_dir))

        # if the user specified a merge script in the params.py, then copy it over
        if(len(self.params.merging_scripts) > 0) and os.path.isfile(self.params.merging_scripts[0]): 
            new_dir = "%s/%s/" % (self.sample["basedir"], self.misc["pfx_babies"])
            self.sample["baby"]["merging_script"] = new_dir+"merging_script.sh"
            for fname in self.params.merging_scripts:
                u.cmd( "cp %s %s" % (fname, new_dir))
            u.cmd( "cp %s %s/merging_script.sh" % (self.params.merging_scripts[0], new_dir))
            u.cmd( "chmod u+x %s/merging_script.sh" % (new_dir))


        # make new executable file with copy command at bottom and variables at top
        # so um, one thing. it seems like condor doesn't immediately copy the executable when you submit the job
        # thus, if we rapidfire submit jobs, they will all end up seeing the last version of the executable
        # ie, they might all output to output_N.root where N is the last imerged value. so all the variables below
        # make the submission file independent of file number (stuff like imerged is computed on the fly)
        copy_cmds = []
        for output_name in output_names:
            output_name_noext = output_name.rsplit(".",1)[0]
            copy_cmd = "gfal-copy -p -f -t 4200 file://`pwd`/%s srm://bsrm-3.t2.ucsd.edu:8443/srm/v2/server?SFN=%s/%s/%s_${IMERGED}.root --checksum ADLER32" \
                    % (output_name, self.sample["baby"]["outputdir_pattern"], output_name_noext, output_name_noext)
            copy_cmds.append(copy_cmd)

        with open(self.sample["baby"]["executable_script"], "w") as fhout:
            fhout.write("#!/bin/bash\n\n")
            fhout.write("CMSSW_VER=$1\n")
            fhout.write("SCRAM_VER=$2\n")
            fhout.write("DATASET=$3\n")
            fhout.write("FILENAME=$4\n")
            fhout.write("ANALYSIS=$5\n")
            fhout.write("BABY_TAG=$6\n")
            fhout.write("SHORTNAME=$7\n")
            fhout.write("NEVENTS=$8\n")
            fhout.write("OUTPUT_NAMES=$9\n")
            fhout.write("EXE_ARGS=${10}\n")
            fhout.write("IMERGED=$(echo $FILENAME | sed 's/.*merged_ntuple_\\([0-9]\\+\\)\\.root/\\1/')\n\n")
            fhout.write("echo cmssw_ver: $CMSSW_VER\n")
            fhout.write("echo scram_ver: $SCRAM_VER\n")
            fhout.write("echo dataset: $DATASET\n")
            fhout.write("echo filename: $FILENAME\n")
            fhout.write("echo analysis: $ANALYSIS\n")
            fhout.write("echo baby_tag: $BABY_TAG\n")
            fhout.write("echo shortname: $SHORTNAME\n")
            fhout.write("echo nevents: $NEVENTS\n")
            fhout.write("echo output_names: $OUTPUT_NAMES\n")
            fhout.write("echo exe_args: $EXE_ARGS\n")
            fhout.write("echo imerged: $IMERGED\n")
            fhout.write("\n\n\n\n")
            fhout.write("echo Extracting package tar file\ntar -xzf package.tar.gz\n\n")
            fhout.write("echo Before executable\necho Date: $(date +%s)\nhostname\nls -l\n\n")
            fhout.write("# ----------------- BEGIN USER EXECUTABLE -----------------\n")
            with open(user_executable, "r") as fhin:
                for line in fhin:
                    fhout.write(line)
            fhout.write("\n# ----------------- END USER EXECUTABLE -----------------\n\n\n")
            fhout.write("echo After executable\necho Date: $(date +%s)\nls -l\n\n")
            for copy_cmd in copy_cmds:
                fhout.write(copy_cmd + "\n\n")
            fhout.write("echo After copy\necho Date: $(date +%s)")
        
        self.sample["baby"]["input_filenames"], self.sample["cms3tag"] = self.get_cms3_info()
        self.sample["baby"]["imerged"] = map(lambda x: int(x.split(".root")[0].split("_")[-1]), self.sample["baby"]["input_filenames"])
        self.sample["baby"]["have_set_inputs"] = True

    def update_params(self, d):
        for param in ["xsec", "kfact", "efact", "sparms"]:
            if param in d and d[param] and not(d[param] == self.sample[param]):
                self.do_log("found a new value of %s: %s. (old value: %s). updating." \
                        % (param, d[param], self.sample[param]) )
                self.sample[param] = d[param]


    def handle_action(self, action):
        # if we return True, then run.py will consume the action

        if "repostprocess" in action:
            self.do_log("found an action to repostprocess, so put status back to 'crab' and enabled skip_tail")
            consume_action = self.force_repostprocess()
            return consume_action

        elif "skip_tail" in action:
            self.do_log("found an action to skip tail crab jobs")
            consume_action = self.force_skip_tail()
            return consume_action

        elif "email_done" in action:
            self.do_log("found an action to send an email when job is complete")
            self.misc["email_when_done"] = True
            return True

        elif "baby_remerge" in action:
            self.do_log("found an action to remerge baby")
            self.set_status("condor")
            return True
        
        else:
            self.do_log("don't recognize action '%s'" % action)

        return False

    def force_repostprocess(self):
        # start workflow from "crab" step
        self.sample["status"] = "crab"

        # clear these dictionaries to regenerate them
        self.sample["ijob_to_nevents"] = {}
        self.sample["imerged_to_ijob"] = {}
        self.sample["ijob_to_miniaod"] = {}

        # delete any residual merged files
        try:
            merged_wildcard = self.sample["crab"]["outputdir"]+"/merged/merged_ntuple_*.root"
            self.do_log("removing residual merged files: %s" % merged_wildcard)
            u.cmd("rm %s" % merged_wildcard)
        except:
            pass

        running_list, running_ID_list = self.get_condor_submitted()
        if len(running_ID_list) > 0:
            self.do_log("removing residual condor jobs for merged indices: %s" % ", ".join(map(str,running_list)))
            u.cmd( "condor_rm %s" % " ".join(map(str,running_ID_list)) )

        consume_action = self.force_skip_tail()
        return consume_action

    
    def force_skip_tail(self):
        if not self.sample["status"] == "crab":
            self.do_log("you want me to skip the tail jobs, but status is '%s', not 'crab'" % self.sample["status"])
            return True

        self.crab_status(do_long=False)
        stat = self.crab_status_res
        self.sample["crab"]["jobs_left_tail"] = []
        if "jobs" in stat and "jobList" in stat:
            for status, ijob in stat["jobList"]:
                if not(status == "finished"):
                    self.sample["crab"]["jobs_left_tail"].append(ijob)

        self.do_skip_tail = True
        self.misc["can_skip_tail"] = True
        self.sample["crab"]["status"] = "COMPLETED"
        return True

    def get_cms3_info(self):
        filenames = []

        if FAKE_BABY_MERGED: 
            self.do_log("Running over %i FAKE merged ntuples" % FAKE_BABY_NJOBS)
            return ["/hadoop/cms/store/user/namin/fakedirectory/merged_ntuple_%i.root" % i for i in range(1,FAKE_BABY_NJOBS+1)]

        query_str = "%s,sample_type=CMS3 | grep cms3tag,location" % self.sample["dataset"]
        response = dis.query(query_str, typ='snt')
        finaldir = response["response"]["payload"][0]["location"]
        cms3tag = response["response"]["payload"][0]["cms3tag"]
        filenames = glob.glob("%s/merged_ntuple_*.root" % finaldir)

        return filenames, cms3tag


    def check_new_merged_for_babies(self):
        self.sample["baby"]["input_filenames"], _ = self.get_cms3_info()
        old_imerged = self.sample["baby"]["imerged"]
        new_imerged = map(lambda x: int(x.split(".root")[0].split("_")[-1]), self.sample["baby"]["input_filenames"])
        self.sample["baby"]["imerged"] = new_imerged

        n_new = len(set(new_imerged) - set(old_imerged))
        if n_new > 0:
            self.do_log("found %i more merged files, so will start running on those next" % n_new)
            self.sample["status"] = "condor"

    def make_crab_config(self):
        if self.misc["crab_config"] is not None: 
            self.do_log("crab config already made, not remaking")
            return

        config = Configuration()
        config.section_('General')
        config.General.workArea = self.misc["pfx_crab"] # all crab output goes into crab/
        config.General.transferOutputs = True
        config.General.transferLogs = True
        config.General.requestName = self.sample["crab"]["requestname"]
        config.section_('JobType')
        config.JobType.inputFiles = [self.params.jecs]
        config.JobType.pluginName = 'Analysis'
        config.JobType.psetName = "%s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"])
        config.JobType.allowUndistributedCMSSW = True
        config.section_('Data')
        config.Data.allowNonValidInputDataset = True
        config.Data.publication = False
        config.Data.inputDataset = self.sample["dataset"]
        config.Data.unitsPerJob = 1
        config.Data.ignoreLocality = True
        config.Data.splitting = 'FileBased'
        config.Data.outLFNDirBase = '/store/user/%s/AutoTwopler/' % (getUsernameFromSiteDB())
        # TODO: per https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile
        #       use outLFNDirBase to make samples go into, say "/hadoop/cms/store/user/<user>/80X/"
        #       need to propagate this change everywhere else too
        config.Data.inputDBS = "phys03" if self.sample["dataset"].endswith("/USER") else "global"
        config.section_('User')
        config.section_('Site')
        config.Site.storageSite = 'T2_US_UCSD'
        # config.Site.whitelist = ['T2_US_*']
        config.Site.whitelist = ['T2_*']

        if self.do_filelist:
            files = self.extra["filelist"]
            files_per_job = self.extra["files_per_job"]

            config.JobType.generator = 'lhe'
            del config.Data.inputDataset
            config.Data.outputPrimaryDataset = self.sample["shortname"]
            config.Data.splitting = 'FileBased'
            config.Data.unitsPerJob = self.extra["files_per_job"]
            config.Data.userInputFiles = files
            config.Data.totalUnits = len(files)



        self.misc["crab_config"] = config
    
    def make_pset(self):
        if not os.path.isdir(self.misc["pfx_pset"]): os.makedirs(self.misc["pfx_pset"])

        pset_in_fname = self.params.cmssw_ver+"/src/CMS3/NtupleMaker/test/"+self.sample["pset"]
        pset_out_fname = "%s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"])

        if os.path.isfile(pset_out_fname): 
            self.do_log("pset already made, not remaking")
            return

        if not os.path.isfile(pset_in_fname):
            self.do_log("skeleton pset %s does not exist!" % (pset_in_fname))
            return

        newlines = []
        with open(pset_in_fname, "r") as fhin:
            lines = fhin.readlines()
            newlines.append("import sys, os\n")
            newlines.append("sys.path.append(os.getenv('CMSSW_BASE')+'/src/CMS3/NtupleMaker/test')\n\n")
            for iline, line in enumerate(lines):
                if line.strip().startswith("fileName") and "process.out" in lines[iline-1]:
                    line = line.split("(")[0]+"('ntuple.root'),\n"
                elif ".GlobalTag." in line: line = line.split("=")[0]+" = '"+self.sample["gtag"]+"'\n"
                elif ".reportEvery" in line: line = line.split("=")[0]+" = 1000\n"
                elif ".eventMaker.datasetName." in line: line = line.split("(")[0]+"('%s')\n" % self.sample["dataset"]
                elif "era=" in line: line = line.split("=")[0]+" = '"+self.params.jecs.replace(".db","")+"'\n"
                elif "runOnData=" in line: line = '%s = %s\n' % (line.split("=")[0], self.sample["isdata"])
                elif ".eventMaker.isData" in line: line = "%s = cms.bool(%s)\n" % (line.split("=")[0], self.sample["isdata"])
                elif "cms.Path" in line:
                    newlines.append( "process.eventMaker.datasetName = cms.string(\"%s\")\n" % self.sample["dataset"] )
                    newlines.append( "process.eventMaker.CMS3tag = cms.string(\"%s\")\n\n" % self.sample["cms3tag"] )

                newlines.append(line)
                
            sparms = self.sample["sparms"]
            if len(sparms) > 0:
                sparms = list(set(map(lambda x: x.strip(), sparms)))
                sparms = ['"%s"' % sp for sp in sparms]
                if self.params.campaign == "80X_miniaodv2":
                    newlines.append('process.sParmMaker.sparm_inputTag       = cms.InputTag("source")\n')
                newlines.append('process.sParmMaker.vsparms = cms.untracked.vstring(' + ",".join(sparms) + ')\n')
                newlines.append('process.p.insert( -1, process.sParmMakerSequence )\n')

        with open(pset_out_fname, "w") as fhout:
            fhout.write( "".join(newlines) )
            self.do_log("made pset %s!" % (pset_out_fname))


    def crab_kill(self):
        try:
            out = crabCommand('kill', dir=self.sample["crab"]["taskdir"], **self.proxy_file_dict)
        except Exception as e:
            self.do_log("ERROR killing: "+str(e))
            return 0
        return out["status"] == "SUCCESS"


    def crab_delete_dir(self):
        self.do_log("deleting %s" % (self.sample["crab"]["taskdir"]))
        self.do_log("deleting pset: %s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"]))
        os.system("rm -rf %s" % self.sample["crab"]["taskdir"])
        os.system("rm %s/%s_cfg.py" % (self.misc["pfx_pset"], self.sample["shortname"]))

    def nuke(self):
        self.crab_kill()
        self.crab_delete_dir()

    def crab_submit(self):
        # first try to see if the job already exists naively
        if "uniquerequestname" in self.sample["crab"]:
            self.do_log("already submitted crab jobs")
            self.sample["status"] = "crab"
            return 1

        # more robust check
        crablog = "%s/crab.log" % self.sample["crab"]["taskdir"]
        if os.path.isfile(crablog):
            try:
                taskline = u.get("/bin/grep 'Success' -A 1 -m 1 %s | /bin/grep 'Task name'" % crablog)
                uniquerequestname = taskline.split("Task name:")[1].strip()
                self.sample["crab"]["uniquerequestname"] = uniquerequestname
                self.sample["crab"]["datetime"] = uniquerequestname.split(":")[0].strip()
                self.do_log("already submitted crab jobs")
                self.sample["status"] = "crab"
                return 1
            except: pass

        try: 
            self.sample["nevents_DAS"] = u.dataset_event_count(self.sample["dataset"])["nevents"]
            self.do_log("sample has %i events according to DAS/DBS" % self.sample["nevents_DAS"])
        except: pass

        try:
            if not self.misc["crab_config"]: self.make_crab_config()
            self.make_pset()
            # # out = crabCommand('submit', config = self.misc["crab_config"], **self.proxy_file_dict)
            # gotta do this BS instead of the above because stupid crab didn't fix their issue
            # https://hypernews.cern.ch/HyperNews/CMS/get/computing-tools/1191/1/1/1.html
            q = multiprocessing.Queue()
            def submit(q,config,proxy=None):
                try:
                    if not proxy:
                        out = crabCommand('submit', config=config)
                    else:
                        out = crabCommand('submit', config=config, proxy=proxy)
                    q.put(out)
                except Exception as e:
                    self.do_log("ERROR with crab submit (will try again later): "+str(e))
                    self.do_log("Deleting crab task directory so that we will submit on next iteration")
                    u.cmd("[[ -e {0} ]] && rm -rf {0}".format(self.sample["crab"]["taskdir"]))
                    q.put({})

            self.do_log("submitting jobs...")
            p = multiprocessing.Process(target=submit, args=(q, self.misc["crab_config"], self.proxy_file_dict.get("proxy",None)))
            p.start()
            p.join()
            out = q.get()

            if not out:
                return 0

            dtstr = out["uniquerequestname"].split(":")[0]
            self.sample["crab"]["uniquerequestname"] = out["uniquerequestname"]
            self.sample["crab"]["datetime"] = dtstr
            self.do_log("submitted jobs. uniquerequestname: %s" % (out["uniquerequestname"]))
            self.sample["status"] = "crab"
            return 1 # succeeded
        except Exception as e:
            self.do_log("ERROR submitting: "+str(e))
            return 0 # failed


    def crab_status(self, do_long=True):

        if self.sample["nevents_DAS"] == 0 and not self.do_filelist:
            try: 
                self.sample["nevents_DAS"] = u.dataset_event_count(self.sample["dataset"])["nevents"]
                self.do_log("sample has %i events according to DAS/DBS" % self.sample["nevents_DAS"])
            except: pass

        try:
            out = crabCommand('status', dir=self.sample["crab"]["taskdir"], long=do_long, **self.proxy_file_dict)
            if "statusFailureMsg" in out and "timed out after" in out["statusFailureMsg"]:
                self.do_log("crab status --long failed with timeout: %s" %  out["statusFailureMsg"])
                self.do_log("falling back to regular old crab status, but I thought you'd like to know at least")
                out = crabCommand('status', dir=self.sample["crab"]["taskdir"], **self.proxy_file_dict)
            self.crab_status_res = out
            return 1 # succeeded
        except Exception as e:
            self.do_log("ERROR getting status: "+str(e))
            if do_long:
                self.do_log("try executing: crab status %s  --json" % (self.sample["crab"]["taskdir"]))
            else:
                self.do_log("try executing: crab status %s"  % (self.sample["crab"]["taskdir"]))
            return 0 # failed

    def crab_resubmit(self, more_ram=False):
        try:
            if more_ram:
                # out = crabCommand('resubmit', dir=self.sample["crab"]["taskdir"], maxmemory="3000", **self.proxy_file_dict)
                out = crabCommand('resubmit', dir=self.sample["crab"]["taskdir"], maxmemory="3500", **self.proxy_file_dict)
            else:
                out = crabCommand('resubmit', dir=self.sample["crab"]["taskdir"], **self.proxy_file_dict)
            return out["status"] == "SUCCESS"
        except Exception as e:
            self.do_log("ERROR resubmitting "+str(e))
            return 0 # failed

    def minutes_since_crab_submit(self):
        # minutes since the crab task was created
        dtstr = self.sample["crab"]["datetime"]
        then = datetime.datetime.strptime(dtstr, "%y%m%d_%H%M%S")
        now = datetime.datetime.utcnow() # crab datestr uses GMT, so must use utcnow()
        return (now-then).seconds / 60.0

    def crab_parse_status(self):
        if self.misc["can_skip_tail"]: return

        self.crab_status()
        stat = self.crab_status_res

        try:
            # if the status from crab exists, then set the old status to it
            if stat.get("status",None):
                self.sample["crab"]["status"] = stat.get("status")
            self.sample["crab"]["task_failure"] = stat.get("taskFailureMsg")
            self.sample["crab"]["task_warning"] = stat.get("taskWarningMsg")
            self.sample["crab"]["status_failure"] = stat.get("statusFailureMsg")
            self.sample["crab"]["commonerror"] = None
            self.sample["crab"]["time"] = u.get_timestamp()
            self.sample["crab"]["njobs"] = len(stat["jobList"])
            self.sample["crab"]["breakdown"] = {
                "unsubmitted": 0, "idle": 0, "running": 0, "failed": 0,
                "transferring": 0, "transferred": 0, "cooloff": 0, "finished": 0,
            }
        except Exception as e:
            # must be the case that not all this info exists because it was recently submitted
            self.do_log("can't get status right now (is probably too new or crab sucks): "+str(e))
            return

        # population of each status (running, failed, etc.)
        if "jobsPerStatus" in stat:
            for status,jobs in stat["jobsPerStatus"].items():
                self.sample["crab"]["breakdown"][status] = jobs

            # override crab status by figuring it out ourselves (don't underestimate crab's stupidity)
            if self.sample["crab"]["status"] == "FAILED" and ("running" in self.sample["crab"]["breakdown"]) and (self.sample["crab"]["breakdown"]["running"] > 0):
                # if "FAILED" but stuff still running, not really failed, eh?
                self.sample["crab"]["status"] = "SUBMITTED"
            if "finished" in self.sample["crab"]["breakdown"] and (self.sample["crab"]["breakdown"]["finished"] == self.sample["crab"]["njobs"]) and (self.sample["crab"]["njobs"] > 0):
                # if all finished, then complete, right?
                self.sample["crab"]["status"] = "COMPLETED"


        # if sample has outright failed, resubmit it the whole damn thing. also "RESUBMITFAILED" because crab is stupid
        if (self.sample["crab"]["status"] == "FAILED") or (self.sample["crab"]["status"] == "RESUBMITFAILED"):
            if self.crab_resubmit():
                self.sample["crab"]["resubmissions"] += 1


        if self.sample["crab"]["status"] == "SUBMITTED" and "taskWarningMsg" in stat:
            warning = stat["taskWarningMsg"]
            if len(warning) > 0 and "not yet bootstrapped" in warning[0]:
                mins = self.minutes_since_crab_submit()
                self.do_log("task has not bootstrapped yet, and it's been %i minutes" % mins)
                if mins > 120: # resubmit if been more than 2 hours
                    self.do_log("been more than 2 hours, so trying to resubmit")
                    self.crab_resubmit()

        if self.sample["crab"]["breakdown"]["finished"] > 0:
            done_frac = 1.0*self.sample["crab"]["breakdown"]["finished"]/self.sample["crab"]["njobs"]
        else: 
            done_frac = 0.0

        self.sample["crab"]["jobs_left"] = []
        self.sample["crab"]["jobs_left_tail"] = []
        if "jobs" in stat and "jobList" in stat:
            for status, ijob in stat["jobList"]:
                if not(status == "finished"):
                    # now look up job in the "jobs" dictionary. example of job_info below:
                    # {'Retries': 5, 'WallDurations': [3367.0, 13520.0, 15821.0, 8811.0, 10528.0, 1040.0], 'StartTimes':
                    # [1457345940.0, 1457381466.0, 1457396962.0, 1457415108.0, 1457436816.0, 1457449310.0], 'SubmitTimes':
                    # [1457345368.0, 1457381344.0, 1457396763.0, 1457414693.0, 1457436538.0, 1457448807.0], 'JobIds': ['7158216.0',
                    # '7188315.0', '7190921.0', '7198820.0', '7209773.0', '7217653.0'], 'EndTimes': [1457349306.0, 1457394751.0,
                    # 1457412626.0, 1457423672.0, 1457446619.0], 'Restarts': 0, 'RecordedSite': True, 'State': 'running',
                    # 'ResidentSetSize': [1207312, 1239416, 1258848, 1195008, 1254328, 1267400], 'TotalUserCpuTimeHistory': [2500,
                    # 12158, 14706, 7816, 8965, 1014.0], 'SiteHistory': ['T2_US_Purdue', 'T2_US_Vanderbilt', 'T2_US_Florida',
                    # 'T2_US_Vanderbilt', 'T2_US_Vanderbilt', 'T2_US_Nebraska'], 'TotalSysCpuTimeHistory': [73, 168, 286, 104, 144, 23.0]}
                    job_info = stat["jobs"][str(ijob)]
                    # avg_walltime = 1.0*sum(job_info['WallDurations'])/len(job_info['WallDurations'])
                    
                    nretries = 0
                    if 'State' in job_info and 'Retries' in job_info:
                        state, nretries = job_info['State'], job_info['Retries']
                    # print ">>>> job %i (%s) has been retried %i times with an average walltime of %.1f" \
                    #         % (ijob, state, nretries, avg_walltime)
                    # print "done frac: %.1f" % done_frac

                    self.sample["crab"]["jobs_left"].append(ijob)

                    if (nretries >= 0 and done_frac > 0.90) or (nretries >= 1 and done_frac > 0.87):
                        self.sample["crab"]["jobs_left_tail"].append(ijob)

        # print self.sample["crab"]["jobs_left"]
        # print self.sample["crab"]["jobs_left_tail"]
        if self.do_skip_tail and self.sample["crab"]["jobs_left"] == self.sample["crab"]["jobs_left_tail"] and (self.sample["crab"]["breakdown"]["finished"] > 0):
            # this means that all crab jobs left are jobs in the tail, so let's ignore them and forge onwards with merging
            self.do_log("there are %i tail jobs left that we will ignore from now on" % len(self.sample["crab"]["jobs_left_tail"]))
            self.misc["can_skip_tail"] = True
            self.sample["crab"]["status"] = "COMPLETED"


        # find most common error (if exists)
        error_codes, details = [], []
        most_common_detail = "n/a"
        if "jobs" in stat:
            for job in stat["jobs"].values():
                if "Error" in job.keys():
                    error_codes.append(job["Error"][0])
                    try:
                        details.append(job["Error"][2]["details"])
                    except: 
                        if len(job["Error"]) > 2: details.append(job["Error"][1])

        
        if len(details) > 0:
            most_common_detail = max(set(details), key=details.count)

        if len(error_codes) > 0:
            most_common_error_code = max(set(error_codes), key=error_codes.count)
            count = error_codes.count(most_common_error_code)

            self.sample["crab"]["commonerror"] = "%i jobs (%.1f%%) failed with error code %s: %s" \
                    % (count, 100.0*count/self.sample["crab"]["njobs"], most_common_error_code, most_common_detail)

        do_get_more_ram = False
        if self.sample["crab"]["commonerror"] and ("excessive memory" in self.sample["crab"]["commonerror"]):
            # flip a switch to force resubmission to request more ram
            do_get_more_ram = True

        # if some jobs are in 'failed' status maybe they won't get automatically resubmitted by crab, so do it manually
        if self.sample["crab"]["status"] == "SUBMITTED":
            if "failed" in self.sample["crab"]["breakdown"] and (self.sample["crab"]["breakdown"]["failed"] > 0):
                if self.crab_resubmit(more_ram=do_get_more_ram):
                    self.do_log("found %i jobs in 'failed' status, so resubmitted those" % (self.sample["crab"]["breakdown"]["failed"]))


    def handle_more_than_1k(self):
        if self.misc["handled_more_than_1k"]: return

        output_dir = self.sample["crab"]["outputdir"]
        without_zeros = self.sample["crab"]["outputdir"].replace("/0000","/")

        for kilobatch in os.listdir(without_zeros):
            if kilobatch == "0000": continue
            u.cmd("mv {0}/{1}/*.root {0}/{2}/".format(without_zeros, kilobatch, "0000"))
            u.cmd("mv {0}/{1}/log/* {0}/{2}/log/".format(without_zeros, kilobatch, "0000"))

        self.do_log("moved files from .../*/ to .../0000/")
        self.misc["handled_more_than_1k"] = True


    def is_crab_done(self):

        primary_dataset = self.sample["dataset"].split("/")[1]
        requestname = self.sample["crab"]["requestname"]
        datetime = self.sample["crab"]["datetime"]
        if self.do_filelist:
            primary_dataset = "_".join(self.sample["dataset"].split("/")[1:3])

        self.sample["crab"]["outputdir"] = "/hadoop/cms/store/user/%s/AutoTwopler/%s/crab_%s/%s/0000/" % (self.sample["user"], primary_dataset, requestname, datetime)

        if "status" not in self.sample["crab"]: return False
        if self.sample["crab"]["status"] != "COMPLETED": return False

        self.handle_more_than_1k()

        def get_num(fname): return int(fname.split("_")[-1].split(".")[0])

        njobs = self.sample["crab"]["njobs"]
        self.misc["rootfiles"] = glob.glob(self.sample["crab"]["outputdir"] + "/*.root")
        self.misc["logfiles"] = glob.glob(self.sample["crab"]["outputdir"] + "/log/*.tar.gz")

        if self.do_skip_tail and self.misc["can_skip_tail"]:
            self.do_log("pruning the ignored tail files from rootfiles and logfiles")
            self.misc["rootfiles"] = [fname for fname in self.misc["rootfiles"] if get_num(fname) not in self.sample["crab"]["jobs_left_tail"]]
            self.misc["logfiles"] = [fname for fname in self.misc["logfiles"] if get_num(fname) not in self.sample["crab"]["jobs_left_tail"]]
            njobs -= len(self.sample["crab"]["jobs_left_tail"])

        if njobs == len(self.misc["rootfiles"]) and not(njobs <= len(self.misc["logfiles"])):
            # we have all the root files, but evidently some log files got lost. try to recover them
            # format: ntuple_1.root and cmsRun_1.log.tar.gz
            root_file_numbers = set([get_num(rfile) for rfile in self.misc["rootfiles"]])
            log_file_numbers = set([get_num(lfile) for lfile in self.misc["logfiles"]])
            log_dont_have = list(root_file_numbers - log_file_numbers)
            if len(log_dont_have) > 0:
                jobids = ",".join(map(str, log_dont_have))
                self.do_log("all rootfiles exist, but not all logfiles are there (missing %s), so recovering with crab getlog --short" % jobids)
                try: out = crabCommand('getlog', dir=self.sample["crab"]["taskdir"], short=True, jobids=jobids, **self.proxy_file_dict)
                except: pass
                textlogs = glob.glob(self.sample["crab"]["taskdir"]+"/results/job_out*.txt") 
                textlogs = [log for log in textlogs if int(log.split("job_out.")[1].split(".")[0]) in log_dont_have]
                if len(textlogs) > 0:
                    self.do_log("got %i of 'em" % len(textlogs))
                    self.misc["logfiles"].extend(textlogs)

        if njobs == len(self.misc["rootfiles"]) and njobs <= len(self.misc["logfiles"]):
            return True

        # at this point, #root files != #jobs
        # could be related to https://hypernews.cern.ch/HyperNews/CMS/get/computing-tools/1599/2.html
        if self.do_skip_tail and self.misc["can_skip_tail"]:
            # however, we do not care if we're skipping the tail anyways
            return True

        self.do_log("ERROR: crab says COMPLETED but not all files are there, even after getlog")
        self.do_log("# jobs, # root files, # log files = %i, %i, %i" % (njobs, len(self.misc["rootfiles"]), len(self.misc["logfiles"])))
        return False


    def make_miniaod_map(self, force=True):
        if self.sample["ijob_to_miniaod"] and not force: return

        self.do_log("making map from unmerged number to miniaod name")
        nlogfiles = len(self.misc["logfiles"])
        # use temp so that if any crashes happen mid-processing, self.sample["ijob_to_miniaod"] will be empty and we re-do the whole thing
        temp = {}
        for ilogfile,logfile in enumerate(self.misc["logfiles"]):
            # print logfile
            if ".tar.gz" in logfile:
                try: # WTF sometimes logfiles get corrupted and aren't gzip format anymore?!
                    with  tarfile.open(logfile, "r:gz") as tar:
                        for member in tar:
                            if "FrameworkJobReport" not in member.name: continue
                            jobnum = int(member.name.split("-")[1].split(".xml")[0])
                            fh = tar.extractfile(member)
                            lines = [line for line in fh.readlines() if "<PFN>" in line and "/store/" in line]
                            miniaod = list(set(map(lambda x: "/store/"+x.split("</PFN>")[0].split("/store/")[1].split("?",1)[0], lines)))
                            temp[jobnum] = miniaod
                            self.do_log("job %i miniaod found [found %i of %i]" % (jobnum,ilogfile+1,nlogfiles))
                            fh.close()
                            break
                except:
                    self.do_log("WTF. CRAB gave us a corrupted logfile that isn't in the gzip format?! %s" % logfile)

            elif ".txt" in logfile:
                # parse the recovered txt files if .tar.gz didn't stageout
                with open(logfile, "r") as fh:
                    # job_out.7.0.txt
                    jobnum = int(logfile.split("job_out.")[1].split(".")[0])
                    lines = [line for line in fh.readlines() if "Initiating request to open file" in line]
                    miniaod = list(set(map(lambda x: "/store/"+x.split("/store/")[1].split(".root")[0]+".root", lines)))
                    temp[jobnum] = miniaod

        self.sample["ijob_to_miniaod"] = temp

    def get_rootfile_info(self, fname):
        # returns: is bad, nevents, nevents effective, file size in GB
        f = TFile.Open(fname,"READ")
        treename = "Events"

        if not f or f.IsZombie():
            self.do_log("WARNING: %s is unopenable or zombified" % fname)
            return (True, 0, 0, 0)

        tree = f.Get(treename)
        try:
            n_entries = tree.GetEntriesFast()
        except ReferenceError as e:
            self.do_log("ERROR with rootfile: "+str(e))
            return (True, 0, 0, 0)


        if n_entries == 0:
            self.do_log("WARNING: %s has 0 entries" % fname)
            return (True, 0, 0, 0)

        pos_weight = n_entries
        isdata = self.sample["isdata"]
        isnlo = ("powheg" in self.sample["dataset"]) or ("amcatnlo" in self.sample["dataset"])
        if not isdata and isnlo:
            # self.do_log("Note: this is an NLO sample, so we're scanning genps_weight")
            pos_weight = tree.GetEntries("genps_weight>=0")
        neg_weight = n_entries - pos_weight
        n_entries_eff = pos_weight - neg_weight

        h_pfmet = TH1F("h_pfmet", "h_pfmet", 100, 0, 1000);
        tree.Draw("evt_pfmet >> h_pfmet", "", "goff",1000)
        avg_pfmet = h_pfmet.GetMean()
        if avg_pfmet < 0.01 or avg_pfmet > 10000:
            self.do_log("WARNING: %s has insane evt_pfmet value of %f" % (fname, avg_pfmet))
            return (True, 0, 0, 0)

        f.Close()
        return (False, n_entries, n_entries_eff, f.GetSize()/1.0e9)

    def check_merged_rootfile(self, fname, total_events, treename="Events", ignore_total=True):
        f = TFile.Open(fname,"READ")
        imerged = int(fname.split(".root")[0].split("_")[-1])

        if not f or f.IsZombie():
            try: f.Close()
            except: pass
            return 1, -1, "Could not open file"

        try:
            tree = f.Get(treename)
        except:
            f.Close()
            return 1, -1, "WTF No tree in file"

        n_entries = 0
        try: n_entries = tree.GetEntries()
        except: 
            self.do_log("WTF Can't do GetEntries()")
            pass

        if n_entries == 0: 
            f.Close()
            return 1, -1, "No events in file"

        scale1fb_max = abs(tree.GetMaximum("evt_scale1fb"))
        scale1fb_min = abs(tree.GetMinimum("evt_scale1fb"))

        if (scale1fb_max - scale1fb_min)/scale1fb_max > 1e-6:
            f.Close()
            return 1, n_entries, "Inconsistent scale1fb. abs(min): %f, abs(max): %f" % (scale1fb_min, scale1fb_max)

        kfactor = tree.GetMaximum("evt_kfactor")
        filteff = tree.GetMaximum("evt_filt_eff")
        xsec = tree.GetMaximum("evt_xsec_incl")
        nevents_branch = int(tree.GetMaximum("evt_nEvts"))
        nevents_eff_branch = int(tree.GetMaximum("evt_nEvts_effective"))
        recalc_scale1fb = 1000.*xsec*filteff*kfactor / nevents_eff_branch

        if nevents_branch != total_events and not ignore_total:
            f.Close()
            return 1, n_entries, "evt_nEvts (%i) differs from total merged event count (%i)" % (nevents_branch, total_events)

        if (recalc_scale1fb - scale1fb_min)/scale1fb_min > 1e-6:
            f.Close()
            return 1, n_entries, "Inconsistent scale1fb. In file: %f, Calculated: %f" % (scale1fb_min, recalc_scale1fb)

        f.Close()
        return 0, n_entries, ""

    def get_events_in_chain(self, fname_wildcard):
        nevents = 0
        try:
            ch = TChain("Events")
            ch.Add(fname_wildcard)
            nevents = ch.GetEntries()
        except: pass
        return nevents


    def make_merging_chunks(self, force=True):
        if self.sample["imerged_to_ijob"] and self.sample["nevents_unmerged"] and not force: return

        self.do_log("making map from merged index to unmerged indicies")
        group, groups = [], []
        tot_size = 0.0
        nrfiles = len(self.misc["rootfiles"])
        for irfile, rfile in enumerate(self.misc["rootfiles"]):
            is_bad, nevents, nevents_eff, file_size = self.get_rootfile_info(rfile)
            ijob = int(rfile.split("_")[-1].replace(".root",""))
            self.do_log("checked ntuple_%i.root. nevents, nevents_eff: %i, %i [checked %i of %i]" % (ijob, nevents, nevents_eff, irfile+1, nrfiles))
            self.sample["ijob_to_nevents"][ijob] = [nevents, nevents_eff]
            if is_bad:
                self.do_log("WARNING: ntuple_%i.root is bad, will skip" % (ijob))
                continue
            tot_size += file_size
            group.append(ijob)
            if tot_size >= 4.7: # in GB!
                groups.append(group)
                group = []
                tot_size = 0.0
        if len(group) > 0: groups.append(group) # finish up last group
        for igp,gp in enumerate(groups):
            self.sample["imerged_to_ijob"][igp+1] = gp

        self.sample['nevents_unmerged'] = sum([x[0] for x in self.sample['ijob_to_nevents'].values()])


    def get_condor_submitted(self, running_at_least_hours=0.0, baby_merge_jobs=False):
        # return lists of merged indices and set of condor clusterID
        macro_val = self.sample["crab"]["requestname"]
        if self.sample["type"] == "BABY":
            macro_val = "BABY_%s_%s_%s" % (self.sample["baby"]["analysis"], self.sample["baby"]["baby_tag"], self.sample["shortname"])

        if baby_merge_jobs:
            macro_val = "BABYMERGE_%s_%s_%s" % (self.sample["baby"]["analysis"], self.sample["baby"]["baby_tag"], self.sample["shortname"])

        cmd = "condor_q $USER -autoformat ClusterId GridJobStatus EnteredCurrentStatus CMD ARGS -const 'AutoTwopleRequestname==\"%s\"'" % macro_val
        output = u.get(cmd)

        merged_ids = []
        clusterIDs = []
        for line in output.split("\n"):
            if len(line.strip()) < 2: continue

            if self.sample["type"] == "BABY":
                if baby_merge_jobs:
                    # 907891.0   fgolf           1/13 21:46   0+00:00:00 I  0   0.0  merging_script.sh UNMERGED_DIR=/hadoop/cms/store/user/fgolf/AutoTwopler_babies/mt2_V00-08-14/WJetsToLNu_HT-600To800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/;OUTPUT_DIR=/nfs-7/userdata/fgolf/tupler_babies/merged//mt2/V00-08-14//output/;IMERGED=2;OUTPUT_NAME=output;SHORTNAME=WJetsToLNu_HT-600To800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1;INPUT_NAMES=output_3.root,output_4.root,output_5.root,output_6.root,output_7.root,output_8.root,output_9.root;
                    clusterID, status, entered_current_status, cmd, args = line.strip().split(" ")[:5]
                    merged_index = args.split("IMERGED=")[-1].split(";")[0]
                else:
                    clusterID, status, entered_current_status, cmd = line.strip().split(" ")[:4]
                    merged_index = int(line.split("merged_ntuple_",1)[1].split(".root")[0])
            else:
                clusterID, status, entered_current_status, cmd, unmerged_dir, _, merged_index = line.strip().split(" ")[:7]


            # if we've specified to look only at jobs which have been running for at least x hours
            if running_at_least_hours > 0.01:
                hours = 1.0*(datetime.datetime.now()-datetime.datetime.fromtimestamp(int(entered_current_status))).seconds / 3600.0
                if status == "RUNNING":
                    if hours < running_at_least_hours: continue
                else:
                    # if the job is not running, then don't consider it regardless of time
                    continue


            merged_ids.append(int(merged_index))
            clusterIDs.append(int(clusterID))


        return merged_ids, clusterIDs


    def get_merged_done(self, merged_dir=None, startswith=None, get_baby_merged=False):
        # return set of merged indices. my naming convention is unfortunate:
        # for CMS3, this means actual merged files, and for babies this means completed *unmerged* babies
        # if "get_baby_merged" is True, then this refers to actually *merged* babies
        if not merged_dir:
            if self.sample["type"] == "CMS3": merged_dir = self.sample["crab"]["outputdir"]+"/merged/"
            elif self.sample["type"] == "BABY": merged_dir = self.sample["baby"]["finaldir"]

        merged_dir = merged_dir.replace("$tag","").replace("${USER}","$USER").replace("$USER",os.getenv("USER"))
        # print "[NJA] WTF this better be a directory: ",merged_dir

        if not os.path.isdir(merged_dir): return set()

        if self.sample["type"] == "CMS3":
            files = os.listdir(merged_dir)
            files = [f for f in files if f.endswith(".root")]
            return set(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))
        elif self.sample["type"] == "BABY" and not get_baby_merged:
            output_names = self.sample["baby"]["output_names"]
            # remember that the file names are merged_dir/{output_name_noext}/{output_name_noext}_{imerged}.root
            d_output_name = {} # key is the output_name and value is a set of the done files
            for output_name in output_names:
                output_name_noext = output_name.rsplit(".",1)[0]
                try:
                    files = os.listdir(merged_dir+"/"+output_name_noext+"/")
                except:
                    files = []
                files = [f for f in files if f.endswith(".root")]
                d_output_name[output_name] = set(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))
                # print output_name, d_output_name[output_name]
            # done files are ones such that ALL output files are there, so we need to take the intersection of the sets
            done_indices = set.intersection(*d_output_name.values())
            return done_indices

        elif self.sample["type"] == "BABY" and get_baby_merged:
            # print "[NJA] Checking for merged babies in %s" % merged_dir
            files = os.listdir(merged_dir)
            # print "[NJA] Files:", files
            files = [f for f in files if f.endswith(".root")]
            files = [f for f in files if f.startswith(startswith)]
            return set(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))



    def pass_tsa_prechecks(self):
        # if we already did this sample, clearly it passes prechecks
        if self.sample["status"] == "done":
            return True

        # check is sample has already been done
        is_done = False
        final_dir = self.sample["finaldir"]
        if os.path.isdir(final_dir):
            files = [f for f in os.listdir(final_dir) if f.endswith(".root")]
            if len(files) > 0: is_done = True

        if is_done:
            self.do_log("NOTE: this sample is already in the final group area. move it to another folder if you want to remake it. skipping for now.")
            return False


        return True

    def is_merging_done(self):
        # want 0 running condor jobs and all merged files in output area
        nmerged = len(self.sample["imerged_to_ijob"].keys())
        done = len(self.get_condor_submitted()[0]) == 0 and len(self.get_merged_done()) == nmerged and nmerged > 0
        if done:
            self.sample["postprocessing"]["running"] = 0
            self.sample["postprocessing"]["idle"] = 0
            self.sample["postprocessing"]["done"] = self.sample["postprocessing"]["total"]

        return done

    def is_babymaking_done(self):
        # want 0 running condor jobs and all merged files in output area
        nmerged = len(self.sample["baby"]["imerged"])
        nswept = self.sample["baby"]["sweepRooted"]
        nmerged_done = len(self.get_merged_done())
        if FAKE_BABY_SUBMIT:
            done = (nmerged_done == nmerged) and (nmerged > 0) and (nswept == nmerged)
        else:
            done = (len(self.get_condor_submitted()[0]) == 0) and (nmerged_done == nmerged) and (nmerged > 0) and (nswept == nmerged)
        # print "done:", done
        if not done: return False

        self.sample["baby"]["running"] = 0
        self.sample["baby"]["idle"] = 0
        self.sample["baby"]["condor_done"] = self.sample["baby"]["total"]
        self.sample["baby"]["sweepRooted"] = self.sample["baby"]["total"]

        if self.params.merge_babies_on_condor:
            merged = self.do_merge_babies_condor()
            
        else:
            
            merged = self.do_merge_babies()

            if not merged: self.do_log("ERROR: This sample didn't merge successfully. Will keep trying on the next pass.")

        if merged:
            self.sample["baby"]["merged_dir"] = self.params.baby_merged_dir.replace("${USER}","$USER").replace("$USER",os.getenv("USER"))
            self.sample["baby"]["merged_dir"] += "/%s/%s/" % (self.sample["baby"]["analysis"], self.sample["baby"]["baby_tag"])
            self.do_log("This sample is now done.")

        return merged

    def do_merge_babies(self):
        script = self.sample["baby"]["merging_script"]
        if not script: return True

        def remove_ext(fname):
            return fname.rsplit(".",1)[0]

        baby = self.sample["baby"]
        output_names = map(remove_ext, baby["output_names"])

        shortname = self.sample["shortname"]
        try: shortname = self.params.dataset_to_shortname(self.sample["dataset"])
        except: pass
        if not shortname.endswith(".root"): shortname += ".root"

        extra_args = self.sample["baby"]["exe_args_nosplit"]

        # this will get passed to the bash script as the first and only argument. inside the script you can simply do "eval $1" to use the variables as they are called here
        long_ass_args_string = "OUTPUT_NAMES=%s;BABY_DIR=%s;ANALYSIS=%s;BABY_TAG=%s;DATASET=%s;SHORTNAME=%s;OUTPUT_DIR=%s;EXTRA_ARGS=%s;" \
                % (",".join(output_names),baby["finaldir"],baby["analysis"],baby["baby_tag"], \
                   self.sample["dataset"], shortname, self.params.baby_merged_dir, extra_args)

        # need to cd into the script area because if the script references local macros, the paths won't be right
        dirname = os.path.dirname(script)
        scriptname = os.path.basename(script)
        self.do_log("Starting to merge the sample")
        stat, out = u.cmd("cd %s; ./%s \"%s\" >& ../%s/merged_log.txt" % (dirname,scriptname,long_ass_args_string, self.sample["crab"]["taskdir"]), returnStatus=True)

        return stat == 0 # 0 is good, anything else is bad

    def do_merge_babies_condor(self):
        input_dir = self.sample["baby"]["finaldir"]

        baby = self.sample["baby"]

        shortname = self.sample["shortname"]
        try: shortname = self.params.dataset_to_shortname(self.sample["dataset"])
        except: pass
        if not shortname.endswith(".root"): shortname += ".root"

        output_names = map(lambda x: x.rsplit(".",1)[0], baby["output_names"])
        
        # make dictionary of baby indices to merge together (each output_name has its own dict)
        if not self.sample["imerged_to_ijob"]:
            tmp = {}
            for oname in output_names:
                tmp[oname] = {}
                self.do_log("making merging chunks for %s_*.root" % oname)
                group, groups = [], []
                tot_size = 0.0
                for irfile, rfile in enumerate(glob.glob("%s/%s/*.root" % (input_dir,oname))):
                    file_size = os.path.getsize(rfile) / 1.0e9
                    ijob = int(rfile.split("_")[-1].replace(".root",""))
                    tot_size += file_size
                    group.append(ijob)
                    if tot_size >= 2.0: # in GB!
                        groups.append(group)
                        group = []
                        tot_size = 0.0
                if len(group) > 0: groups.append(group) # finish up last group
                for igp,gp in enumerate(groups):
                    tmp[oname][igp+1] = gp

            self.sample["imerged_to_ijob"] = tmp.copy()

        # print "[NJA] About to do submit_baby_merge_jobs if self.sample['imerged_to_ijob'] is not null"
        if self.sample["imerged_to_ijob"]:
            return self.submit_baby_merge_jobs()

        return False
        

    def fake_baby_creation(self):
        output_names = self.sample["baby"]["output_names"]
        merged_dir = self.sample["baby"]["finaldir"]
        # remember that the file names are merged_dir/{output_name_noext}/{output_name_noext}_{imerged}.root
        d_output_name = {} # key is the output_name and value is a set of the done files
        for output_name in output_names:
            output_name_noext = output_name.rsplit(".",1)[0]
            try: files = os.listdir(merged_dir+"/"+output_name_noext+"/")
            except: files = []
            files = [f for f in files if f.endswith(".root")]
            d_output_name[output_name] = set(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))
            # print output_name, d_output_name[output_name]
        # done files are ones such that ALL output files are there, so we need to take the intersection of the sets
        done_indices = set.intersection(*d_output_name.values())
        notdone_indices = set(list(range(1,FAKE_BABY_NJOBS+1))) - done_indices

        indices_to_make = random.sample(notdone_indices, min(len(notdone_indices),FAKE_BABY_MAKEPERITERATION))
        for output_name in output_names:
            output_name_noext = output_name.rsplit(".",1)[0]
            thedir = merged_dir+"/"+output_name_noext
            if not os.path.isdir(thedir): os.system("mkdir -p %s" % thedir)

            for imerged in indices_to_make:
                fname = "%s/%s_%i.root" % (thedir,output_name_noext,imerged)
                os.system("touch %s" % fname)
                # self.do_log("FAKE touched the FAKE baby file %s" % fname)
                self.do_log("FAKE touched the FAKE baby file %s_%i.root" % (output_name_noext,imerged))




    def submit_merge_jobs(self):
        working_dir = self.sample["basedir"]
        shortname = self.sample["shortname"]
        unmerged_dir = self.sample["crab"]["outputdir"]
        xsec = self.sample["xsec"]
        kfactor = self.sample["kfact"]
        efactor = self.sample["efact"]

        submit_file = self.sample["crab"]["taskdir"]+"/submit.cmd"
        executable_script = working_dir+"/scripts/mergeWrapper.sh"
        merge_script = working_dir+"/scripts/mergeScript.C"
        addbranches_script = working_dir+"/scripts/addBranches.C"
        proxy_file = u.get("find /tmp/x509up_u* -user $USER").strip()
        condor_log_files = "/nfs-7/userdata/%s/tupler/%s/%s.log" % (os.getenv("USER"),shortname,datetime.datetime.now().strftime("+%Y.%m.%d-%H.%M.%S"))
        std_log_files = "/nfs-7/userdata/%s/tupler/%s/std_logs/" % (os.getenv("USER"),shortname)
        input_files = ",".join([executable_script, merge_script, addbranches_script])
        nevents_both = self.sample['ijob_to_nevents'].values()
        nevents = sum([x[0] for x in nevents_both])
        nevents_effective = sum([x[1] for x in nevents_both])

        if self.do_filelist:
            if "nevents" in self.extra and "nevents_effective" in self.extra:
                nevents = self.extra["nevents"]
                nevents_effective = self.extra["nevents_effective"]
                self.do_log("NOTE: Since it looks like you specified nevents and nevents_effective in the filelist, I will be using %i and %i respectively for the merged output" % (nevents, nevents_effective))

        try:
            if not os.path.isdir(std_log_files): os.makedirs(std_log_files)
        except:
            self.do_log("ERROR making log file directory: %s" % std_log_files)
            self.do_log("see if you can make it manually. if not, switch to another uaf.")
            raise Exception("can't make log file directory: %s" % std_log_files)

        condor_params = {
                "exe": executable_script,
                "inpfiles": input_files,
                "condorlog": condor_log_files,
                "stdlog": std_log_files,
                "proxy": proxy_file,
                "requestname": self.sample["crab"]["requestname"]
                }

        cfg_format = "universe=grid \n" \
                     "grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu \n" \
                     "+remote_DESIRED_Sites=\"T2_US_UCSD\" \n" \
                     "executable={exe} \n" \
                     "arguments={args} \n" \
                     "transfer_executable=True \n" \
                     "when_to_transfer_output = ON_EXIT \n" \
                     "transfer_input_files={inpfiles} \n" \
                     "+Owner = undefined  \n" \
                     "+AutoTwopleRequestname=\"{requestname}\" \n" \
                     "log={condorlog} \n" \
                     "output={stdlog}/1e.$(Cluster).$(Process).out \n" \
                     "error={stdlog}/1e.$(Cluster).$(Process).err \n" \
                     "notification=Never \n" \
                     "x509userproxy={proxy} \n" \
                     "should_transfer_files = yes \n" \
                     "queue \n" 

        # don't resubmit the ones that are already running or done
        imerged_set = set(self.sample['imerged_to_ijob'].keys())
        processing_list, processing_ID_list = self.get_condor_submitted()
        processing_set = set(processing_list)
        processing_ID_set = set(processing_ID_list)

        do_kill_long_running_condor = True
        if do_kill_long_running_condor:
            # only ~2% of jobs take more than 5 hours
            longrunning_list, longrunning_ID_list = self.get_condor_submitted(running_at_least_hours=4.0)
            if len(longrunning_list) > 0:
                longrunning_set = set(longrunning_list)
                longrunning_ID_set = set(longrunning_ID_list)
                self.do_log("The following merged file indices have been merging for more than 5 hours: %s" % ", ".join(map(str,longrunning_set)))
                self.do_log("Killing and resubmitting condor IDs: %s" % ", ".join(map(str,longrunning_ID_set)))

                u.cmd( "condor_rm %s" % " ".join(map(str,longrunning_ID_set)) )
                processing_set = processing_set - longrunning_set
                processing_ID_set = processing_ID_set - longrunning_ID_set

        # subtract running jobs from done. we might think they're done if they begin
        # to stageout, but they're not yet done staging out
        done_set = self.get_merged_done() - processing_set
        imerged_list = list( imerged_set - processing_set - done_set ) 

        self.sample["postprocessing"]["total"] = len(imerged_set)
        self.sample["postprocessing"]["running"] = len(processing_set)
        self.sample["postprocessing"]["done"] = len(done_set)

        if len(imerged_list) > 0:
            self.sample["status"] = "postprocessing"
            self.do_log("submitting %i merge jobs" % len(imerged_list))

        error = ""
        for imerged in imerged_list:
            input_indices=",".join(map(str,self.sample['imerged_to_ijob'][imerged]))

            input_arguments = " ".join(map(str,[unmerged_dir, input_indices, imerged, nevents, nevents_effective, xsec, kfactor, efactor]))
            condor_params["args"] = input_arguments

            cfg = cfg_format.format(**condor_params)
            with open(submit_file, "w") as fhout:
                fhout.write(cfg)

            submit_output = u.get("condor_submit %s" % submit_file)

            if " submitted " in submit_output: 
                self.do_log("job for merged_ntuple_%i.root submitted successfully" % imerged)
            else:
                self.do_log("error submitting job for merged_ntuple_%i.root" % imerged)
                error = submit_output

        self.sample["postprocessing"]["idle"] = self.sample["postprocessing"]["total"] - self.sample["postprocessing"]["running"] - self.sample["postprocessing"]["done"]

        if len(error) > 0:
            self.do_log("submit error: %s" % error)
    
    def submit_baby_merge_jobs(self):

        """
        executable will take args:
        inputdir (merge the comma separated input_files here)
        outputdir (copy them here)
        imerged 
        output_name
        shortname (name should be: <shortname>_<output_name>_<imerged>.root
                   so if we merge Wjets skim.root and we're in the first (or only
                   chunk), then the output should be named Wjets_skim_1.root)
        input_files
        """
        # print "[NJA] Inside submit_baby_merge_jobs"

        baby = self.sample["baby"]

        shortname = str(self.sample["shortname"])
        long_shortname = str(self.sample["shortname"])
        try: shortname = self.params.dataset_to_shortname(self.sample["dataset"])
        except: pass
        shortname = shortname.replace(".root","")

        output_names = map(lambda x: x.rsplit(".",1)[0], baby["output_names"])
        
        working_dir = self.sample["basedir"]
        unmerged_dir = baby["finaldir"]

        output_dir = self.params.baby_merged_dir.replace("${USER}","$USER").replace("$USER",os.getenv("USER"))
        output_dir = self.params.baby_merged_dir.replace("${SHORTNAME}","$SHORTNAME").replace("$SHORTNAME",shortname)
        output_dir += "/%s/%s/" % (self.sample["baby"]["analysis"], self.sample["baby"]["baby_tag"])

        path_fragment = "%s/%s/%s" % (self.sample["baby"]["analysis"], self.sample["baby"]["baby_tag"], shortname)
        submit_file = "%s/%s/submit_merge.cmd" % (self.sample["basedir"], self.misc["pfx_babies"])
        executable_script = baby["merging_script"]
        merge_script = [ms for ms in self.params.merging_scripts if ".sh" not in ms][0]
        proxy_file = u.get("find /tmp/x509up_u* -user $USER").strip()
        condor_log_files = "/nfs-7/userdata/%s/tupler_babies/%s/%s.log" % (os.getenv("USER"),path_fragment,datetime.datetime.now().strftime("+%Y.%m.%d-%H.%M.%S"))
        std_log_files = "/nfs-7/userdata/%s/tupler_babies/%s/std_logs/" % (os.getenv("USER"),path_fragment)
        input_files = ",".join([executable_script, merge_script])

        try:
            if not os.path.isdir(std_log_files): os.makedirs(std_log_files)
        except:
            self.do_log("ERROR making log file directory: %s" % std_log_files)
            self.do_log("see if you can make it manually. if not, switch to another uaf.")
            raise Exception("can't make log file directory: %s" % std_log_files)

        condor_params = {
                "exe": executable_script,
                "inpfiles": input_files,
                "condorlog": condor_log_files,
                "stdlog": std_log_files,
                "proxy": proxy_file,
                "requestname": "BABYMERGE_%s_%s_%s" % (self.sample["baby"]["analysis"], self.sample["baby"]["baby_tag"], long_shortname),
                }

        cfg_format = "universe=grid \n" \
                     "grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu \n" \
                     "+remote_DESIRED_Sites=\"T2_US_UCSD\" \n" \
                     "executable={exe} \n" \
                     "arguments={args} \n" \
                     "transfer_executable=True \n" \
                     "when_to_transfer_output = ON_EXIT \n" \
                     "transfer_input_files={inpfiles} \n" \
                     "+Owner = undefined  \n" \
                     "+AutoTwopleRequestname=\"{requestname}\" \n" \
                     "log={condorlog} \n" \
                     "output={stdlog}/1e.$(Cluster).$(Process).out \n" \
                     "error={stdlog}/1e.$(Cluster).$(Process).err \n" \
                     "notification=Never \n" \
                     "x509userproxy={proxy} \n" \
                     "should_transfer_files = yes \n" \
                     "queue \n" 

        self.sample["postprocessing"]["total"] = 0
        self.sample["postprocessing"]["running"] = 0
        self.sample["postprocessing"]["done"] = 0

        for oname in output_names:
            # print "[NJA]",oname
            imerged_set = set(self.sample['imerged_to_ijob'][oname].keys())
            processing_list, processing_ID_list = self.get_condor_submitted(baby_merge_jobs=True)
            processing_set = set(processing_list)
            processing_ID_set = set(processing_ID_list)

            do_kill_long_running_condor = True
            if do_kill_long_running_condor:
                # only ~2% of jobs take more than 5 hours
                longrunning_list, longrunning_ID_list = self.get_condor_submitted(running_at_least_hours=4.0, baby_merge_jobs=True)
                if len(longrunning_list) > 0:
                    longrunning_set = set(longrunning_list)
                    longrunning_ID_set = set(longrunning_ID_list)
                    self.do_log("The following merged file indices have been merging for more than 5 hours: %s" % ", ".join(map(str,longrunning_set)))
                    self.do_log("Killing and resubmitting condor IDs: %s" % ", ".join(map(str,longrunning_ID_set)))

                    u.cmd( "condor_rm %s" % " ".join(map(str,longrunning_ID_set)) )
                    processing_set = processing_set - longrunning_set
                    processing_ID_set = processing_ID_set - longrunning_ID_set

            # subtract running jobs from done. we might think they're done if they begin
            # to stageout, but they're not yet done staging out
            # print "[NJA] Making done_set by calling get_merged_done() with merged_dir of %s/%s" % (output_dir,oname)
            done_set = self.get_merged_done(merged_dir="%s/%s/" % (output_dir,oname),startswith="%s_%s_" % (shortname, oname), get_baby_merged=True) - processing_set
            # print "[NJA] Done making done_set:", done_set
            imerged_list = list( imerged_set - processing_set - done_set ) 

            self.sample["postprocessing"]["total"] += len(imerged_set)
            self.sample["postprocessing"]["running"] += len(processing_set)
            self.sample["postprocessing"]["done"] += len(done_set)

            if len(imerged_list) > 0:
                self.sample["status"] = "postprocessing"
                self.do_log("submitting %i merge jobs" % len(imerged_list))

            error = ""
            for imerged in imerged_list:
                input_names=",".join(map(lambda x: "%s_%i.root" % (oname,x),self.sample['imerged_to_ijob'][oname][imerged]))
                input_arguments = map(str,[unmerged_dir+"/"+oname, "%s/%s/" % (output_dir, oname), imerged, oname, shortname, input_names])
                long_ass_args_string = "UNMERGED_DIR=%s;OUTPUT_DIR=%s;IMERGED=%s;OUTPUT_NAME=%s;SHORTNAME=%s;INPUT_NAMES=%s;" % tuple(input_arguments)
                condor_params["args"] = long_ass_args_string

                cfg = cfg_format.format(**condor_params)
                with open(submit_file, "w") as fhout:
                    fhout.write(cfg)

                submit_output = u.get("condor_submit %s" % submit_file)

                if " submitted " in submit_output: 
                    self.do_log("job for merged_ntuple_%i.root submitted successfully" % imerged)
                else:
                    self.do_log("error submitting job for merged_ntuple_%i.root" % imerged)
                    error = submit_output

            if len(error) > 0:
                self.do_log("submit error: %s" % error)

        self.sample["postprocessing"]["idle"] = self.sample["postprocessing"]["total"] - self.sample["postprocessing"]["running"] - self.sample["postprocessing"]["done"]

        return self.sample["postprocessing"]["total"] == self.sample["postprocessing"]["done"]


    def sweep_baby(self, fname):

        script = self.sample["baby"]["sweepRoot_script"]
        if script:

            # need to cd into the script area because if the script references local macros, the paths won't be right
            dirname = os.path.dirname(script)
            scriptname = os.path.basename(script)
            stat, out = u.cmd("cd %s; ./%s %s" % (dirname,scriptname,fname), returnStatus=True)
            return stat == 0 # 0 is good, anything else is bad

        return True

    def sweep_babies(self):
        imerged_swept_set = set(self.sample["baby"]["imerged_swept"])
        output_names = self.sample["baby"]["output_names"]
        merged_dir = self.sample["baby"]["finaldir"]

        not_swept = self.get_merged_done() - imerged_swept_set
        # print self.get_merged_done(),  imerged_swept_set, not_swept
        if len(not_swept) > 0:
            self.do_log("sweeprooting %i output files" % len(not_swept))
        for imerged in not_swept:
            fnames = []
            for output_name in output_names:
                output_name_noext = output_name.rsplit(".",1)[0]
                outfile = "%s/%s/%s_%i.root" % (merged_dir, output_name_noext, output_name_noext, imerged)
                fnames.append(outfile)

            # all output files must be good for this imerged to be good
            good = True
            for fname in fnames:
                if not self.sweep_baby(fname):
                    good = False
                    break

            if good:
                self.sample["baby"]["imerged_swept"].append(imerged)
            else:
                # delete all if even one of them is bad since we will have to run the whole job again anyways
                for fname in fnames:
                    os.system("rm -f %s" % outfile)
                    self.do_log("Deleted the baby file %s because this job failed sweepRooting." % (fname.rsplit("/")[-1]))

        self.sample["baby"]["sweepRooted"] = len(self.sample["baby"]["imerged_swept"])

    def submit_baby_jobs(self):
        filenames = self.sample["baby"]["input_filenames"]
        analysis = self.sample["baby"]["analysis"]
        tag = self.sample["baby"]["baby_tag"]
        shortname = self.sample["shortname"]
        nevts = self.sample["baby"]["nevents"]
        output_names = self.sample["baby"]["output_names_nosplit"]
        exe_args = self.sample["baby"]["exe_args_nosplit"]
        package = self.sample["baby"]["package"]
        executable_script = self.sample["baby"]["executable_script"]

        path_fragment = "%s/%s/%s" % (analysis, tag, shortname)
        condor_log_files = "/nfs-7/userdata/%s/tupler_babies/%s/%s.log" % (os.getenv("USER"),path_fragment,datetime.datetime.now().strftime("+%Y.%m.%d-%H.%M.%S"))
        std_log_files = "/nfs-7/userdata/%s/tupler_babies/%s/std_logs/" % (os.getenv("USER"),path_fragment)

        for directory in [condor_log_files, std_log_files]:
            if not os.path.isdir(std_log_files): os.makedirs(std_log_files)

        submit_file = "%s/%s/submit.cmd" % (self.sample["basedir"], self.misc["pfx_babies"])
        proxy_file = u.get_proxy_file()
        input_files = ",".join([package,executable_script])

        try:
            if not os.path.isdir(std_log_files): os.makedirs(std_log_files)
        except:
            self.do_log("ERROR making log file directory: %s" % std_log_files)
            self.do_log("see if you can make it manually. if not, switch to another uaf.")
            raise Exception("can't make log file directory: %s" % std_log_files)

        condor_params = {
                "exe": executable_script,
                "inpfiles": input_files,
                "condorlog": condor_log_files,
                "stdlog": std_log_files,
                "proxy": proxy_file,
                "requestname": "BABY_%s_%s_%s" % (analysis, tag, shortname),
                }

        cfg_format = "universe=grid \n" \
                     "grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu \n" \
                     "+remote_DESIRED_Sites=\"T2_US_UCSD\" \n" \
                     "executable={exe} \n" \
                     "arguments={args} \n" \
                     "transfer_executable=True \n" \
                     "transfer_input_files={inpfiles} \n" \
                     "transfer_output_files = \"\"\n" \
                     "+Owner = undefined  \n" \
                     "+AutoTwopleRequestname=\"{requestname}\" \n" \
                     "log={condorlog} \n" \
                     "output={stdlog}/1e.$(Cluster).$(Process).out \n" \
                     "error={stdlog}/1e.$(Cluster).$(Process).err \n" \
                     "notification=Never \n" \
                     "should_transfer_files = YES \n" \
                     "when_to_transfer_output = ON_EXIT \n" \
                     "x509userproxy={proxy} \n" \
                     "queue \n" 

        # don't resubmit the ones that are already running or done
        imerged_set = set(self.sample['baby']['imerged'])
        processing_list, processing_ID_list = self.get_condor_submitted()
        processing_set = set(processing_list)

        # subtract running jobs from done. we might think they're done if they begin
        # to stageout, but they're not yet done staging out
        done_set = self.get_merged_done() - processing_set
        imerged_set = imerged_set - processing_set - done_set

        self.sample["baby"]["total"] = len(self.sample['baby']['imerged'])
        self.sample["baby"]["running"] = len(processing_set)
        self.sample["baby"]["condor_done"] = len(done_set)

        if FAKE_BABY_MAKEPERITERATION > 0 and FAKE_BABY_SUBMIT:
            self.fake_baby_creation()


        error = ""
        for filename in filenames:
            
            imerged = int(filename.split(".root")[0].split("_")[-1])
            if imerged not in imerged_set: continue

            if FAKE_BABY_SUBMIT:
                # if len(done_set) == 0:
                #     self.do_log("FAKE baby job for FAKE merged_ntuple_%i.root submitted successfully" % imerged) # NJA
                continue


            condor_params["args"] = " ".join(map(str,\
                    [self.sample["baby"]["cmsswver"], self.sample["baby"]["scramarch"], self.sample["dataset"], filename, analysis, tag, shortname, nevts, output_names, exe_args]\
                    ))
            
            cfg = cfg_format.format(**condor_params)
            with open(submit_file, "w") as fhout:
                fhout.write(cfg)




            submit_output = u.get("condor_submit %s" % submit_file)
            if " submitted " in submit_output: 
                self.do_log("baby job for merged_ntuple_%i.root submitted successfully" % imerged)
            else:
                self.do_log("error submitting baby job for merged_ntuple_%i.root" % imerged)
                error = submit_output
                print error

        self.sample["baby"]["idle"] = self.sample["baby"]["total"] - self.sample["baby"]["running"]
        self.sample["status"] = "condor"

    
    def make_metadata(self):
        metadata_file = self.sample["crab"]["taskdir"]+"/metadata.json"
        d_tot = self.sample.copy()
        with open(metadata_file, "w") as fhout:
            json.dump(d_tot, fhout, sort_keys = True, indent = 4)

        # mirror the central snt directory structure for metadata files
        metadatabank_dir = "/nfs-7/userdata/metadataBank/%s/%s/%s/" \
                % (self.sample["specialdir"], self.sample["shortname"], self.sample["cms3tag"].split("_",1)[1])

        # copy to merged and backup
        u.cmd('chmod a+rw %s' % (metadata_file))
        u.cmd("cp %s %s/" % (metadata_file, self.sample["crab"]["outputdir"]+"/merged/"))
        u.cmd('mkdir -p {0} ; chmod a+rw {0}'.format(metadatabank_dir))
        u.cmd('cp %s %s/' % (metadata_file, metadatabank_dir))

        self.do_log("made metadata and copied it to merged and backup areas")

    def do_send_email(self):
        if self.misc["email_when_done"]:
            u.send_email(self.sample["dataset"])
            self.misc["email_when_done"] = False

    def do_update_dis(self):
        if self.misc["update_dis_when_done"]:

            d = copy.deepcopy(self.sample)

            timestamp = int(time.time())
            if self.baby:
                query_str = "dataset_name=%s,sample_type=%s,analysis=%s,cms3tag=%s,baby_tag=%s,location=%s,timestamp=%i" \
                   % (d["dataset"], d["type"], d["baby"]["analysis"], d["cms3tag"], d["baby"]["baby_tag"], d["baby"]["merged_dir"],timestamp)
            else:
                query_str = "dataset_name=%s,sample_type=%s,cms3tag=%s,gtag=%s,location=%s,nevents_in=%i,nevents_out=%i,xsec=%s,kfactor=%s,filter_eff=%s,timestamp=%i" \
                   % (d["dataset"], d["type"], d["cms3tag"], d["gtag"], d["finaldir"], d["nevents_DAS"], d["nevents_merged"], str(d["xsec"]), str(d["kfact"]), str(d["efact"]),timestamp)

            response = {}
            try:
                succeeded = False
                response = dis.query(query_str, typ='update_snt')
                response = response["response"]["payload"]
                if "updated" in response and str(response["updated"]).lower() == "true": succeeded = True
                self.do_log("updated DIS")
            except: pass

            if not succeeded:
                self.do_log("WARNING: failed to update sample using DIS with query_str: %s" % query_str)
                self.do_log("WARNING: got response: %s" % str(response))
                return

            self.misc["update_dis_when_done"] = False


    def do_done_stuff(self):

        self.do_send_email()
        self.do_update_dis()


    def copy_files(self):
        self.do_log("started copying files to %s" % self.sample["finaldir"])
        u.cmd("mkdir -p %s/" % self.sample["finaldir"])
        u.cmd( "mv %s/merged/* to %s/" % (self.sample["crab"]["outputdir"], self.sample["finaldir"]) )
        self.do_log("finished copying files")

        if self.get_events_in_chain(self.sample["finaldir"]+"/*.root") == self.sample['nevents_merged']:
            # if finaldir doesn't have nevents_merged, must've been a mv error, so redo merging and mv again
            self.do_log("copying was successful, so we're done!!!")
            self.sample["status"] = "done"
        else:
            self.do_log("lost some events after moving into final directory. re-merging now.")
            self.submit_merge_jobs()

    def check_output(self):
        merged_wildcard = self.sample["crab"]["outputdir"]+"/merged/merged_ntuple_*.root"

        tot_events = self.get_events_in_chain(merged_wildcard)

        fnames = glob.glob(merged_wildcard)

        num_failed = 0
        problems = []
        fname_to_info = {}
        imerged_to_ijob = self.sample["imerged_to_ijob"]
        ijob_to_nevents = self.sample["ijob_to_nevents"]

        # main loop to store any and all problems with each merged file
        self.do_log("started looping over merged files to check for validity")
        for fname in fnames:
            failed, nevents_actual, problem = self.check_merged_rootfile(fname, tot_events)

            merged_idx = int(fname.split(".root")[0].split("_")[-1])
            unmerged_indices = imerged_to_ijob[merged_idx]
            nevents_expected = sum(map(lambda x: ijob_to_nevents.get(x)[0], unmerged_indices))

            fname_to_info[fname] = {}
            fname_to_info[fname]["failed"] = failed
            fname_to_info[fname]["nevents_actual"] = nevents_actual
            fname_to_info[fname]["nevents_expected"] = nevents_expected
            fname_to_info[fname]["problem"] = problem

        self.do_log("finished looping over merged files to check for validity")

        # first loop over merged files
        # if any merged file has difference between actual and expected events, resubmit it
        for fname in fname_to_info.keys():
            nevents_actual = fname_to_info[fname]["nevents_actual"]
            nevents_expected = fname_to_info[fname]["nevents_expected"]
            
            if nevents_actual != nevents_expected:
                problem_str = "%s: actual (%i) and expected events (%i) differ" % (fname, nevents_actual, nevents_expected)
                problems.append(problem_str)
                self.do_log(problem_str)
                u.cmd("rm %s" % fname)
                num_failed += 1

        # second loop over merged files where we consider more detailed problems
        # only do this if each of the merged files have the correct event counts
        if num_failed == 0:
            for fname in fname_to_info.keys():
                failed = fname_to_info[fname]["failed"]
                problem = fname_to_info[fname]["problem"]
                if failed:
                    problem_str = "%s: %s" % (problem, fname)
                    problems.append(problem_str)
                    self.do_log(problem_str)
                    u.cmd("rm %s" % fname)
                    num_failed += 1

        if num_failed > 0:
            self.do_log("%i merged ntuples are bad" % num_failed)
            self.submit_merge_jobs()

        self.sample["checks"]["nproblems"] = num_failed
        self.sample["checks"]["problems"] = problems
        self.sample['nevents_merged'] = self.sample['nevents_unmerged'] if num_failed == 0 else 0

        return num_failed == 0


if __name__=='__main__':
    pass
