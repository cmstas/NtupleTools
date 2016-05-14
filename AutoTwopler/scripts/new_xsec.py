from __future__ import print_function

import sys
sys.path.insert(0,"../")
import utils as u
import os, glob, select
import datetime, tarfile, pprint
import pickle, json, logging
import multiprocessing
import re, copy

# try:
    # I recommend putting `Root.ErrorIgnoreLevel: Error` in your .rootrc file
from ROOT import TFile, TH1F, TChain
# except:
#     print ">>> Make sure to source setup.sh first!"
#     sys.exit()


def get_events_in_chain(fname_wildcard):
    nevents = 0
    try:
        ch = TChain("Events")
        ch.Add(fname_wildcard)
        nevents = ch.GetEntries()
    except: pass
    return nevents

def get_condor_submitted(in_dir):
    cmd = "condor_q $USER -autoformat ClusterId GridJobStatus EnteredCurrentStatus CMD ARGS -const 'new_xsec==\"yes\"'"
    output = u.get(cmd)

    merged_ids = []
    for line in output.split("\n"):
        if len(line.strip()) < 2: continue
        if in_dir not in line: continue

        clusterID, status, entered_current_status, cmd, in_file = line.strip().split(" ")[:5]

        merged_index = int(in_file.split("_")[-1].split(".")[0])
        merged_ids.append(int(merged_index))

    return set(merged_ids)

def get_merged_done(in_dir):
    output_dir = in_dir + "/new_xsec/"
    if not os.path.isdir(output_dir): return set()
    files = os.listdir(output_dir)
    files = [f for f in files if f.endswith(".root")]
    # print(in_dir + str(files))
    return set(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))

def submit_merge_jobs(in_dir, imerged_set, nevents, nevents_effective, xsec, kfactor, efactor):
    submit_file = "temp.sub"
    executable_script = os.getcwd()+"/xsecWrapper.sh"
    addbranches_script = os.getcwd()+"/addBranches.C"
    proxy_file = u.get("find /tmp/x509up_u* -user $USER").strip()
    condor_log_files = "/nfs-7/userdata/%s/new_xsec/%s.log" % (os.getenv("USER"),datetime.datetime.now().strftime("+%Y.%m.%d-%H.%M.%S"))
    std_log_files = "/nfs-7/userdata/%s/new_xsec/std_logs/" % (os.getenv("USER"))

    if not os.path.isdir(std_log_files): os.makedirs(std_log_files)

    condor_params = {
            "exe": executable_script,
            "inpfiles": ",".join([executable_script, addbranches_script]),
            "condorlog": condor_log_files,
            "stdlog": std_log_files,
            "proxy": proxy_file,
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
                 "+new_xsec=\"yes\" \n" \
                 "log={condorlog} \n" \
                 "output={stdlog}/1e.$(Cluster).$(Process).out \n" \
                 "error={stdlog}/1e.$(Cluster).$(Process).err \n" \
                 "notification=Never \n" \
                 "x509userproxy={proxy} \n" \
                 "should_transfer_files = yes \n" \
                 "queue \n" 

    # don't resubmit the ones that are already running or done
    processing_set = get_condor_submitted(in_dir)

    # subtract running jobs from done. we might think they're done if they begin
    # to stageout, but they're not yet done staging out
    done_set = get_merged_done(in_dir) - processing_set
    imerged_list = list( imerged_set - processing_set - done_set ) 

    if len(imerged_list) > 0:
        print("submitting %i merge jobs" % len(imerged_list))

    error = ""
    # print("imerged to submit"+ str(imerged_list))
    # print("imerged done"+ str(list(done_set)))
    for imerged in imerged_list:
        in_file = "%s/merged_ntuple_%i.root" % (in_dir, imerged)

        input_arguments = " ".join(map(str,[in_file, nevents, nevents_effective, xsec, kfactor, efactor]))
        condor_params["args"] = input_arguments

        cfg = cfg_format.format(**condor_params)
        with open(submit_file, "w") as fhout:
            fhout.write(cfg)

        # print("fake submission for merged_ntuple_%i.root" % imerged)

        submit_output = u.get("condor_submit %s" % submit_file)
        if " submitted " in submit_output: 
            print("job for merged_ntuple_%i.root submitted successfully" % imerged)
        else:
            print("error submitting job for merged_ntuple_%i.root" % imerged)
            error = submit_output

    if len(error) > 0:
        print("submit error: %s" % error)

if __name__ == "__main__":
    samples = [
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/ZJetsToNuNu_HT-600To800_13TeV-madgraph_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/",3.279),
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/ZJetsToNuNu_HT-800To1200_13TeV-madgraph_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/",1.514),
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/ZJetsToNuNu_HT-1200To2500_13TeV-madgraph_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/",0.368),
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/ZJetsToNuNu_HT-2500ToInf_13TeV-madgraph_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/",0.00812),
        ]

    for in_dir,xsec in samples:
        print("Considering sample %s" % in_dir)
        print("with xsec %f" % xsec)
        metadata_fname = in_dir + "/metadata.json"

        with open(metadata_fname, "r") as fhin:
            metadata = json.load(fhin)
            nevents = sum([x[0] for x in metadata["ijob_to_nevents"].values()])
            nevents_effective = sum([x[1] for x in metadata["ijob_to_nevents"].values()])

            kfact = metadata["kfact"]
            efact = metadata["efact"]

            nevents_chain = get_events_in_chain(in_dir + "/*.root")
            if nevents_chain != nevents: continue

            imerged_set = set(map(int,metadata["imerged_to_ijob"].keys()))

            print("Total: %i" % len(imerged_set))
            print("Done: %i" % len(get_merged_done(in_dir)))

            submit_merge_jobs(in_dir=in_dir, imerged_set=imerged_set, nevents=nevents_chain, nevents_effective=nevents_effective, \
                               xsec=xsec, kfactor=kfact, efactor=efact)



