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

def check_merged_rootfile(fname, total_events, treename="Events"):
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

    if nevents_branch != total_events:
        f.Close()
        return 1, n_entries, "evt_nEvts (%i) differs from total merged event count (%i)" % (nevents_branch, total_events)

    if (recalc_scale1fb - scale1fb_min)/scale1fb_min > 1e-6:
        f.Close()
        return 1, n_entries, "Inconsistent scale1fb. In file: %f, Calculated: %f" % (scale1fb_min, recalc_scale1fb)

    f.Close()
    return 0, n_entries, ""

if __name__ == "__main__":
    samples = [
            # dis_client.py -t snt "/WJetsToLNu_HT-*_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_*/MINIAODSIM | grep location,xsec,kfactor,filter_eff" --table
            # dis_client.py -t snt "/ZJetsToNuNu_HT-100To200_13TeV-madgraph/RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_*/MINIAODSIM | grep location,xsec,kfactor,filter_eff" --table
            # dis_client.py -t snt "/GJets_HT-*_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-*/MINIAODSIM | grep location,xsec,kfactor,filter_eff" --table
            # dis_client.py -t snt "/ST_t-channel_top_4f_leptonDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM | grep location,xsec,kfactor,filter_eff" --table
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v3/V08-00-05/"      , 1345.0  , 1.21 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/" , 1345.0  , 1.21 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"      , 359.7   , 1.21 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/" , 359.7   , 1.21 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"      , 48.91   , 1.21 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/" , 48.91   , 1.21 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/ZJetsToNuNu_HT-100To200_13TeV-madgraph_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"                             , 280.35  , 1.23 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/ZJetsToNuNu_HT-100To200_13TeV-madgraph_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/"                        , 280.35  , 1.23 , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-40To100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"            , 20790   , 1.0  , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v4/V08-00-05/"           , 9238    , 1.0  , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"           , 2305    , 1.0  , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"           , 274.4   , 1.0  , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"           , 93.46   , 1.0  , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/ST_t-channel_top_4f_leptonDecays_13TeV-powheg-pythia8_TuneCUETP8M1_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/" , 44.1    , 1.0  , 1.0) ,
            # ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/W1JetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/",              9493.0,    1.238, 1.0),
        ]

    # for in_dir,xsec in samples:
    #     print("rm -f {0}/*.root ; mv {0}/new_xsec/*.root {0}/".format(in_dir))

    for in_dir,xsec,kfact,efact in samples:
        print("Considering sample %s" % in_dir)
        print("with xsec %f" % xsec)
        print("     kfact %f" % kfact)
        print("     efact %f" % efact)
        metadata_fname = in_dir + "/metadata.json"

        with open(metadata_fname, "r") as fhin:
            metadata = json.load(fhin)
            nevents = sum([x[0] for x in metadata["ijob_to_nevents"].values()])
            nevents_effective = sum([x[1] for x in metadata["ijob_to_nevents"].values()])

            # efact = metadata["efact"]

            nevents_chain = get_events_in_chain(in_dir + "/*.root")
            # nevents_chain = nevents
            if nevents_chain != nevents: continue

            imerged_set = set(map(int,metadata["imerged_to_ijob"].keys()))

            total = len(imerged_set)
            done = len(get_merged_done(in_dir)-get_condor_submitted(in_dir))
            print("Done: %i / %i" % (done,total))

            if total != done:

                submit_merge_jobs(in_dir=in_dir, imerged_set=imerged_set, nevents=nevents, nevents_effective=nevents_effective, \
                                   xsec=xsec, kfactor=kfact, efactor=efact)

            else:

                from glob import glob
                nbad = 0
                for fname in glob(in_dir+"/new_xsec/*.root"):
                    isbad, nentries, prob = check_merged_rootfile(fname, nevents)
                    if isbad:
                        nbad += 1
                        print("%s IS BAD: %s" % (fname, isbad))
                        print("Reason: %s" % (prob))


                if isbad == 0:
                    new_nevents = get_events_in_chain(in_dir + "/new_xsec/*.root")
                    if new_nevents == nevents:
                        print("rm -f {0}/*.root ; mv {0}/new_xsec/* {0}/".format(in_dir))
                        metadata["xsec"] = xsec
                        metadata["kfact"] = kfact
                        metadata["efact"] = efact
                        with open("tmp.json", "w") as fhout:
                            json.dump(metadata, fhout, sort_keys = True, indent = 4)
                        u.cmd("cp tmp.json %s" % in_dir+"/new_xsec/metadata.json")
                        print("made new metadata.json!")

                    else:
                        print("ERROR, nevents don't match (new,old) = (%i,%i)" % (new_nevents, nevents))





