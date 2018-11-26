import time
import itertools
import json
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email


def get_tasks():

    infos = [


            # "/TTZToLLNuNu_M-10_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext3-v1/MINIAODSIM|0.2529|1|1",

            # "/TTTT_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.009103|1|1",
            # "/WGToLNuG_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|489|1.0482618|1.0",

            # "/ZZTo4L_13TeV_powheg_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1.256|1.0|1.0",
            # "/WpWpJJ_EWK-QCD_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.03711|1.0|1.0",
            # "/ZGTo2LG_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|117.864|1.051212|1.0",
            # "/TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|730.0|1.139397|1.0",
            # "/TTGamma_Dilept_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.632|1.0|1.0",
            # "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v1/MINIAODSIM|6104.0|0.9871|1.0",

            ]


    tasks = []
    for info in infos:
        dsname = info.split("|")[0].strip()
        xsec = float(info.split("|")[1].strip())
        kfact = float(info.split("|")[2].strip())
        efact = float(info.split("|")[3].strip())

        cmsswver = "CMSSW_9_4_9"
        tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V09-04-17_949.tar.gz"
        pset = "psets/pset_mc2016_94x_v3.py"
        scramarch = "slc6_amd64_gcc630"

        allow_invalid_files = False
        events_per_output = 300e3

        # allow_invalid_files = any(x in dsname for x in [
        #     "/TTGJets_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8",
        #     "/WGToLNuG_01J_5f_TuneCP5_13TeV-amcatnloFXFX-pythia8",
        #     "/ZGToLLG_01J_5f_TuneCP5_13TeV-amcatnloFXFX-pythia8",
        #     "/ST_tWll_5f_LO_TuneCP5_PSweights_13TeV-madgraph-pythia8",
        #     "_HToTT_",
        #     "/SMS-T1tttt_mGluino",
        #     ])


        task = CMSSWTask(
                sample = DBSSample(
                    dataset=dsname,
                    xsec=xsec,
                    kfact=kfact,
                    efact=efact,
                    allow_invalid_files=allow_invalid_files,
                    ),
                events_per_output = events_per_output,
                output_name = "merged_ntuple.root",
                tag = "CMS4_V09-04-17",
                pset = pset,
                pset_args = "data=False",
                scram_arch = scramarch,
                condor_submit_params = {"use_xrootd":True},
                cmssw_version = cmsswver,
                tarfile = tarfile,
                # min_completion_fraction = 0.90,
                # min_completion_fraction = 0.84,
                publish_to_dis = True,
                snt_dir = True,
                special_dir = "run2_mc2016_94x/",
        )
        tasks.append(task)
    return tasks

if __name__ == "__main__":

    for i in range(10000):
        total_summary = {}
        total_counts = {}
        tasks = []
        tasks.extend(get_tasks())
        for task in tasks:
            dsname = task.get_sample().get_datasetname()
            try:
                if not task.complete():
                    task.process()
            except:
                traceback_string = traceback.format_exc()
                print "Runtime error:\n{0}".format(traceback_string)
                send_email(subject="metis error", body=traceback_string)
            total_summary[dsname] = task.get_task_summary()
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do()
        time.sleep(1.*3600)
