import time
import itertools
import json
import traceback

from metis.Sample import DBSSample, DirectorySample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email
from metis.Constants import Constants


def get_tasks():

    samples = [

            # DirectorySample(
            #     location = "/hadoop/cms/store/user/mliu/mcProduction/MINIAODSIM/www/",
            #     globber = "*_MiniAODv2.root",
            #     dataset = "/PrivateWWW/www-cms4-Private80X-v1/MINIAODSIM",
            #     gtag = "80X_mcRun2_asymptotic_2016_TrancheIV_v6",
            #     ),

            # DirectorySample(
            #     location = "/hadoop/cms/store/user/mliu/mcProduction/MINIAODSIM/www_ext/",
            #     globber = "www_ext_*_MiniAODv2.root",
            #     dataset = "/PrivateWWW/wwwext-cms4-Private80X-v1/MINIAODSIM",
            #     gtag = "80X_mcRun2_asymptotic_2016_TrancheIV_v6",
            #     ),

            # DirectorySample(
            #     location = "/hadoop/cms/store/user/mliu/mcProduction/MINIAODSIM/TChiWH_HToVVTauTau_HToBB_mChargino200_mLSP50/",
            #     globber = "*_MiniAODv2.root",
            #     dataset = "/TChiWH_HToVVTauTau_HToBB_mChargino200_mLSP50/cms4-Private80X-v1/MINIAODSIM",
            #     gtag = "80X_mcRun2_asymptotic_2016_TrancheIV_v6",
            #     ),

            DirectorySample(
                location = "/hadoop/cms/store/user/bhashemi/mcProduction/MINIAODSIM/wh_ext/",
                globber = "*MiniAODv2.root",
                dataset = "/TChiWH_HToVVTauTau_HToBB_mChargino200_mLSP50/cms4-Private80X-v1/MINIAODSIM",
                gtag = "80X_mcRun2_asymptotic_2016_TrancheIV_v6",
                ),


            ]

    tasks = []

    for sample in samples:

        pset_args = "data=False"
        cmsswver = "CMSSW_8_0_26_patch1"
        tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-02_2017Sep27.tar.gz"

        task = CMSSWTask(
                sample = sample,
                files_per_output = 30,
                output_name = "merged_ntuple.root",
                tag = "CMS4_V00-00-02_2017Sep27",
                pset = "psets/pset_moriondremc.py",
                pset_args = pset_args,
                # condor_submit_params = {"use_xrootd":True},
                condor_submit_params = {"sites":"T2_US_UCSD"},
                cmssw_version = cmsswver,
                tarfile = tarfile,
                special_dir = "run2_moriond17_cms4/ProjectMetis",
                min_completion_fraction = 0.90,
                publish_to_dis = True,
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
