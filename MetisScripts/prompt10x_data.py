import time
import itertools
import json
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email
import scripts.dis_client as dis

def get_tasks():


    pds = ["MuonEG","DoubleMuon","EGamma", "JetHT", "MET", "SingleMuon"]
    proc_vers = [

            ("Run2018A","v1"),
            ("Run2018A","v2"),
            ("Run2018A","v3"),
            ("Run2018B","v1"),
            ("Run2018B","v2"),

            ("Run2018C","v1"),
            ("Run2018C","v2"),
            ("Run2018C","v3"),

            ("Run2018D","v1"), # very short, not in golden json, most PDs are missing on DAS
            ("Run2018D","v2"),

            ]
    dataset_names =  ["/{0}/{1}-PromptReco-{2}/MINIAOD".format(x[0],x[1][0],x[1][1]) for x in itertools.product(pds,proc_vers)]

    # skip datasets that aren't on das
    out = dis.query("/*/Run2018*-PromptReco*/MINIAOD")
    dis_names = out["response"]["payload"]
    dis_names = [ds for ds in dis_names if any("/{}/".format(pd) in ds for pd in pds)]
    dataset_names = list(set(dataset_names) & set(dis_names))

    tasks = []
    for dsname in dataset_names:

        cmsswver, tarfile = None, None
        scram_arch = "slc6_amd64_gcc630"
        pset = "psets/pset_prompt10x_data_1.py"

        if "Run2018A-PromptReco-v1" in dsname:
            cmsswver = "CMSSW_10_1_2_patch2"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V10_01_00_1012p2.tar.gz"
        elif "Run2018A-PromptReco-v2" in dsname:
            cmsswver = "CMSSW_10_1_5"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V10_01_00_1015.tar.gz"
        elif "Run2018A-PromptReco-v3" in dsname:
            cmsswver = "CMSSW_10_1_5"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V10_01_00_1015.tar.gz"
        elif "Run2018B-PromptReco-v1" in dsname:
            cmsswver = "CMSSW_10_1_5"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V10_01_00_1015.tar.gz"
        elif "Run2018B-PromptReco-v2" in dsname:
            cmsswver = "CMSSW_10_1_7"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V10_01_00_1017.tar.gz"
        elif ("Run2018C-PromptReco-" in dsname) or ("Run2018D-PromptReco-" in dsname):
            cmsswver = "CMSSW_10_2_1"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V10_01_00_1021_nodeepak8.tar.gz"
            scram_arch = "slc6_amd64_gcc700"
            pset = "psets/pset_prompt10x_data_2.py"


        task = CMSSWTask(
                sample = DBSSample(dataset=dsname),
                open_dataset = False,
                # flush = ((i+1)%48==0),
                # flush = ((i)%48==0),
                events_per_output = 350e3,
                output_name = "merged_ntuple.root",
                recopy_inputs = False,
                tag = "CMS4_V10-01-00",
                scram_arch = scram_arch,
                # global_tag = "", # if global tag blank, one from DBS is used
                pset = pset,
                pset_args = "data=True prompt=True",
                cmssw_version = cmsswver,
                # condor_submit_params = {"use_xrootd":True},
                tarfile = tarfile,
                is_data = True,
                publish_to_dis = True,
                snt_dir = True,
                special_dir = "run2_data2018_prompt/",
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
                # if not task.complete():
                task.process()
            except:
                traceback_string = traceback.format_exc()
                print "Runtime error:\n{0}".format(traceback_string)
                send_email(subject="metis error", body=traceback_string)
            total_summary[dsname] = task.get_task_summary()
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do()
        time.sleep(1.*3600)

