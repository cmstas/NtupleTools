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
    # pds = ["MuonEG","DoubleMuon","EGamma"] #, "JetHT", "MET", "SingleMuon"]
    # pds = ["EGamma"]
    proc_vers = [

            ("Run2018A","v1"),
            ("Run2018A","v2"),
            ("Run2018A","v3"),
            ("Run2018B","v1"),
            ("Run2018B","v2"),
            ("Run2018C","v1"),
            ("Run2018C","v2"),
            ("Run2018C","v3"),

            # ("Run2018C","v1"),

            # ("Run2018D","v1"), # very short, not in golden json, most PDs are missing on DAS
            # ("Run2018D","v2"),

            ]
    dataset_names =  ["/{0}/{1}-17Sep2018-{2}/MINIAOD".format(x[0],x[1][0],x[1][1]) for x in itertools.product(pds,proc_vers)]

    # skip datasets that aren't on das
    # out = dis.query("/*/Run2018*-17Sep2018*/MINIAOD")
    # NOTE Screw it, just get all the datasets and pretend they are open. Comp/ops people allow production->valid flag if
    # the dataset is 99% complete to not "block their distribution" even though it's stupid.
    # See https://hypernews.cern.ch/HyperNews/CMS/get/physics-validation/3267/1/1/1/1/2/2.html
    out = dis.query("/*/Run2018*-17Sep2018*/MINIAOD,all")
    dis_names = out["response"]["payload"]
    dis_names = [ds for ds in dis_names if any("/{}/".format(pd) in ds for pd in pds)]
    dataset_names = list(set(dataset_names) & set(dis_names))

    # print dataset_names
    # blah

    tasks = []
    for dsname in dataset_names:

        scram_arch = "slc6_amd64_gcc700"
        cmsswver = "CMSSW_10_2_4_patch1"
        pset = "psets/pset_data2018_102x.py"
        tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V10-02-01_1024p1.tar.xz"

        task = CMSSWTask(
                sample = DBSSample(dataset=dsname),
                open_dataset = True,
                # flush = True,
                # flush = ((i+1)%48==0),
                # flush = ((i)%48==0),
                events_per_output = 300e3,
                output_name = "merged_ntuple.root",
                tag = "CMS4_V10-02-01",
                scram_arch = scram_arch,
                pset = pset,
                pset_args = "data=True prompt=True", # NOTE, this isn't actually prompt. but just read the NtupleMaker 10x branch readme, ok?
                cmssw_version = cmsswver,
                # condor_submit_params = {"use_xrootd":True},
                tarfile = tarfile,
                is_data = True,
                publish_to_dis = True,
                snt_dir = True,
                special_dir = "run2_data2018/",
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
                # if not task.complete(): task.finalize()
                task.process()
            except:
                traceback_string = traceback.format_exc()
                print "Runtime error:\n{0}".format(traceback_string)
                send_email(subject="metis error", body=traceback_string)
            total_summary[dsname] = task.get_task_summary()
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do()
        time.sleep(1.*3600)
