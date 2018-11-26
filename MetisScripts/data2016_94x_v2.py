import time
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email

import scripts.dis_client as dis

def get_tasks():

    pds = [
            "MuonEG","SingleElectron","MET","SinglePhoton","SingleMuon","DoubleMuon","JetHT","DoubleEG","HTMHT"
            ]
    out = dis.query("/*/Run2016*-17Jul2018*/MINIAOD")
    all_names = out["response"]["payload"]
    all_names = sorted([ds for ds in all_names if any("/{}/".format(pd) in ds for pd in pds)])

    out = dis.query("/*/Run2016*-17Jul2018*/MINIAOD,cms3tag=*17 | grep dataset_name",typ="snt")
    already_done = out["response"]["payload"]
    already_done = sorted([ds for ds in already_done if any("/{}/".format(pd) in ds for pd in pds)])
    dataset_names = list(set(all_names)-set(already_done))

    # dataset_names = [ds for ds in dataset_names if ds not in ["/MET/Run2016H-17Jul2018-v1/MINIAOD"]]

    # dataset_names = []
    # dataset_names += ["/MET/Run2016B-17Jul2018_ver1-v1/MINIAOD"]
    # dataset_names += ["/SingleMuon/Run2016B-17Jul2018_ver2-v1/MINIAOD"] # deleted a corruption and want to make sure this gets redone
    # dataset_names += ["/SinglePhoton/Run2016E-17Jul2018-v1/MINIAOD"] # deleted a corruption and want to make sure this gets redone

    # dataset_names = []
    # dataset_names += ["/JetHT/Run2016B-17Jul2018_ver2-v2/MINIAOD"] # deleted a corruption and want to make sure this gets redone

    tasks = []
    for dsname in dataset_names:

        cmsswver = "CMSSW_9_4_9"
        tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V09-04-17_949.tar.gz"
        pset = "psets/pset_data2016_94x_v2.py"
        scramarch = "slc6_amd64_gcc630"

        task = CMSSWTask(
                sample = DBSSample(dataset=dsname),
                open_dataset = False,
                events_per_output = 400e3,
                output_name = "merged_ntuple.root",
                tag = "CMS4_V09-04-17",
                pset = pset,
                pset_args = "data=True prompt=False name=DQM",
                scram_arch = scramarch,
                cmssw_version = cmsswver,
                condor_submit_params = {"use_xrootd":True},
                tarfile = tarfile,
                is_data = True,
                publish_to_dis = True,
                snt_dir = True,
                special_dir = "run2_data2016_94x/",
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
        break
        # StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do()
        # time.sleep(1.*3600)
