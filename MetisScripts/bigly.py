import time
import os
import sys
import itertools
import traceback

from metis.Sample import DBSSample, DirectorySample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email, interruptible_sleep
from metis.Optimizer import Optimizer
from pprint import pprint

# # /nfs-7 mount is screwed up from uaf-4, so mirror stuff for now
tarballdir = "/nfs-7/userdata/libCMS3"
# tarballdir = "/home/users/namin/2017/ProjectMetis/tarmirror"

def get_master_list():

    # Master list of all campaigns and samples
    dinfos = {}

    ########################################
    ############ Data 2016 94x  ############
    ########################################
    pds = [
            "MuonEG",
            "SingleElectron",
            "MET",
            "SinglePhoton",
            "SingleMuon",
            "DoubleMuon",
            "JetHT",
            "DoubleEG",
            "HTMHT"
            ]
    proc_vers = [
            ("Run2016B","_ver2-v1"),
            ("Run2016C","-v1"),
            ("Run2016D","-v1"),
            ("Run2016E","-v1"),
            ("Run2016F","-v1"),
            ("Run2016G","-v1"),
            ("Run2016H","-v1"),
            ]
    dataset_names =  ["/{0}/{1}-17Jul2018{2}/MINIAOD".format(x[0],x[1][0],x[1][1]) for x in itertools.product(pds,proc_vers)]
    # Three PDs needed to be reprocessed, so we replace them here
    dataset_names = ",".join(dataset_names).replace("/JetHT/Run2016B-17Jul2018_ver2-v1/MINIAOD","/JetHT/Run2016B-17Jul2018_ver2-v2/MINIAOD") \
            .replace("/MET/Run2016H-17Jul2018-v1/MINIAOD","/MET/Run2016H-17Jul2018-v2/MINIAOD") \
            .replace("/MuonEG/Run2016E-17Jul2018-v1/MINIAOD","/MuonEG/Run2016E-17Jul2018-v2/MINIAOD").split(",")
    # Can end up with empty string as a single datasetname if everything is commented out. Filter out non-truthy things.
    dataset_names = filter(None,dataset_names)
    dinfos["data_2016_94x_v3"] = {
            "samples": dataset_names,
            "params": dict(
                pset_args = "data=True year=2016",
                special_dir = "run2_data2016_94x/",
                global_tag = "94X_dataRun2_v10",
                is_data = True,
                tag = "CMS4_V10-02-05",
                tarfile = "{}/lib_CMS4_V10-02-05_1025.tar.xz".format(tarballdir),
                )
            }

    ########################################
    ########## Data 2017 rereco 1 ##########
    ########################################
    pds = [
            "MuonEG",
            "SingleElectron",
            "MET",
            "SinglePhoton",
            "SingleMuon",
            "DoubleMuon",
            "JetHT",
            "DoubleEG",
            "HTMHT",
            ]
    eraletters = list("BCDEF")
    # eraletters = list("B")
    dataset_names = ["/{}/Run2017{}-31Mar2018-v1/MINIAOD".format(*x) for x in list(itertools.product(pds,eraletters))]
    dinfos["data_2017_94x_rereco1"] = {
            "samples": dataset_names,
            "params": dict(
                pset_args = "data=True year=2017 metrecipe=True",
                special_dir = "run2_data2017/",
                global_tag = "94X_dataRun2_v11",
                is_data = True,
                tag = "CMS4_V10-02-05",
                tarfile = "{}/lib_CMS4_V10-02-05_1025.tar.xz".format(tarballdir),
                events_per_output = 250e3,
                )
            }

    ########################################
    ########## Data 2017 rereco 2 ##########
    ########################################
    pds = [
            "MuonEG",
            "SingleElectron",
            "MET",
            "SinglePhoton",
            "SingleMuon",
            "DoubleMuon",
            "JetHT",
            "DoubleEG",
            "HTMHT",
            ]
    eraletters = list("F")
    dataset_names = ["/{}/Run2017{}-09May2018-v1/MINIAOD".format(*x) for x in list(itertools.product(pds,eraletters))]
    dinfos["data_2017_94x_rereco2"] = {
            "samples": dataset_names,
            "params": dict(
                pset_args = "data=True year=2017 metrecipe=False",
                special_dir = "run2_data2017/",
                global_tag = "94X_dataRun2_v11",
                is_data = True,
                tag = "CMS4_V10-02-04",
                tarfile = "{}/lib_CMS4_V10-02-04_1025.tar.xz".format(tarballdir),
                )
            }

    ########################################
    ########## Data 2018 rereco 1 ##########
    ########################################
    pds = [
            "MuonEG",
            "EGamma",
            "DoubleMuon",
            "JetHT",
            "MET",
            "SingleMuon",
            ]
    proc_vers = [
            ("Run2018A","v1"),
            ("Run2018A","v2"),
            ("Run2018B","v1"),
            ("Run2018C","v1"),
            ]
    # Half of the PDs had bad EraA, so Av2 was made. Delete the ones that don't exist here
    dataset_names =  list(set(["/{0}/{1}-17Sep2018-{2}/MINIAOD".format(x[0],x[1][0],x[1][1]) for x in itertools.product(pds,proc_vers)])-set([
        "/DoubleMuon/Run2018A-17Sep2018-v1/MINIAOD",
        "/EGamma/Run2018A-17Sep2018-v1/MINIAOD",
        "/JetHT/Run2018A-17Sep2018-v2/MINIAOD",
        "/MET/Run2018A-17Sep2018-v2/MINIAOD",
        "/MuonEG/Run2018A-17Sep2018-v2/MINIAOD",
        "/SingleMuon/Run2018A-17Sep2018-v1/MINIAOD",
        ]))
    dinfos["data_2018_102x_rereco1"] = {
            "samples": dataset_names,
            "params": dict(
                pset_args = "data=True year=2018",
                special_dir = "run2_data2018/",
                global_tag = "102X_dataRun2_Sep2018Rereco_v1",
                is_data = True,
                # FIXME Eventually flip these two bools, get more, then flip back
                open_dataset = False,
                flush = False,
                )
            }

    ########################################
    ########### Data 2018 prompt ###########
    ########################################
    pds = [
            "MuonEG",
            "JetHT",
            "MET",
            "SingleMuon",
            "EGamma",
            "DoubleMuon",
            ]
    proc_vers = [
            ("Run2018A","v1"),
            ("Run2018A","v2"),
            ("Run2018A","v3"),
            ("Run2018B","v1"),
            ("Run2018B","v2"),
            ("Run2018C","v1"),
            ("Run2018C","v2"),
            ("Run2018C","v3"),
            ("Run2018D","v2"),
            ]
    dataset_names =  ["/{0}/{1}-PromptReco-{2}/MINIAOD".format(x[0],x[1][0],x[1][1]) for x in itertools.product(pds,proc_vers)]
    dataset_names += ["/EGamma/Run2018D-22Jan2019-v2/MINIAOD"] # Not a rereco. Just a re-promptreco to recover deleted files.
    dinfos["data_2018_102x_prompt"] = {
            "samples": dataset_names,
            "params": dict(
                additional_input_files = [
                    # "/home/users/namin/2017/ProjectMetis/metis/executables/condor_chirp",
                    # "/home/users/namin/2017/ProjectMetis/scratch/single102x/CMSSW_10_2_5/src/CMS3/NtupleMaker/test/Cert_314472-325175_13TeV_PromptReco_Collisions18_JSON.txt"
                    ],
                # pset_args = "data=True year=2018",
                # pset_args = "data=True year=2018 goldenjson=Cert_314472-325175_13TeV_PromptReco_Collisions18_JSON.txt",
                pset_args = "data=True year=2018",
                special_dir = "run2_data2018/",
                global_tag = "102X_dataRun2_Prompt_v11",
                is_data = True,
                )
            }

    ########################################
    ############# MC 2016 80x ##############
    ########################################
    infos = [

            "/GluGluHToZZTo4L_M125_13TeV_powheg2_JHUGenV709_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|29.99|0.0002745|1.0",
            "/GluGluHToZZTo4L_M125_13TeV_tunedown_powheg2_JHUgenV700_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|29.99|0.0002745|1.0",
            "/GluGluHToZZTo4L_M125_13TeV_tuneup_powheg2_JHUgenV700_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|29.99|0.0002745|1.0",
            "/VBF_HToZZTo4L_M125_13TeV_tunedown_powheg2_JHUgenV700_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|3.769|0.0002745|1.0",
            "/VBF_HToZZTo4L_M125_13TeV_tuneup_powheg2_JHUgenV700_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|3.769|0.0002745|1.0",
            "/ZH_HToZZ_4LFilter_M125_13TeV_powheg2-minlo-HZJ_JHUGenV709_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.7534|0.00074903|1.0",
            "/ZH_HToZZ_4LFilter_M125_13TeV_tunedown_powheg-minlo-HWJ_JHUgenV700_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.7534|0.00074903|1.0",
            "/ZH_HToZZ_4LFilter_M125_13TeV_tuneup_powheg-minlo-HWJ_JHUgenV700_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.7534|0.00074903|1.0",
            "/WWZJetsTo4L2Nu_4f_TuneCUETP8M1_13TeV_aMCatNLOFxFx_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|5.249605e-4|1.0|1.0",

            ]
    dinfos["mc_2016_80x_v2"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2016 is80x=True",
                special_dir = "run2_mc2016/",
                global_tag = "80X_mcRun2_asymptotic_2016_TrancheIV_v6",
                tag = "CMS4_V10-02-05",
                tarfile = "{}/lib_CMS4_V10-02-05_1025.tar.xz".format(tarballdir),
                )
        }


    ########################################
    ############# MC 2016 94x ##############
    ########################################
    infos = [

            # "/DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|18610.0|1.11|1.0",
            "/DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|18610.0|1.11|1.0",
            # "/DYJetsToLL_M-50_HT-100to200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|147.4|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-100to200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|147.4|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-1200to2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.1514|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-200to400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|40.99|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-200to400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|40.99|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-2500toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.003565|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-400to600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|5.678|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-400to600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|5.678|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-600to800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|1.367|1.23|1.0",
            # "/DYJetsToLL_M-50_HT-800to1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.6304|1.23|1.0",
            "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v1/MINIAODSIM|6104.0|0.9871|1",
            # "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|4895.0|1.23|1.0",
            # "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v2/MINIAODSIM|4895.0|1.23|1.0",
            "/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|202.9|1|1",
            # "/GJets_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|9238|1.0|1.0",
            # "/GJets_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|2305|1.0|1.0",
            # "/GJets_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|2305|1.0|1.0",
            # "/GJets_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|274.4|1.0|1.0",
            # "/GJets_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|274.4|1.0|1.0",
            # "/GJets_HT-40To100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|20790|1.0|1.0",
            # "/GJets_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|93.46|1.0|1.0",
            # "/GJets_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|93.46|1.0|1.0",
            "/QCD_HT50to100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|246400000|1|1",
            "/QCD_HT100to200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|27540000|1|1",
            "/QCD_HT200to300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|1717000|1|1",
            # "/QCD_HT1000to1500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|1207|1.0|1.0",
            # "/QCD_HT1000to1500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|1207|1.0|1.0",
            # "/QCD_HT1500to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|119.9|1.0|1.0",
            # "/QCD_HT1500to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|119.9|1.0|1.0",
            # "/QCD_HT2000toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|25.24|1.0|1.0",
            # "/QCD_HT2000toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|25.24|1.0|1.0",
            # "/QCD_HT300to500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|347700|1.0|1.0",
            # "/QCD_HT300to500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|347700|1.0|1.0",
            # "/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|32100|1.0|1.0",
            # "/QCD_HT500to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|32100|1.0|1.0",
            # "/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|6831|1.0|1.0",
            # "/ST_t-channel_antitop_4f_inclusiveDecays_13TeV-powhegV2-madspin-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|80.95|1.0|1.0",
            # "/ST_t-channel_top_4f_inclusiveDecays_13TeV-powhegV2-madspin-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|136.02|1.0|1.0",
            "/ST_s-channel_4f_leptonDecays_13TeV-amcatnlo-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|3.6818|1|1",
            "/ST_tW_antitop_5f_NoFullyHadronicDecays_13TeV-powheg_TuneCUETP8M1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|38.09|0.5135|1.0",
            # "/ST_tW_antitop_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|38.09|0.9346|1.0",
            "/ST_tW_top_5f_NoFullyHadronicDecays_13TeV-powheg_TuneCUETP8M1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|38.09|0.5135|1.0",
            # "/ST_tW_top_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|38.09|0.9346|1.0",
            "/ST_tWll_5f_LO_13TeV-MadGraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.01123|1.0|1.0",
            "/TTGJets_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|3.697|1.0|1.0",
            "/TGJets_leptonDecays_13TeV_amcatnlo_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|2.987|1.0|1.0",
            "/TTGamma_Dilept_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.632|1|1",
            "/TTGamma_SingleLeptFromT_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.77|1.0|1.0",
            "/TTGamma_SingleLeptFromTbar_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.769|1.0|1.0",
            "/TTJets_TuneCUETP8M2T4_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|831.76|1.0|1.0",
            "/TTJets_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|831.76|1.0|1.0",
            "/TTJets_DiLept_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|57.35|1.5225|1.0",
            # "/TTJets_DiLept_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|57.35|1.5225|1.0",
            # "/TTJets_Dilept_TuneCUETP8M2T4_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|57.35|1.5225|1",
            # "/TTJets_SingleLeptFromT_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|114.6|1.594|1.0",
            "/TTJets_SingleLeptFromT_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|114.6|1.594|1.0",
            # "/TTJets_SingleLeptFromTbar_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|114.6|1.594|1.0",
            "/TTJets_SingleLeptFromTbar_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|114.6|1.594|1.0",
            "/TTTT_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.009103|1|1",
            # "/TTTo2L2Nu_TuneCUETP8M2_ttHtranche3_13TeV-powheg-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|76.7|1.0|1.0",
            "/TTWJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v1/MINIAODSIM|0.2043|1.0|1.0",
            # "/TTWJetsToQQ_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.4062|1.0|1.0",
            "/TTZToLLNuNu_M-10_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v1/MINIAODSIM|0.2529|1|1",
            "/TTZToLL_M-1to10_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.0493|1|1",
            # "/TTZToQQ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.5297|1.0|1.0",
            "/TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|730.0|1.139397|1",
            # "/VHToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.952|1.0|1.0",
            # "/W1JetsToLNu_NuPt-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|2.128|1.11|1",
            # "/W2JetsToLNu_NuPt-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|3.721|1.33|1",
            # "/W3JetsToLNu_NuPt-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|3.248|1.52|1",
            # "/W4JetsToLNu_NuPt-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_backup_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|5.945|1.485|1",
            # "/W1JetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|9493.0|1.238|1.0",
            # "/W2JetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|3120.0|1.231|1.0",
            # "/W3JetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|942.3|1.231|1.0",
            # "/W4JetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v1/MINIAODSIM|524.2|1.144|1.0",
            "/WGToLNuG_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|489|1.0482618|1",
            "/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|1345|1.21|1.0",
            # "/WJetsToLNu_HT-1200To2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|1.329|1.21|1.0",
            # "/WJetsToLNu_HT-1200To2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|1.329|1.21|1.0",
            # "/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|359.7|1.21|1.0",
            # "/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|359.7|1.21|1.0",
            "/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v2/MINIAODSIM|359.7|1.21|1.0",
            # "/WJetsToLNu_HT-2500ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.03216|1.21|1.0",
            # "/WJetsToLNu_HT-2500ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|0.03216|1.21|1.0",
            # "/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|48.91|1.21|1.0",
            "/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|48.91|1.21|1.0",
            # "/WJetsToLNu_HT-600To800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|12.05|1.21|1.0",
            "/WJetsToLNu_HT-600To800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|12.05|1.21|1.0",
            # "/WJetsToLNu_HT-800To1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|5.501|1.21|1.0",
            "/WJetsToLNu_HT-800To1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|5.501|1.21|1.0",
            "/WJetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext2-v2/MINIAODSIM|50690.0|1.21|1.0",
            # "/WWTo1L1Nu2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|49.997|1|1",
            # "/WWToLNuQQ_13TeV-powheg/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|49.997|1|1",
            # "/WWTo2L2Nu_13TeV-powheg/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|12.178|1|1",
            "/WWTo2L2Nu_DoubleScattering_13TeV-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.16975|1.0|1.0",
            "/WWW_4F_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.2086|1.0|1.0",
            "/WWZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.1651|1.0|1.0",
            "/WZG_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.04123|1|1",
            "/WWG_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.2147|1|1",
            # "/WW_TuneCUETP8M1_13TeV-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|63.21|1.878|1.0",
            # "/WZTo1L1Nu2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|10.96|0.9781|1.0",
            # "/WZTo1L3Nu_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|3.05|1.00262|1",
            # "/WZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|5.52|1|1",
            # "/WZTo3LNu_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|4.4297|1|1",
            "/WZTo3LNu_TuneCUETP8M1_13TeV-powheg-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|4.4296|1|1",
            "/WZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.05565|1.0|1.0",
            # "/WZ_TuneCUETP8M1_13TeV-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|22.82|1.0|1.0",
            "/WpWpJJ_EWK-QCD_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.0539|1|1",
            "/ZGTo2LG_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|117.864|1.051212|1",
            # "/ZJetsToNuNu_HT-100To200_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|280.35|1.23|1.0",
            # "/ZJetsToNuNu_HT-100To200_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|280.35|1.23|1.0",
            # "/ZJetsToNuNu_HT-200To400_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|77.67|1.23|1.0",
            # "/ZJetsToNuNu_HT-200To400_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|77.67|1.23|1.0",
            # "/ZJetsToNuNu_HT-400To600_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|10.73|1.23|1.0",
            # "/ZJetsToNuNu_HT-400To600_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|10.73|1.23|1.0",
            # "/ZJetsToNuNu_HT-600To800_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|3.221|1.0|1.0",
            # "/ZJetsToNuNu_HT-800To1200_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1.474|1.0|1.0",
            # "/ZJetsToNuNu_HT-1200To2500_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.3586|1.0|1.0",
            # "/ZJetsToNuNu_HT-2500ToInf_13TeV-madgraph/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.008203|1.0|1.0",
            "/ZZTo4L_13TeV_powheg_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1.256|1|1",
            "/ZZTo4L_13TeV_powheg_pythia8_ext1/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|1.256|1|1",
            "/ZZTo2L2Q_13TeV_powheg_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|3.221|1|1",
            "/ZZTo2L2Nu_13TeV_powheg_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.5644|1|1",
            "/ZZTo2Q2Nu_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|4.7349|1|1",
            "/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.01398|1.0|1.0",
            "/tZq_ll_4f_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.0758|1.0|1.0",
            "/ttHToNonbb_M125_TuneCUETP8M2_ttHtranche3_13TeV-powheg-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.215|1.0|1.0",
            # "/ttZJets_13TeV_madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|0.7826|1|1",
            "/VHToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.952|1|1",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_pilot_94X_mcRun2_asymptotic_v3-v4/MINIAODSIM|1|1|1", # FIXME uncomment when phedex stops being a piece of shit

            "/TTWW_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.0115|1|1",
            "/TTWZ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.003884|1|1",
            "/TTZH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.001535|1|1",
            "/TTZZ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.001982|1|1",
            "/TTTJ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.000474|1|1",
            "/TTTW_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.000788|1|1",
            "/TTWH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.001582|1|1",
            "/TTHH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|0.000757|1|1",

            "/GluGluToContinToZZTo2e2mu_13TeV_MCFM701_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.00319142|1|1|skip50",
            "/GluGluToContinToZZTo2e2tau_13TeV_MCFM701_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.00319142|1|1|skip50",
            "/GluGluToContinToZZTo2mu2tau_13TeV_MCFM701_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.00319142|1|1|skip50",
            "/GluGluToContinToZZTo4e_13TeV_MCFM701_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.00158549|1|1|skip50",
            "/GluGluToContinToZZTo4mu_13TeV_MCFM701_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.00158549|1|1|skip50",
            "/GluGluToContinToZZTo4tau_13TeV_MCFM701_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|0.00158549|1|1|skip50",

            "/VBF_HToZZTo4L_M125_13TeV_powheg2_JHUGenV709_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|3.769|0.0002745|1.0",
            "/QCD_Pt-20toInf_MuEnrichedPt15_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|720648000.0|1.0|0.00042",
            "/QCD_Pt-20to30_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|558528000.0|1.0|0.0053",
            "/QCD_Pt-30to50_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|139803000.0|1.0|0.01182|skip95",
            "/QCD_Pt-50to80_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|19222500.0|1.0|0.02276",
            "/QCD_Pt-80to120_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|2758420.0|1.0|0.03844",
            "/QCD_Pt-120to170_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|469797.0|1.0|0.05362",
            "/QCD_Pt-300to470_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|7820.25|1.0|0.10196",
            "/QCD_Pt-470to600_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|645.528|1.0|0.12242",
            "/QCD_Pt-600to800_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|187.109|1.0|0.13412",
            "/QCD_Pt-1000toInf_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|10.4305|1.0|0.15544|skip90",

            "/QCD_Pt-20to30_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|557600000.0|1.0|0.0096",
            "/QCD_Pt-30to50_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|136000000.0|1.0|0.073",
            "/QCD_Pt-50to80_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|19800000.0|1.0|0.146",
            "/QCD_Pt-80to120_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|2800000.0|1.0|0.125",
            "/QCD_Pt-120to170_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v2/MINIAODSIM|477000.0|1.0|0.132",
            "/QCD_Pt-170to300_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|114000.0|1.0|0.165",
            "/QCD_Pt-300toInf_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|9000.0|1.0|0.15",

            "/QCD_Pt_15to20_bcToE_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|1272980000.0|1.0|0.0002",
            "/QCD_Pt_20to30_bcToE_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|557627000.0|1.0|0.00059",
            "/QCD_Pt_30to80_bcToE_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|159068000.0|1.0|0.00255",
            "/QCD_Pt_80to170_bcToE_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_backup_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|3221000.0|1.0|0.01183",
            "/QCD_Pt_170to250_bcToE_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|105771.0|1.0|0.02492",
            "/QCD_Pt_250toInf_bcToE_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|21094.1|1.0|0.03375",

            "/SMS-T1tbs_RPV_mGluino1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino1900_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1tbs_RPV_mGluino2100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1",
            # "/SMS-T1tbs_RPV_mGluino2200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1",  # FIXME uncomment when phedex stops being a piece of shit

            "/ST_FCNC-TH_Tleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2/MINIAODSIM|7.18|1|1",
            "/TT_FCNC-aTtoHJ_Tleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|9.6|1|1",
            "/TT_FCNC-TtoHJ_aTleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|9.6|1|1",

            # # Long-list of Long-lived
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1175_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2175_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2475_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1275_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1275_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1475_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1675_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2075_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2275_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1275_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1875_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2375_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2375_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1075_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1375_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2375_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2475_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1775_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2475_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-150_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2275_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-150_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2275_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1975_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2775_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1775_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2775_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1500_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1675_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2075_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2575_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1075_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1175_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1375_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1475_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1575_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2175_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-25_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1375_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1575_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1775_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1975_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2775_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # # new Apr 25
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2675_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1875_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1900_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2575_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-1900_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-2675_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-50_mLSP-800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # # new Apr 29
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-2600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-975_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-150_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1575_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1900_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-10_mLSP-1975_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T1qqqq-LLChipm_ctau-200_mLSP-2075_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T2qq-LLChipm_ctau-10_mLSP-1600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T2qq-LLChipm_ctau-200_mLSP-2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # # new Apr 25
            # "/SMS-T2qq-LLChipm_ctau-10_mLSP-2100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T2qq-LLChipm_ctau-10_mLSP-2175_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T2qq-LLChipm_ctau-50_mLSP-1400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # "/SMS-T2qq-LLChipm_ctau-10_mLSP-1275_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # # new Apr 29
            # "/SMS-T2qq-LLChipm_ctau-10_mLSP-2075_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUMoriond17_longlived_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|v08",
            # NOTE when adding LLP samples need to add v09 to get decayZ short track branch

            ]
    dinfos["mc_2016_94x_v3"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2016",
                special_dir = "run2_mc2016_94x/",
                global_tag = "94X_mcRun2_asymptotic_v3",
                tag = "CMS4_V10-02-05",
                tarfile = "{}/lib_CMS4_V10-02-05_1025.tar.xz".format(tarballdir),
                )
        }

    ########################################
    ######### MC 2016 94x Fastsim ##########
    ########################################
    infos = [
            "/TTJets_DiLept_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_lhe_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|54.29|1.609|1.0",
            "/TTJets_SingleLeptFromT_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_lhe_94X_mcRun2_asymptotic_v3-v1/MINIAODSI|109.1|1.677|1.0",
            "/TTJets_SingleLeptFromTbar_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_lhe_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|109.1|1.677|1.0",

            "/SMS-T1tttt_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T1bbbb_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2bb_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2bb_mSbot-1650to2600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_pileupfromucsd_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2qq_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2qq_mSq-1850to2600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2tt_mStop-1200to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T2tt_mStop-150to250_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T2tt_mStop-250to350_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T2tt_mStop-350to400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T2tt_mStop-400to1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T1qqqq_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T1qqqqL_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip80",
            "/SMS-T6ttHZ_BR-H_0p6_mStop300to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip75",
            "/SMS-T6ttHZ_BR-H_0p6_mStop1050to1600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip75",
            "/SMS-T1ttbb_deltaM5to25_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T6ttWW_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip85",
            "/SMS-T2bW_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1",
            "/SMS-T2bt_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1",
            "/SMS-T2cc_genHT-160_genMET-80_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1",
            "/RPV-monoPhi_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1",

            "/SMS-T5ttcc_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5ttcc_mGluino1750to2800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5qqqqVV_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5qqqqVV_dM20_mGlu-600to2300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5tttt_dM175_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T1ttbb_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM|1|1|1|skip90",

            ]
    dinfos["mc_2016_94x_v3_fastsim"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2016 fastsim=True",
                special_dir = "run2_mc2016_94x/",
                global_tag = "94X_mcRun2_asymptotic_v3",
                tag = "CMS4_V10-02-05",
                tarfile = "{}/lib_CMS4_V10-02-05_1025.tar.xz".format(tarballdir),
                )
        }

    ########################################
    ############## MC 2017 v2 ##############
    ########################################
    infos = [


            # # NOTE another issue -- [LHEWeights] Don't know what to do with alternate weight id = 1500(weightstype == 0) -- /home/users/namin/2017/ProjectMetis/tasks/CMSSWTask_ST_t-channel_antitop_4f_inclusiveDecays_TuneCP5_13TeV-powhegV2-madspin-pythia8_RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2_MINIAODSIM_CMS4_V10-02-04/logs/std_logs//1e.546292.2.out
            # Fixed in another commit after the one mentioned above
            "/ST_t-channel_antitop_4f_inclusiveDecays_TuneCP5_13TeV-powhegV2-madspin-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|80.95|1.0|1.0|singtop",
            "/ST_t-channel_top_4f_inclusiveDecays_TuneCP5_13TeV-powhegV2-madspin-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|136.02|1.0|1.0|singtop",

            "/TTToSemiLepton_HT500Njet9_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1.|1.0|1.0",

            "/DYJetsToLL_M-10to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|18610|1.11|1",
            "/DYJetsToLL_M-50_HT-100to200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|141.7|1.16|1.137",
            "/DYJetsToLL_M-50_HT-200to400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|42.91|1.16|1.133",
            "/DYJetsToLL_M-50_HT-400to600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|5.867|1.16|1.175",
            "/DYJetsToLL_M-50_HT-600to800_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|1.533|1.16|1.109",
            "/DYJetsToLL_M-50_HT-800to1200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.7350|1.16|0.994",
            "/DYJetsToLL_M-50_HT-1200to2500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.1574|1.16|1.023",
            "/DYJetsToLL_M-50_HT-2500toInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.003148|1.16|1.117",
            "/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|6198|1.0|1",
            "/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|6198|1.0|1",
            "/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017RECOSIMstep_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|5343.0|1.16|1",
            "/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017RECOSIMstep_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|5343.0|1.16|1",
             "/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|6198|1.0|1.0",
            "/GJets_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|9238|1.0|1.0",
            "/GJets_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|2305|1.0|1.0",
            "/GJets_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|274.4|1.0|1.0",
            "/GJets_HT-40To100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14_ext1-v2/MINIAODSIM|20790|1.0|1.0",
            "/GJets_HT-600ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|93.46|1.0|1.0",
            "/GJets_DR-0p4_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8_v2/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|5000|1.0|1.0",
            "/GJets_DR-0p4_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8_v2/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1079|1.0|1.0",
            "/GJets_DR-0p4_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8_v2/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|125.9|1.0|1.0",
            "/GJets_DR-0p4_HT-600ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|43.36|1.0|1.0",

            "/GluGluHToZZTo4L_M125_13TeV_powheg2_JHUGenV7011_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|29.99|0.0002745|1.0",
            "/GluGluHToZZTo4L_M125_13TeV_tunedown_powheg2_JHUGenV7011_pythia8/RunIIFall17MiniAOD-94X_mc2017_realistic_v10-v1/MINIAODSIM|29.99|0.0002745|1.0",
            "/GluGluHToZZTo4L_M125_13TeV_tuneup_powheg2_JHUGenV7011_pythia8/RunIIFall17MiniAOD-94X_mc2017_realistic_v10-v1/MINIAODSIM|29.99|0.0002745|1.0",

            "/GluGluToContinToZZTo2e2mu_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.00319142|1|1|skip50",
            "/GluGluToContinToZZTo2e2tau_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.00319142|1|1|skip50",
            "/GluGluToContinToZZTo2mu2tau_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.00319142|1|1|skip50",
            "/GluGluToContinToZZTo4e_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.00158549|1|1|skip50",
            "/GluGluToContinToZZTo4mu_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.00158549|1|1|skip50",
            "/GluGluToContinToZZTo4tau_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v2/MINIAODSIM|0.00158549|1|1|skip50",
            "/GluGluToContinToZZTo2e2nu_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.0190407|1.0|1.0",
            "/GluGluToContinToZZTo2mu2nu_13TeV_MCFM701_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.0190407|1.0|1.0",

            "/VBF_HToZZTo4L_M125_13TeV_powheg2_JHUGenV7011_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|3.769|0.0002745|1.0",
            "/VBF_HToZZTo4L_M125_13TeV_tunedown_powheg2_JHUGenV7011_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|3.769|0.0002745|1.0",
            "/VBF_HToZZTo4L_M125_13TeV_tuneup_powheg2_JHUGenV7011_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|3.769|0.0002745|1.0",

            # Note that the ZH cross sections for nominal vs tune are slightly different due to final state and filtering differences
            "/ZH_HToZZ_4LFilter_M125_13TeV_powheg2-minlo-HZJ_JHUGenV7011_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.75182|0.00074903|1.0",
            "/ZH_HToZZ_4LFilter_M125_13TeV_tunedown_powheg-minlo-HWJ_JHUgenV700_pythia8/RunIIFall17MiniAOD-94X_mc2017_realistic_v10-v1/MINIAODSIM|0.783960|0.00074903|1.0",
            "/ZH_HToZZ_4LFilter_M125_13TeV_tuneup_powheg-minlo-HWJ_JHUgenV700_pythia8/RunIIFall17MiniAOD-94X_mc2017_realistic_v10-v1/MINIAODSIM|0.783960|0.00074903|1.0",

            "/QCD_HT200to300_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1559000|1.0|1.0",
            "/QCD_HT300to500_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|323300|1.0|1.0",
            "/QCD_HT500to700_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|30000|1.0|1.0",
            "/QCD_HT700to1000_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|6330|1.0|1.0",
            "/QCD_HT1000to1500_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|1098|1.0|1.0",
            "/QCD_HT1500to2000_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|98.8|1.0|1.0",
            "/QCD_HT2000toInf_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|20.26|1.0|1.0",
            "/QCD_Pt-120to170_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|407300.0|1.0|0.1636",
            "/QCD_Pt-120to170_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|479500|1.0|0.04958",
            "/QCD_Pt-15to20_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|841200000.0|1.0|0.0016",
            "/QCD_Pt-15to20_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1267000000.0|1.0|0.002633",
            "/QCD_Pt-170to300_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|118100|1.0|0.07022",
            "/QCD_Pt-170to300_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAOD-94X_mc2017_realistic_v10-v1/MINIAODSIM|122700|1.0|0.17",
            "/QCD_Pt-20to30_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|399100000.0|1.0|0.0124",
            "/QCD_Pt-20to30_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|562700000.0|1.0|0.007087",
            "/QCD_Pt-300to470_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|7820.25|1.0|0.10196",
            "/QCD_Pt-300toInf_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|9000.0|1.0|0.15",
            "/QCD_Pt-30to50_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|107200000.0|1.0|0.059",
            "/QCD_Pt-30to50_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|139900000.0|1.0|0.01219",
            "/QCD_Pt-470to600_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|648.8|1.0|0.08722",
            "/QCD_Pt-50to80_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|15680000.0|1.0|0.1152",
            "/QCD_Pt-50to80_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|19400000|1.0|0.02037",
            "/QCD_Pt-600to800_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAOD-94X_mc2017_realistic_v10-v1/MINIAODSIM|187.109|1.0|0.13412",
            "/QCD_Pt-80to120_EMEnriched_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|2349000.0|1.0|0.162",
            "/QCD_Pt-80to120_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|2762000|1.0|0.0387",
            "/QCD_Pt_15to20_bcToE_TuneCP5_13TeV_pythia8/RunIIFall17MiniAOD-94X_mc2017_realistic_v11-v1/MINIAODSIM|849200000.0|1.0|0.000226",
            "/QCD_Pt_170to250_bcToE_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|105771.0|1.0|0.02492",
            "/QCD_Pt_20to30_bcToE_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|557627000.0|1.0|0.00059",
            "/QCD_Pt_250toInf_bcToE_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|21094.1|1.0|0.03375",
            "/QCD_Pt_30to80_bcToE_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|159068000.0|1.0|0.00255",
            "/QCD_Pt_80to170_bcToE_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|3221000.0|1.0|0.01183",
            "/QCD_Pt_80to120_TuneCP5_13TeV_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|2762530|1|1|skip75",
            "/ST_s-channel_4f_leptonDecays_TuneCP5_PSweights_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|3.74|1.0|1.0",
            "/ST_tW_antitop_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|34.91|1.0|0.53",
            "/ST_tW_antitop_5f_inclusiveDecays_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|38.09|0.9346|1",
            "/ST_tW_top_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|34.91|1.0|0.58",
            "/ST_tW_top_5f_inclusiveDecays_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|38.09|0.9346|1",
            "/ST_tWll_5f_LO_TuneCP5_PSweights_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|0.01123|1|1",
            "/TGJets_leptonDecays_TuneCP5_PSweights_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|2.987|1|1",
            "/TTGJets_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|3.697|1.0|1.0",
            "/TTGamma_Dilept_TuneCP5_PSweights_13TeV_madgraph_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.632|1|1",
            "/TTGamma_SingleLeptFromT_TuneCP5_PSweights_13TeV_madgraph_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.769|1|1",
            "/TTGamma_SingleLeptFromTbar_TuneCP5_PSweights_13TeV_madgraph_pythia8/RunIIFall17MiniAOD-PU2017_94X_mc2017_realistic_v11-v1/MINIAODSIM|0.769|1|1",
            "/TTHH_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.000757|1|1",
            "/TTJets_DiLept_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|54.29|1.609|1.0",
            "/TTJets_SingleLeptFromTbar_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|109.1|1.677|1.0",
            "/TTJets_SingleLeptFromT_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|109.1|1.677|1.0",
            "/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|831.76|1|1",
            "/TTJets_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|831.76|1.0|1.0",
            "/TTPlus1Jet_DiLept_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|81.32|1|1",
            "/TTPlus1Jet_DiLept_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|81.32|1|1",
            "/TTTJ_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.000474|1|1",
            "/TTTT_TuneCP5_PSweights_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.009103|1|1",
            "/TTTW_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.000788|1|1",
            "/TTTo2L2Nu_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|88.29|1.0|1.0",
            "/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|88.29|1.0|1.0",
            "/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|365.34|1.0|1.0",
            "/TTToSemiLeptonic_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|229.2|1.594|1",
            "/TTWH_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.001582|1|1",
            "/TTWJetsToLNu_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.2043|1|1",
            "/TTWJetsToLNu_TuneCP5_PSweights_13TeV-amcatnloFXFX-madspin-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.2043|1|1",
            "/TTWJetsToQQ_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.4026|1.0|1.0",
            "/TTWW_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|0.0115|1|1",
            "/TTWZ_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.003884|1|1",
            "/TTZH_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.001535|1|1",
            "/TTZToLLNuNu_M-10_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.2529|1|1",
            "/TTZToLLNuNu_M-10_TuneCP5_PSweights_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.2529|1|1",
            "/TTZToLL_M-1to10_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.0493|1|1",
            "/TTZToQQ_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.5297|1.0|1.0",
            "/TTZZ_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.001982|1|1",
            "/TT_DiLept_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|72.48|1|1",
            "/TT_DiLept_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|72.48|1|1",
            "/VHToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.952|1|1",
            "/W1JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|2.128|1.11|1",
            "/W2JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|3.721|1.33|1",
            "/W3JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|3.248|1.52|1",
            "/W4JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|5.945|1.485|1",
            "/W1JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v3/MINIAODSIM|9493|1.238|1",
            "/W2JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v4/MINIAODSIM|3120.0|1.231|1",
            "/W3JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|942.3|1.231|1",
            "/W4JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|524.2|1.144|1",
            "/WGToLNuG_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|405.271|1.0|1.0",
            "/WJetsToLNu_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|1379|1.21|1.005",
            "/WJetsToLNu_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|359.7|1.21|1.136",
            "/WJetsToLNu_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|48.91|1.21|1.186",
            "/WJetsToLNu_HT-600To800_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|12.05|1.21|1.196",
            "/WJetsToLNu_HT-800To1200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|5.501|1.21|1.173",
            "/WJetsToLNu_HT-1200To2500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1.329|1.21|1.077",
            "/WJetsToLNu_HT-2500ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v3/MINIAODSIM|0.03216|1.21|1.044",
            "/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v2/MINIAODSIM|52940|1.21|1.0",
            "/WWG_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.2147|1|1",
            "/WWTo1L1Nu2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|49.997|1|1",
            "/WWTo2L2Nu_NNPDF31_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|12.178|1|1",
            "/WWToLNuQQ_NNPDF31_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|49.997|1|1",
            "/WWToLNuQQ_NNPDF31_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|49.997|1|1",
            "/WWW_4F_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.2086|1|1",
            "/WWW_4F_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.2086|1|1",
            "/WWZ_4F_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.1651|1|1",
            "/WWZJetsTo4L2Nu_4f_TuneCP5_13TeV_amcatnloFXFX_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|5.249605e-4|1.0|1.0",
            "/WW_DoubleScattering_13TeV-pythia8_TuneCP5/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1.64|0.986|1",
            "/WW_TuneCP5_13TeV-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|63.21|1.878|1",
            "/WZG_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.04123|1|1",
            "/WZTo1L1Nu2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|10.72|1|1",
            "/WZTo1L3Nu_13TeV_amcatnloFXFX_madspin_pythia8_v2/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|3.058|1|1",
            "/WZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|5.595|1|1",
            "/WZTo3LNu_0Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|2.76132|1|1",
            "/WZTo3LNu_0Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v3/MINIAODSIM|0.692472|1|1",
            "/WZTo3LNu_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|4.4297|1|1",
            "/WZTo3LNu_1Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.63492|1|1",
            "/WZTo3LNu_1Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.413468|1|1",
            "/WZTo3LNu_2Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.098352|1|1",
            "/WZTo3LNu_2Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.092208|1|1",
            "/WZTo3LNu_3Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.113784|1|1",
            "/WZTo3LNu_3Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|0.13344|1|1",
            "/WZTo3LNu_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|4.4297|1|1",
            "/WZZ_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.05565|1|1",
            "/WZ_TuneCP5_13TeV-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|22.82|1|1",
            "/WpWpJJ_EWK-QCD_TuneCP5_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.0539|1|1",
            "/ZGToLLG_01J_5f_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v3/MINIAODSIM|117.864|1.051212|1",
            "/ZJetsToNuNu_HT-100To200_13TeV-madgraph/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|280.35|1.23|1.073",
            "/ZJetsToNuNu_HT-200To400_13TeV-madgraph/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|77.67|1.23|1.169",
            "/ZJetsToNuNu_HT-400To600_13TeV-madgraph/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|10.73|1.23|1.2",
            "/ZJetsToNuNu_HT-600To800_13TeV-madgraph/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|2.619|1.23|1.212",
            "/ZJetsToNuNu_HT-800To1200_13TeV-madgraph/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1.198|1.23|1.139",
            "/ZJetsToNuNu_HT-1200To2500_13TeV-madgraph/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.2915|1.23|1.032",
            "/ZJetsToNuNu_HT-2500ToInf_13TeV-madgraph/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.006669|1.23|0.985",
            "/ZZTo2L2Nu_13TeV_powheg_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.5644|1|1",
            "/ZZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|3.221|1|1",
            "/ZZTo2Q2Nu_TuneCP5_13TeV_amcatnloFXFX_madspin_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|4.7349|1|1",
            "/ZZTo4L_13TeV_powheg_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1.256|1|1",
            "/ZZTo4L_13TeV_powheg_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v1/MINIAODSIM|1.256|1|1",
            "/ZZZ_TuneCP5_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.01398|1|1",
            "/tHW_HToTT_0J_mH-350_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-375_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-425_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-450_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-475_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-525_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-550_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-575_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-625_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-650_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-350_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-375_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-425_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-450_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-475_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-525_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-550_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-575_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-625_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-650_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/tZq_ll_4f_ckm_NLO_TuneCP5_PSweights_13TeV-amcatnlo-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.0758|1|1",
            "/ttbb_4FS_ckm_NNPDF31_TuneCP5_amcatnlo_madspin_pythia/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|1|1|1",
            "/ttHToNonbb_M125_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.215|1|1",
            "/ttH_HToTT_1J_mH-350_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-375_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-425_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-450_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-475_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017RECOPF_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-525_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-550_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-575_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-625_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-650_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/ttWJets_TuneCP5_13TeV_madgraphMLM_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.6105|1|1",
            "/ttWJets_TuneCP5_13TeV_madgraphMLM_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.6105|1.0|1.0",
            "/ttZJets_TuneCP5_13TeV_madgraphMLM_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|0.7826|1.0|1.0",
            "/ttZJets_TuneCP5_13TeV_madgraphMLM_pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14_ext1-v3/MINIAODSIM|0.7826|1.0|1.0",
            "/SMS-T1tbs_RPV_mGluino1000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1100_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1300_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1400_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1700_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1800_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1900_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino2000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino2100_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino2200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1|rpv",

            # "/SMS-T1tttt_mGluino-1200_mLSP-800_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-1200_mLSP-100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-650_mLSP-350_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-850_mLSP-100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|1|1|1",

            "/ST_FCNC-TH_Tleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|7.18|1|1",
            "/TT_FCNC-aTtoHJ_Tleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|9.6|1|1",
            "/TT_FCNC-TtoHJ_aTleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|9.6|1|1",
        ]
    dinfos["mc_2017_94x_v2"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2017 metrecipe=True",
                special_dir = "run2_mc2017/",
                global_tag = "94X_mc2017_realistic_v17",
                tag = "CMS4_V10-02-05",
                tarfile = "{}/lib_CMS4_V10-02-05_1025.tar.xz".format(tarballdir),
                events_per_output = 250e3,
                )
        }

    ########################################
    ########## MC 2017 v2 Fastsim ##########
    ########################################
    infos = [

            "/TTJets_DiLept_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_lhe_94X_mc2017_realistic_v15-v1/MINIAODSIM|54.29|1.609|1.0",
            "/TTJets_SingleLeptFromT_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_lhe_94X_mc2017_realistic_v15-v1/MINIAODSIM|109.1|1.677|1.0",
            "/TTJets_SingleLeptFromTbar_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_lhe_94X_mc2017_realistic_v15-v1/MINIAODSIM|109.1|1.677|1.0",
            "/SMS-T1tttt_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_pilot_94X_mc2017_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T1tttt_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_pilot_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1ttbb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T1bbbb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T1bbbb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2bb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2bb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2bb_mSbot-1650to2600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2bb_mSbot-1650to2600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_pileupfromucsd_94X_mc2017_realistic_v15-v2/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2qq_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2qq_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2qq_mSq-1850to2600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2qq_mSq-1850to2600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T6ttWW_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip85",
            "/SMS-T6ttWW_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip85",
            "/SMS-T2tt_mStop-1200to2000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5tttt_dM175_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5tttt_dM175_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5qqqqVV_dM20_mGlu-600to2300_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5qqqqVV_dM20_mGlu-600to2300_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5qqqqVV_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T5qqqqVV_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2tt_mStop-150to250_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2tt_mStop-250to350_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2tt_mStop-350to400_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2tt_mStop-400to1200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2tt_mStop-1200to2000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T2tt_mStop-150to250_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-250to350_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-350to400_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-400to1200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1",
            "/SMS-T1qqqq_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T1qqqq_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90",
            "/SMS-T6ttHZ_BR-H_0p6_mStop1050to1600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip75",
            "/SMS-T6ttHZ_BR-H_0p6_mStop300to1000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip75",
            "/SMS-T6ttHZ_BR-H_0p6_mStop300to1000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip75",
            "/SMS-T6ttHZ_BR-H_0p6_mStop1050to1600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip75",
            "/SMS-T1qqqqL_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip80",
            "/SMS-T1qqqqL_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip75",
            "/SMS-T2bW_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/SMS-T2bt_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/SMS-T2cc_genHT-160_genMET-80_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/SMS-T5ttcc_mGluino1750to2800_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95",
            "/SMS-T5ttcc_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95",
            "/RPV-monoPhi_TuneCP2_13TeV-madgraph-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1",

            "/SMS-T2qq-LLChipm_ctau-10_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v08",
            "/SMS-T2qq-LLChipm_ctau-50_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v08",
            "/SMS-T2qq-LLChipm_ctau-50_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95,v08",
            "/SMS-T2qq-LLChipm_ctau-200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v08",
            "/SMS-T2qq-LLChipm_ctau-200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95,v08",
            "/SMS-T2bt-LLChipm_ctau-10_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v08",
            "/SMS-T2bt-LLChipm_ctau-50_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90,v08",
            "/SMS-T2bt-LLChipm_ctau-50_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95,v08",
            "/SMS-T2bt-LLChipm_ctau-200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip90,v08",

            "/SMS-T2bt-LLChipm_ctau-50_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95,v09",
            "/SMS-T2bt-LLChipm_ctau-50_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v09",
            "/SMS-T2bt-LLChipm_ctau-200_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v09",

            "/SMS-T2bt-LLChipm_ctau-200_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95,v09",
            "/SMS-T2bt-LLChipm_ctau-10_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95,v09",
            "/SMS-T2bt-LLChipm_ctau-10_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v09",
            # NOTE when adding LLP samples need to add v09 to get decayZ short track branch

            "/SMS-T1qqqq-LLChipm_ctau-10_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v09",
            "/SMS-T1qqqq-LLChipm_ctau-50_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip90,v09",
            "/SMS-T1qqqq-LLChipm_ctau-200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15_ext1-v1/MINIAODSIM|1|1|1|skip95,v09",
            "/SMS-T1qqqq-LLChipm_ctau-200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PUFall17Fast_94X_mc2017_realistic_v15-v1/MINIAODSIM|1|1|1|skip95,v09",


            ]
    dinfos["mc_2017_94x_v2_fastsim"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2017 metrecipe=True fastsim=True",
                special_dir = "run2_mc2017/",
                global_tag = "94X_mc2017_realistic_v17",
                tag = "CMS4_V10-02-05",
                tarfile = "{}/lib_CMS4_V10-02-05_1025.tar.xz".format(tarballdir),
                events_per_output = 200e3,
                )
        }

    infos = [
             "/VHToWW_M125_13TeV_amcatnloFXFX_madspin_pythia8/PRIVATE_RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-ext1-v2/MINIAODSIM|/hadoop/cms/store/user/phchang/metis/private_miniaod/VHToWW_M125_13TeV_amcatnloFXFX_madspin_pythia8_PRIVATE_RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-ext1-v2_MINIAODSIM_/|94X_mc2017_realistic_v14",
             "/WWW_4F_TuneCP5_13TeV-amcatnlo-pythia8/PRIVATE_RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM|/hadoop/cms/store/user/phchang/metis/private_miniaod/WWW_4F_TuneCP5_13TeV-amcatnlo-pythia8_PRIVATE_RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1_MINIAODSIM_/|94X_mc2017_realistic_v14",
        ]

    dinfos["mc_2017_94x_v2_private"] = {
            "samples": infos,
            "params": dict(
                files_per_output = 60,
                condor_submit_params = {"sites":"T2_US_UCSD"},
                snt_dir = False,
                )
        }
    dinfos["mc_2017_94x_v2_private"]["params"].update(dinfos["mc_2017_94x_v2"]["params"])

    ########################################
    ############## MC 2018 v1 ##############
    ########################################
    infos = [

            "/DYJetsToLL_M-10to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|18610.0|1.11|1.0",
            "/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|6198|1.0|1",
            "/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|5343|1.16|1.0",
            "/DYJetsToLL_M-50_Zpt-150toInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|202.9|1|1",
            "/DYJetsToLL_M-50_HT-100to200_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|141.7|1.16|1.137",
            "/DYJetsToLL_M-50_HT-200to400_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|42.91|1.16|1.133",
            "/DYJetsToLL_M-50_HT-400to600_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v3/MINIAODSIM|5.867|1.16|1.175",
            "/DYJetsToLL_M-50_HT-600to800_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1.533|1.16|1.109",
            "/DYJetsToLL_M-50_HT-800to1200_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.735|1.16|0.994",
            "/DYJetsToLL_M-50_HT-1200to2500_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.1574|1.16|1.023",
            "/DYJetsToLL_M-50_HT-2500toInf_TuneCP5_PSweights_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.003148|1.16|1.117",
            "/GJets_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-4cores5k_102X_upgrade2018_realistic_v15-v1/MINIAODSIM|9238|1.0|1.0",
            "/GJets_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|2305|1.0|1.0",
            "/GJets_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|274.4|1.0|1.0",
            "/GJets_HT-40To100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|20790|1.0|1.0",
            "/GJets_HT-600ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|93.46|1.0|1.0",
            "/GluGluToContinToZZTo2e2mu_13TeV_MCFM701_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.00319|1.7|1|skip50",
            "/GluGluToContinToZZTo2e2tau_13TeV_MCFM701_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.00319|1.7|1|skip50",
            "/GluGluToContinToZZTo2mu2tau_13TeV_MCFM701_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.00319|1.7|1|skip50",
            "/GluGluToContinToZZTo4e_13TeV_MCFM701_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.00159|1.7|1|skip50",
            "/GluGluToContinToZZTo4mu_13TeV_MCFM701_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.00159|1.7|1|skip50",
            "/GluGluToContinToZZTo4tau_13TeV_MCFM701_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.00159|1.7|1|skip50",
            "/GluGluHToZZTo4L_M125_13TeV_powheg2_JHUGenV7011_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|43.92|0.000269|1.0|skip99",
            "/QCD_HT100to200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|19380000|1|1",
            "/QCD_HT200to300_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1559000|1.0|1.0",
            "/QCD_HT300to500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|323300|1.0|1.0",
            "/QCD_HT500to700_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|30000|1.0|1.0",
            "/QCD_HT700to1000_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|6330|1.0|1.0",
            "/QCD_HT1000to1500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1098|1.0|1.0",
            "/QCD_HT1500to2000_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|98.9|1.0|1.0",
            "/QCD_HT2000toInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|20.26|1.0|1.0",
            "/QCD_Pt-15to20_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1267000000.0|1.0|0.002633",
            "/QCD_Pt-120to170_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|407300|1.0|0.1636",
            "/QCD_Pt-120to170_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|479500.0|1.0|0.04958",
            "/QCD_Pt-120to170_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|410800.0|1.0|0.04773",
            "/QCD_Pt-15to20_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|841200000.0|1|0.0016",
            "/QCD_Pt-170to300_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|122700|1.0|0.17",
            "/QCD_Pt-170to300_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v3/MINIAODSIM|103500|0.06962|1",
            "/QCD_Pt-20to30_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|399100000|1.0|0.0124",
            "/QCD_Pt-20to30_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v4/MINIAODSIM|562700000.0|1.0|0.007087",
            "/QCD_Pt-300to470_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v3/MINIAODSIM|6834|1.0|0.09182",
            "/QCD_Pt-300toInf_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|9000|1.0|0.15",
            "/QCD_Pt-30to50_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|107200000.0|1.0|0.059",
            "/QCD_Pt-30to50_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v3/MINIAODSIM|106900000.0|1.0|0.0123",
            "/QCD_Pt-470to600_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|547.3|1.0|0.1035",
            "/QCD_Pt-50to80_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|15680000|1.0|0.1152",
            "/QCD_Pt-50to80_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v3/MINIAODSIM|15710000|0.02324|1",
            "/QCD_Pt-600to800_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|187.109|1.0|0.13412",
            "/QCD_Pt-80to120_EMEnriched_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|2349000|1.0|0.162",
            "/QCD_Pt-80to120_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|2335000|1.0|0.03513",
            "/QCD_Pt-80to120_MuEnrichedPt5_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|2335000|1|0.03513",
            "/QCD_Pt_80to120_TuneCP5_13TeV_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|2762530|1|1|skip75",
            "/ST_tW_antitop_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v3/MINIAODSIM|34.91|1.0|0.53",
            "/ST_tW_top_5f_NoFullyHadronicDecays_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|34.97|1.0|1.0",
            "/ST_tW_top_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v3/MINIAODSIM|34.91|1.0|0.58",
            "/ST_tW_top_5f_NoFullyHadronicDecays_TuneCP5_PSweights_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|34.91|1.0|1.0",
            "/ST_tWll_5f_LO_TuneCP5_PSweights_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v1/MINIAODSIM|0.01123|1.0|1.0",
            "/ST_s-channel_4f_leptonDecays_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v4/MINIAODSIM|3.74|1.0|1.0|singtop", # FIXME need updated cms4 tag for LHEweights error?
            "/ST_t-channel_antitop_4f_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|80.95|1.0|1.0|singtop", # FIXME need updated cms4 tag for LHEweights error?
            "/ST_t-channel_top_4f_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|136.02|1.0|1.0|singtop", # FIXME need updated cms4 tag for LHEweights error?
            "/TGJets_leptonDecays_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|2.987|1|1",
            "/TTGJets_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|3.697|1.0|1.0",
            "/TTGamma_Dilept_TuneCP5_13TeV_madgraph_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.632|1.0|1.0",
            "/TTGamma_SingleLeptFromT_TuneCP5_13TeV_madgraph_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.769|1.0|1.0",
            "/TTGamma_SingleLeptFromTbar_TuneCP5_13TeV_madgraph_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.769|1.0|1.0",
            "/TTHH_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.000757|1.0|1.0",
            "/TTJets_DiLept_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|54.29|1.609|1.0",
            "/TTJets_SingleLeptFromT_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|109.1|1.677|1.0",
            "/TTJets_SingleLeptFromTbar_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|109.1|1.677|1.0",
            "/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|831.76|1.0|1.0",
            "/TTPlus1Jet_DiLept_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|81.32|1.0|1.0",
            "/TTTJ_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.000474|1.0|1.0",
            "/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.009103|1.0|1.0|ttttnew",
            "/TTTW_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.000788|1.0|1.0",
            "/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|72.1|1.0|1.0",
            "/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|365.34|1.0|1.0",
            "/TTWH_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.001582|1.0|1.0",
            "/TTWJetsToLNu_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.2043|1.0|1.0",
            "/TTWW_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.0115|1.0|1.0",
            "/TTWZ_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.003884|1.0|1.0",
            "/TTZH_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.001535|1.0|1.0",
            "/TTZToLLNuNu_M-10_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.2529|1|1",
            "/TTZToLL_M-1to10_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.0493|1.0|1.0",
            # "/TTZToBB_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.38|1.0|1.0|skip90", # Nobody even needs this sample
            "/TTZZ_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.001982|1|1",
            "/TT_DiLept_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|72.48|1|1",
            "/TTTo2L2Nu_HT500Njet7_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/TTToSemiLepton_HT500Njet9_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttZJets_TuneCP5_13TeV_madgraphMLM_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.7826|1.0|1.0",
            "/ttWJets_TuneCP5_13TeV_madgraphMLM_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.6105|1.0|1.0",
            "/VHToNonbb_M125_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.952|1|1",
            "/VHToNonbb_M125_DiLeptonFilter_TuneCP5_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.952|1|1",
            "/W1JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|2.128|1.11|1",
            "/W2JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|3.721|1.33|1",
            "/W3JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|3.248|1.52|1",
            "/W4JetsToLNu_NuPt-200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|5.945|1.485|1",
            "/W1JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|9493|1.238|1",
            "/W2JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|3120.0|1.231|1",
            "/W3JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|942.3|1.231|1.0",
            "/W4JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|524.2|1.144|1.0",
            "/WGToLNuG_01J_5f_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v1/MINIAODSIM|405.271|1.0|1.0",
            "/WGToLNuG_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|405.271|1.0|1.0",
            "/WJetsToLNu_HT-100To200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1379|1.21|0.961",
            "/WJetsToLNu_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|359.7|1.21|1.089",
            "/WJetsToLNu_HT-400To600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|48.91|1.21|1.138",
            "/WJetsToLNu_HT-600To800_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|12.05|1.21|1.161",
            "/WJetsToLNu_HT-800To1200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|5.501|1.21|1.119",
            "/WJetsToLNu_HT-1200To2500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1.329|1.21|1.118",
            "/WJetsToLNu_HT-2500ToInf_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.03216|1.21|1.0",
            "/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|50690|1.21|1",
            "/WWG_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.2147|1.0|1.0",
            "/WWW_4F_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.2086|1.0|1.0",
            "/WWW_4F_DiLeptonFilter_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.2086|1.0|1.0",
            "/WWZ_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.1651|1|1",
            "/WWZJetsTo4L2Nu_4f_TuneCP5_13TeV_amcatnloFXFX_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|5.249605e-4|1.0|1.0",
            "/WW_DoubleScattering_13TeV-pythia8_TuneCP5/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v1/MINIAODSIM|1.64|0.986|1.0",
            "/WW_TuneCP5_13TeV-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|63.21|1.878|1.0",
            "/WWTo2L2Nu_NNPDF31_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|12.178|1|1",
            "/WWToLNuQQ_NNPDF31_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|49.997|1|1",
            "/WZG_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.04123|1.0|1.0",
            "/WZTo3LNu_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|4.4297|1.0|1.0",
            "/WZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|5.52|1|1",
            "/WZTo1L3Nu_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|3.05|1.00262|1",
            # "/WZTo3LNu_0Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|2.76132|1|1",
            # "/WZTo3LNu_0Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.692472|1|1",
            # "/WZTo3LNu_1Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.63492|1|1",
            # "/WZTo3LNu_1Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.413468|1|1",
            # "/WZTo3LNu_2Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.098352|1|1",
            # "/WZTo3LNu_2Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.092208|1|1",
            # "/WZTo3LNu_3Jets_MLL-4to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.113784|1|1",
            # "/WZTo3LNu_3Jets_MLL-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.13344|1|1",
            # "/WZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|5.52|1|1",
            # "/WZTo1L3Nu_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|3.05|1.00262|1",
            "/WZZ_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.05565|1.0|1.0",
            "/WZ_TuneCP5_13TeV-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v3/MINIAODSIM|22.82|1.0|1.0",
            "/WpWpJJ_EWK-QCD_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.0539|1|1",
            "/ZGToLLG_01J_5f_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|117.864|1|1",
            "/ZJetsToNuNu_HT-100To200_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|280.35|1.23|1.073",
            "/ZJetsToNuNu_HT-200To400_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|77.67|1.23|1.169",
            "/ZJetsToNuNu_HT-400To600_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|10.73|1.23|1.2|skip95",
            "/ZJetsToNuNu_HT-600To800_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|2.619|1.23|1.212",
            "/ZJetsToNuNu_HT-800To1200_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1.198|1.23|1.139",
            "/ZJetsToNuNu_HT-1200To2500_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.2915|1.23|1.032",
            "/ZJetsToNuNu_HT-2500ToInf_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|0.006669|1.23|0.985",
            "/ZZTo4L_TuneCP5_13TeV_powheg_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext2-v2/MINIAODSIM|1.256|1|1",
            "/ZZTo2L2Nu_TuneCP5_13TeV_powheg_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.5644|1|1",
            "/ZZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|3.221|1|1",
            "/ZZTo2Q2Nu_TuneCP5_13TeV_amcatnloFXFX_madspin_pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|4.7349|1|1",
            "/ZZZ_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.01398|1.0|1.0",
            "/ZZ_TuneCP5_13TeV-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|10.32|1|1",
            "/tZq_ll_4f_ckm_NLO_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM|0.0758|1|1",
            "/ttHToNonbb_M125_TuneCP5_13TeV-powheg-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2/MINIAODSIM|0.215|1.0|1.0",

            "/ttH_HToTT_1J_mH-375_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-425_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-450_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-475_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-500_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-525_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-550_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-575_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-600_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-625_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/ttH_HToTT_1J_mH-650_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-350_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-375_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-400_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-425_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-450_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-475_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            # "/tHW_HToTT_0J_mH-500_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-525_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-550_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-575_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-600_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-625_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHW_HToTT_0J_mH-650_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-350_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            # "/tHq_HToTT_0J_mH-375_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-400_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-425_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-450_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-475_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-500_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-525_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-550_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-575_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-600_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-625_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",
            "/tHq_HToTT_0J_mH-650_TuneCP5_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",

            "/SMS-T1tbs_RPV_mGluino1000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv,skip70",
            "/SMS-T1tbs_RPV_mGluino1100_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1300_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1400_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv,skip50",
            "/SMS-T1tbs_RPV_mGluino1500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1700_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1800_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino1900_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino2000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino2100_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",
            "/SMS-T1tbs_RPV_mGluino2200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1|rpv",

        ]
    dinfos["mc_2018_102x_v1"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2018",
                special_dir = "run2_mc2018/",
                global_tag = "102X_upgrade2018_realistic_v12",
                )
        }

    ########################################
    ########## MC 2018 Fastsim V1 ##########
    ########################################
    infos = [

            "/TTJets_DiLept_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_lhe_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|54.29|1.609|1.0",
            "/TTJets_SingleLeptFromT_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_lhe_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|109.1|1.677|1.0",
            "/TTJets_SingleLeptFromTbar_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_lhe_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|109.1|1.677|1.0",
            "/SMS-T1tttt_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_pilot_102X_upgrade2018_realistic_v15-v1/MINIAODSIM|1|1|1",

            "/RPV-monoPhi_TuneCP2_13TeV-madgraph-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",           
            # "/SMS-T1bbbb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T1qqqqL_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T1qqqq_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            # "/SMS-T1ttbb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            # "/SMS-T1tttt_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T2bW_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T2bb_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T2bb_mSbot-1650to2600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T2bt_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T2qq_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T2qq_mSq-1850to2600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-1200to2000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-150to250_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-250to350_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            # "/SMS-T2tt_mStop-350to400_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T2tt_mStop-400to1200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T5qqqqVV_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T5qqqqVV_dM20_mGlu-600to2300_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T5ttcc_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",
            "/SMS-T5ttcc_mGluino1750to2800_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T5tttt_dM175_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            # "/SMS-T6ttHZ_BR-H_0p6_mStop1050to1600_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            "/SMS-T6ttHZ_BR-H_0p6_mStop300to1000_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v2/MINIAODSIM|1|1|1",
            # "/SMS-T6ttWW_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1",

            # NOTE when adding LLP samples need to add v09 to get decayZ short track branch

            ]
    dinfos["mc_2018_102x_v1_fastsim"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2018 fastsim=True",
                special_dir = "run2_mc2018/",
                global_tag = "102X_upgrade2018_realistic_v12",
                tag = "CMS4_V10-02-08",
                tarfile = "{}/lib_CMS4_V10-02-08_1025.tar.xz".format(tarballdir),
                events_per_output = 200e3,
                )
        }

    ########################################
    ########## MC 2018 Fastsim V3 ##########
    ########################################
    infos = [
            # "/SMS-T2bt-LLChipm_ctau-50_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1|v08",
            # # New Apr 25
            # "/SMS-T2bt-LLChipm_ctau-10_mStop-1550to2500_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1|v08",
            # # New Apr 29
            # "/SMS-T2bt-LLChipm_ctau-200_TuneCP2_13TeV-madgraphMLM-pythia8/RunIIAutumn18MiniAOD-PUFall18Fast_102X_upgrade2018_realistic_v15-v3/MINIAODSIM|1|1|1|v08",
            # NOTE when adding LLP samples need to add v09 to get decayZ short track branch
            ]
    dinfos["mc_2018_102x_v3_fastsim"] = {
            "samples": infos,
            "params": dict(
                pset_args = "data=False year=2018 fastsim=True",
                special_dir = "run2_mc2018/",
                global_tag = "102X_upgrade2018_realistic_v15",
                tag = "CMS4_V10-02-08",
                tarfile = "{}/lib_CMS4_V10-02-08_1025.tar.xz".format(tarballdir),
                events_per_output = 200e3,
                )
        }

    return dinfos

if __name__ == "__main__":


    dinfos = get_master_list()

    # Print total number of samples
    # pprint(dinfos)
    print sum(len(x["samples"]) for x in dinfos.values())
    # sys.exit()

    # # reduce all campaigns to one dataset for testing:
    # for k in dinfos.keys():
    #     dinfos[k]["samples"] = sorted(dinfos[k]["samples"])[:1]
    # pprint(dinfos)
    # sys.exit()

    global_params = dict(
            scram_arch = "slc6_amd64_gcc700",
            cmssw_version = "CMSSW_10_2_5",
            tag = "CMS4_V10-02-04",
            pset = "/home/users/namin/2017/ProjectMetis/pset_CMS4_V10-02-04.py",
            tarfile = "{}/lib_CMS4_V10-02-04_1025.tar.xz".format(tarballdir),
            publish_to_dis = True,
            snt_dir = True,
            # additional_input_files = ["/home/users/namin/2017/ProjectMetis/metis/executables/condor_chirp"],
            events_per_output = 300e3,
            output_name = "merged_ntuple.root",
            # condor_submit_params = {"use_xrootd":True},
            )

    # dinfos = {}
    # dinfos["mc_2017_94x_v2"] = {
    #         "samples": [
    #         "/ST_tW_antitop_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|34.91|1.0|0.53",
    #         "/ST_tW_top_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v1/MINIAODSIM|34.91|1.0|0.58",
    #         ],
    #         "params": dict(
    #             pset_args = "data=False year=2017 metrecipe=True",
    #             special_dir = "run2_mc2017/",
    #             global_tag = "94X_mc2017_realistic_v17",
    #             )
    #     }

    special = []

    # force process these guys if needed to update xsecs/metadata
    # so we can bypass the "if not complete" check and dump metadata.json
    special = map(lambda x:x.split("|")[0],[

            # "/DYJetsToLL_M-50_HT-100to200_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_new_pmx_94X_mc2017_realistic_v14-v2/MINIAODSIM|141.7|1.16|1.137",
            # "/DYJetsToLL_M-50_HT-200to400_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM|42.91|1.16|1.133",

            ])


    optimizer = Optimizer()
    # Loop 'til we're dizzy
    for i in range(10000):

        total_summary = {}
        # Loop over all campaigns
        # for campaign in ["data_2018_102x_rereco1"]:
        for campaign in dinfos.keys():

            # Update global parameter dict with campaign-specific stuff
            info = dinfos[campaign]
            params = dict(global_params)
            params.update(info["params"])

            # Make all the tasks for a campaign
            tasks = []
            for sampstr in info["samples"]:
                task_params = dict(params)
                if "PRIVATE" in sampstr:
                    dsname = sampstr.split("|")[0].strip()
                    location = sampstr.split("|")[1].strip()
                    gtag = sampstr.split("|")[2].strip()
                    sample = DirectorySample(location=location,dataset=dsname,globber="*.root",gtag=gtag)
                elif "MINIAODSIM" in sampstr:
                    dsname = sampstr.split("|")[0].strip()
                    xsec = float(sampstr.split("|")[1].strip())
                    kfact = float(sampstr.split("|")[2].strip())
                    efact = float(sampstr.split("|")[3].strip())
                    extra = ""
                    if sampstr.count("|") == 4:
                        extra = sampstr.split("|")[4].strip()
                        if "skip" in extra:
                            task_params["min_completion_fraction"] = 0.01*int(extra.split("skip",1)[1].split(",")[0])
                        if "singtop" in extra:
                            # hacky. increment tag for these singletop guys because of LHEweights crashes with older tag
                            task_params["tag"] = "CMS4_V10-02-06"
                            task_params["tarfile"] = "{}/lib_CMS4_V10-02-06_1025.tar.xz".format(tarballdir)
                        if "rpv" in extra:
                            # hacky. increment tag for rpv samples for same reason as singtop
                            task_params["tag"] = "CMS4_V10-02-07"
                            task_params["tarfile"] = "{}/lib_CMS4_V10-02-07_1025.tar.xz".format(tarballdir)
                            # task_params["recopy_inputs"] = True
                        if "open" in extra:
                            task_params["open_dataset"] = True
                            task_params["flush"] = True
                            print "This dataset is open!"
                        if "v08" in extra: # short track branch
                            task_params["tag"] = "CMS4_V10-02-08"
                            task_params["tarfile"] = "{}/lib_CMS4_V10-02-08_1025.tar.xz".format(tarballdir)
                        if "v09" in extra: # another short track branch (decayZ)
                            task_params["tag"] = "CMS4_V10-02-09"
                            task_params["tarfile"] = "{}/lib_CMS4_V10-02-09_1025.tar.xz".format(tarballdir)
                        if "ttttnew" in extra:
                            # redo TTTT sample with newer tag to get more events from miniaod
                            task_params["tag"] = "CMS4_V10-02-08"
                            task_params["tarfile"] = "{}/lib_CMS4_V10-02-08_1025.tar.xz".format(tarballdir)
                    sample = DBSSample(dataset=dsname,xsec=xsec,kfact=kfact,efact=efact)
                else:
                    dsname = str(sampstr)
                    sample = DBSSample(dataset=dsname)
                task = CMSSWTask(sample=sample, **task_params)
                tasks.append(task)

            # Now process the tasks and store summaries in a dict
            for task in tasks:
                try:
                    # if (not task.complete()) or ("WZTo3LNu" in task.get_sample().get_datasetname()):
                    # if (not task.complete()) or ("WJetsToLNu_HT" in task.get_sample().get_datasetname()):
                    # if not task.complete():
                    if (not task.complete()) or (task.get_sample().get_datasetname() in special):
                        task.kwargs["condor_submit_params"] = dict(
                                classads=[ ["JobBatchName",campaign], ],
                                )
                        if "PRIVATE" in task.get_sample().get_datasetname(): task.process()
                        else:
                            task.process(optimizer=optimizer)
                except:
                    traceback_string = traceback.format_exc()
                    print "Runtime error:\n{0}".format(traceback_string)
                    send_email(subject="metis error", body=traceback_string)
                dsname = task.get_sample().get_datasetname()
                total_summary[dsname] = task.get_task_summary()

        # break

        # Done with all campaigns' tasks, so update the dashboard and get some zzzzs
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do(show_progress_bar=True)
        interruptible_sleep(4*3600)

