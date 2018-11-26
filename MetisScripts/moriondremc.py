import time
import itertools
import json
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email
from metis.Constants import Constants



def get_tasks():

            # put metadata names into text file and then do
            # cat samples.txt  | pyline.py  '"\"{0}|{1}|{2}|{3}\",".format(*map(lambda a,b=json.load(open(x)): b[a],["dataset","xsec","kfact","efact"]))'

    infos = [

            # "/WZTo3LNu_2Jets_MLL-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.07684|1|1",
            # "/WZTo3LNu_1Jets_MLL-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.3446|1|1",
            # "/WZTo3LNu_0Jets_MLL-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.5771|1|1",
            # "/WZTo3LNu_3Jets_MLL-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.1112|1|1",
            # "/WZTo3LNu_1Jets_MLL-4To50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.5291|1|1",
            # "/WZTo3LNu_0Jets_MLL-4To50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|2.3011|1|1",
            # "/WZTo3LNu_2Jets_MLL-4To50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.08196|1|1",
            # "/WZTo3LNu_3Jets_MLL-4To50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.09482|1|1",


             # "/SMS-T1tbs_RPV_mGluino1000to1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-pLHE_PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino",

            # "/TTTo2L2Nu_HT500Njet7_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",

            # "/TTToSemiLepton_HT500Njet9_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",

            # "/TTWJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext2-v1/MINIAODSIM|0.2043|1.0|1.0",

            # "/ZJetsToQQ_HT600toInf_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|5.67|1|1",
            # "/DYJetsToQQ_HT180_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1187|1|1",
            # "/WJetsToQQ_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|95.14|1|1",
            # "/WJetsToQQ_HT180_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|2788|1|1",

            # "/TT_FCNC-TtoHJ_aThadronic_HToaa_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/TT_FCNC-TtoHJ_aTleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/TT_FCNC-TtoHJ_aTleptonic_HToaa_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/TT_FCNC-TtoHJ_aTleptonic_HTobb_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/TT_FCNC-aTtoHJ_Thadronic_HToaa_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/TT_FCNC-aTtoHJ_Tleptonic_HToWWZZtautau_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/TT_FCNC-aTtoHJ_Tleptonic_HToaa_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/TT_FCNC-aTtoHJ_Tleptonic_HTobb_eta_hut-MadGraph5-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",

             # "/VBF-C1N2_WZ_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUSummer16Fast_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1|mChi,mLSP",

            # "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v2/MINIAODSIM|4895.0|1.23|1.0",
            # "/TTGamma_Dilept_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.632|1.0|1.0",
            # "/TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|730.0|1.139397|1.0",
            # "/WJetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|50690.0|1.21|1.0",
            # "/tZq_ll_4f_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|0.0758|1.0|1.0",


            # "/Sphaleron_NNPDF30_lo_as_0118_0_pythia8TuneCUETP8M1/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/TTGJets_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|3.697|1|1",

            # "/TTJets_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|831.76|1.0|1.0",
            # "/GluGluHToBB_M125_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/GluGluHToBB_M125_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|1|1|1",
            # "/WLLJJ_WToLNu_EWK_TuneCUETP8M1_13TeV_madgraph-madspin-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.01763|1|1",

            # "/WJetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|50690.0|1.21|1.0",
            # "/WZTo1L1Nu2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v3/MINIAODSIM|10.96|0.9781|1.0",


             # "/SMS-ResonantSmu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
             # "/SMS-ResonantSneu_x0p1_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
             # "/SMS-ResonantSneu_x0p5_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
             # "/SMS-ResonantSneu_x0p9_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",

             # "/SMS-TChiHH_HToWWZZTauTau_HToWWZZTauTau_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v2/MINIAODSIM|1|1|1|mGluino",
             # "/VBF-C1N2_leptonicDecays_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUSummer16Fast_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
             # "/GluGluHToWWToLNuQQ_M125_13TeV_powheg_JHUGenV628_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
             # "/VBFHToWWToLNuQQ_M125_13TeV_powheg_JHUGenV628_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",


            # "/DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|18610.0|1.11|1.0",
            # "/DYJetsToLL_M-50_HT-600to800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1.367|1.23|1.0",
            # "/ST_t-channel_antitop_4f_inclusiveDecays_13TeV-powhegV2-madspin-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|80.95|1.0|1.0",
            # "/ST_t-channel_top_4f_inclusiveDecays_13TeV-powhegV2-madspin-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|136.02|1.0|1.0",
            # "/TTJets_DiLept_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|57.35|1.5225|1.0",
            # "/TTJets_SingleLeptFromT_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|114.6|1.594|1.0",
            # "/TTJets_SingleLeptFromTbar_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|114.6|1.594|1.0",
            # "/WGToLNuG_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|405.271|1.0|1.0",
            # "/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|1345.0|1.21|1.0",
            # "/WJetsToLNu_HT-2500ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|0.03216|1.21|1.0",
            # "/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|48.91|1.21|1.0",
            # "/WJetsToLNu_HT-600To800_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|12.05|1.21|1.0",
            # "/WZTo1L3Nu_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|3.05|1.00262|1.0",
            # "/ZZTo2L2Nu_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.5644|1.0|1.0",
            # "/ZZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|3.38|0.95296|1.0",
            # "/ZZTo2Q2Nu_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|4.072|1.1628|1.0",
            # "/ttWJets_13TeV_madgraphMLM/RunIISummer16MiniAODv2-80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.6105|1.0|1.0",
            # "/ttZJets_13TeV_madgraphMLM/RunIISummer16MiniAODv2-80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.7826|1.0|1.0",

            # "/GluGluZH_HToWW_M125_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.06961|1|1",
            # "/HWminusJ_HToWW_M125_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.1734|1|1",
            # "/HWplusJ_HToWW_M125_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.851|1|1",
            # "/HZJ_HToWW_M125_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.752|1|1",

            # "/DoublyChargedHiggsGMmodel_HWW_M800_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",

            # "/DoublyChargedHiggsGMmodel_HWW_M300_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M500_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M700_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v3/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M200_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M1000_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M400_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M600_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M900_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M2000_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            # "/DoublyChargedHiggsGMmodel_HWW_M1500_13TeV-madgraph/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|1|1|1",
            
            # "/SMS-TChiWH_WToLNu_HToVVTauTau_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mChi,mLSP",
            # "/WprimeToWHToWhadHinc_narrow_M-6000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",

            # "/WprimeToWHToWhadHinc_narrow_M-600_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-800_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-1000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-1200_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-1400_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-1600_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-1800_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-2000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-2500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-3000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-3500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-4000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-4500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-5000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWhadHinc_narrow_M-5500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-600_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-800_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-1000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-1200_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-1400_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-1600_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-1800_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-2000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-2500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-3500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-4000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-4500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-5000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-5500_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",
            # "/WprimeToWHToWlepHinc_narrow_M-6000_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1",



            # "/TTTT_TuneCUETP8M2T4_13TeV-amcatnlo-isrdown-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.009103|1|1",
            # "/TTTT_TuneCUETP8M2T4_13TeV-amcatnlo-fsrdown-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.009103|1|1",
            # "/TTTT_TuneCUETP8M2T4_13TeV-amcatnlo-fsrup-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.009103|1|1",
            # "/TTTT_TuneCUETP8M2T4_13TeV-amcatnlo-isrup-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.009103|1|1",
            # "/TTWW_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.011500|1|1", 
            # "/TTWH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.001582|1|1", 
            # "/TTWZ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.003884|1|1",
            # "/TTZZ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.001982|1|1",
            # "/TTHH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.000757|1|1",
            # "/TTTJ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.000474|1|1",
            # "/TTZH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.001535|1|1",

            # "/TTTW_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.000788|1|1", 
            # "/TTWZ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.003884|1|1", 
            # "/TTTJ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.000474|1|1", 
            # "/TTHH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.000757|1|1", 
            # "/TTZH_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.001535|1|1", 
            # "/TTZZ_TuneCUETP8M2T4_13TeV-madgraph-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.001982|1|1", 

            # "/TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|730.0|1.139397|1",
            # "/ttHToNonbb_M125_TuneCUETP8M2_ttHtranche3_13TeV-powheg-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.215|1|1",

            # "/LQLQToTopTau_M-1400_TuneCUETP8M1_13TeV_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/ttHJetToGG_M125_13TeV_amcatnloFXFX_madspin_pythia8_v2/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/GluGluHToGG_M125_13TeV_amcatnloFXFX_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext2-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/VHToGG_M125_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/VBFHToGG_M125_13TeV_amcatnlo_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext2-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/VBFHToGG_M125_13TeV_amcatnlo_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext1-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/GJet_Pt-40toInf_DoubleEMEnriched_MGG-80toInf_TuneCUETP8M1_13TeV_Pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/QCD_Pt-40toInf_DoubleEMEnriched_MGG-80toInf_TuneCUETP8M1_13TeV_Pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/QCD_Pt-30toInf_DoubleEMEnriched_MGG-40to80_TuneCUETP8M1_13TeV_Pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/QCD_Pt-30to40_DoubleEMEnriched_MGG-80toInf_TuneCUETP8M1_13TeV_Pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/GJet_Pt-20toInf_DoubleEMEnriched_MGG-40to80_TuneCUETP8M1_13TeV_Pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/GJet_Pt-20to40_DoubleEMEnriched_MGG-80toInf_TuneCUETP8M1_13TeV_Pythia8/RunIISummer16MiniAODv2-PUMoriond17_backup_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox_M40_80-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox_MGG-80toInf_13TeV-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox1BJet_MGG-80toInf_13TeV-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox2BJets_MGG-80toInf_13TeV-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox_M40_80-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox_MGG-80toInf_13TeV-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox1BJet_MGG-80toInf_13TeV-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/DiPhotonJetsBox2BJets_MGG-80toInf_13TeV-Sherpa/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",
            # "/TTGG_0Jets_TuneCUETP8M1_13TeV_amcatnlo_madspin_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1.0|1.0|1.0",

            # "/SMS-T8bbstausnu_XCha0p5_XStau0p25_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUSummer16Fast_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T8bbstausnu_XCha0p5_XStau0p5_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUSummer16Fast_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T8bbstausnu_XCha0p5_XStau0p75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUSummer16Fast_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|1|1|1|mGluino,mLSP",


            # "/SMS-T1tttt_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T2bb_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mSbottom,mLSP",
            # "/SMS-T5ZZ_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mChi,mLSP",
            # "/SMS-TChiSlepSnu_x0p5_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mChi,mLSP",
            # "/SMS-TChiWG_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mChi",
            # "/SMS-T5qqqqVV_dM20_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T2tt_mStop-400to1200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T2bW_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-TChiSlepSnu_x0p05_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v2/MINIAODSIM|1|1|1|mChi,mLSP",
            # "/SMS-T1bbbb_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T2tt_mStop-150to250_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T2tt_mStop-250to350_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T2tt_mStop-350to400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T2tt_dM-10to80_2Lfilter_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T1qqqq_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T5qqqqVV_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-TChiWZ_ZToLL_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v2/MINIAODSIM|1|1|1|mChi,mLSP",
            # "/SMS-TChiWH_WToLNu_HToBB_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mChi,mLSP",
            # "/SMS-T5ttcc_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v3/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T2qq_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mSquark,mLSP",
            # "/SMS-T2cc_genHT-160_genMET-80_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mSquark,mLSP",
            # "/SMS-T5tttt_dM175_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T6ttWW_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T2tt_dM-10to80_genHT-160_genMET-80_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T2bt_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T1ttbb_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T5ZZ_mGluino-1850to2100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T6bbllslepton_mSbottom-400To950_mLSP-120To140_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mSbottom,mLSP",
            # "/SMS-T6bbllslepton_mSbottom-1000To1500_mLSP-120To1450_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mSbottom,mLSP",
            # "/SMS-T6qqWW_dM10_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T2tt_dM-10to80_genHT-160_genMET-80_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mStop,mLSP",
            # "/SMS-T2cc_genHT-160_genMET-80_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mSquark,mLSP",
            # "/SMS-T6qqWW_dM5_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-T6qqWW_dM20_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM|1|1|1|mGluino,mLSP",
            # "/SMS-TChiHZ_HToBB_ZToLL_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v3/MINIAODSIM|1|1|1|mChi",
            # "/SMS-TChiZZ_ZToLL_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v2/MINIAODSIM|1|1|1|mChi",
            # "/SMS-TChiHH_HToBB_HToBB_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v2/MINIAODSIM|1|1|1|mChi,mLSP",
            # "/SMS-TStauStau_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v2/MINIAODSIM|1|1|1|mStau,mLSP",
            # "/SMS-TChipmSlepSnu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv2-PUSpring16Fast_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v2/MINIAODSIM|1|1|1|mChi,mLSP",

            # "/GJets_DR-0p4_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_qcut19_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|43.36|1|1",
            # "/TTGamma_SingleLeptFromT_TuneCUETP8M2T4_13TeV-amcatnlo-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.77|1|1",
            # "/TTWJetsToQQ_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.4062|1|1",

            # "/WmWmJJ_13TeV-powheg-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.007868|1|1",
            # "/WpWpJJ_13TeV-powheg-pythia8_TuneCUETP8M1/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.007868|1|1",


            # "/ZH_ZToEE_HToInvisible_M800_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v2/MINIAODSIM|0.00002637|1.0|1.0",
            # "/WminusH_HToInvisible_WToQQ_M300_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.01602|1.0|1.0",
            # "/WminusH_HToInvisible_WToQQ_M800_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.0002112|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M110_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.8999|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M150_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.3395|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M200_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.1280|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M300_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.0293|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M400_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.009653|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M500_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.003927|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M600_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.001826|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M800_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.0005086|1.0|1.0",
            # "/WplusH_HToInvisible_WToQQ_M1000_13TeV_powheg_pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM|0.0001743|1.0|1.0",

            ]



    tasks = []
    for info in infos:
        dsname = info.split("|")[0].strip()
        xsec = float(info.split("|")[1].strip())
        kfact = float(info.split("|")[2].strip())
        efact = float(info.split("|")[3].strip())
        sparms = []
        pset_args = "data=False"
        if info.count("|") == 4:
            sparms = info.split("|")[4].split(",")
            pset_args = "data=False fastsim=True"
        # break

        cmsswver = "CMSSW_8_0_26_patch1"
        tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-02_2017Sep27.tar.gz"

        # allow_invalid_files = any(x in dsname for x in ["TTWW","TTWH","TTTW"])


        task = CMSSWTask(
                sample = DBSSample(
                    dataset=dsname,
                    xsec=xsec,
                    kfact=kfact,
                    efact=efact,
                    # allow_invalid_files=allow_invalid_files,
                    allow_invalid_files=False,
                    ),
                events_per_output = 350e3,
                output_name = "merged_ntuple.root",
                tag = "CMS4_V00-00-02_2017Sep27",
                global_tag = "", # if global tag blank, one from DBS is used
                pset = "psets/pset_moriondremc.py",
                pset_args = pset_args,
                condor_submit_params = {"use_xrootd":True},
                # condor_submit_params = {"sites":"UCSB"},
                # condor_submit_params = {"sites":"T2_US_Nebraska,T2_US_Purdue,T2_US_MIT"},
                cmssw_version = cmsswver,
                tarfile = tarfile,
                special_dir = "run2_moriond17_cms4/ProjectMetis",
                # min_completion_fraction = 0.94,
                publish_to_dis = True,
                sparms = sparms,
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
