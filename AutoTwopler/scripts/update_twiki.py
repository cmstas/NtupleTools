import sys
sys.path.insert(1, "../dashboard")
import twiki
import os

samples = [
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v3/V08-00-05/"      , 1345.0  , 1.21 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/" , 1345.0  , 1.21 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"      , 359.7   , 1.21 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/" , 359.7   , 1.21 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"      , 48.91   , 1.21 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/WJetsToLNu_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/" , 48.91   , 1.21 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/ZJetsToNuNu_HT-100To200_13TeV-madgraph_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"                             , 280.35  , 1.23 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/ZJetsToNuNu_HT-100To200_13TeV-madgraph_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0_ext1-v1/V08-00-05/"                        , 280.35  , 1.23 , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-40To100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"            , 20790   , 1.0  , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-100To200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v4/V08-00-05/"           , 9238    , 1.0  , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"           , 2305    , 1.0  , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-400To600_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"           , 274.4   , 1.0  , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/GJets_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/"           , 93.46   , 1.0  , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv2/ST_t-channel_top_4f_leptonDecays_13TeV-powheg-pythia8_TuneCUETP8M1_RunIISpring16MiniAODv2-PUSpring16_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/V08-00-05/" , 44.1    , 1.0  , 1.0) ,
            ("/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/W1JetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/V08-00-01/",              9493.0,    1.238, 1.0),
]


d_samples = {}
sample_locations = set()
for s in samples:
    loc = os.path.normpath(s[0])
    d_samples[loc] = [s[1], s[2], s[3]]
    sample_locations.add( loc )

twiki_samples = [sample for sample in twiki.get_samples(assigned_to="all", username="namin", get_unmade=False, page="Run2Samples25ns80XminiAODv2") if os.path.normpath(sample["location"]) in sample_locations]

new_samples = []
for ts in twiki_samples:
    loc = os.path.normpath(ts["location"])
    ts["xsec"] = d_samples[loc][0]
    ts["kfact"] = d_samples[loc][1]
    ts["efact"] = d_samples[loc][2]
    new_samples.append(ts.copy())

## Disable dryrun if satisfied
twiki.update_samples(new_samples, username="namin", page="Run2Samples25ns80XminiAODv2", update_done=True, dryrun=True)
