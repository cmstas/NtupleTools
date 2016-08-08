#!/usr/bin/env python
# instructions: call as 
#     python findMissingLumis.py
# https://twiki.cern.ch/twiki/bin/view/CMS/DBS3APIInstructions
import  sys
try:
    from dbs.apis.dbsClient import DbsApi
    from FWCore.PythonUtilities.LumiList import LumiList
except:
    print "Do cmsenv and then crabenv, *in that order*"
    print "If you screw up, tough luck, re-ssh"
import pprint
import json,urllib2
from itertools import groupby

def listToRanges(a):
    # turns [1,2,4,5,9] into [[1,2],[4,5],[9]]
    ranges = []
    for k, iterable in groupby(enumerate(sorted(a)), lambda x: x[1]-x[0]):
         rng = list(iterable)
         if len(rng) == 1: first, second = rng[0][1], rng[0][1]
         else: first, second = rng[0][1], rng[-1][1]
         ranges.append([first,second])
    return ranges

def getChunks(v,n=990): return [ v[i:i+n] for i in range(0, len(v), n) ]

def getDatasetFileLumis(dataset):
    url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    api=DbsApi(url=url)
    dRunLumis = {}

    files = api.listFiles(dataset=dataset)
    files = [f.get('logical_file_name','') for f in files]

    # chunk into size less than 1000 or else DBS complains
    fileChunks = getChunks(files)

    for fileChunk in fileChunks:

        info = api.listFileLumiArray(logical_file_name=fileChunk)

        for f in info:
            fname = f['logical_file_name']
            dRunLumis[fname] = {}
            run, lumis = str(f['run_num']), f['lumi_section_num']
            if run not in dRunLumis[fname]: dRunLumis[fname][run] = []
            dRunLumis[fname][run].extend(lumis)

    for fname in dRunLumis.keys():
        for run in dRunLumis[fname].keys():
            dRunLumis[fname][run] = listToRanges(dRunLumis[fname][run])

    return dRunLumis

def getLumiFromLL(d):
    totLumi = 0.0
    for run,ls in d.getLumis():
        if (run,ls) not in dLumiMap: continue
        # print run,ls, dLumiMap[(run,ls)]
        totLumi += dLumiMap[(run,ls)]
    return totLumi

datasets = []
lumisCompleted = []
# goldenJson = "Cert_271036-273450_13TeV_PromptReco_Collisions16_JSON_NoL1T.txt"
# goldenJson = "/home/users/namin/dataTuple/NtupleTools/dataTuple/Cert_271036-273730_13TeV_PromptReco_Collisions16_JSON.txt"
# goldenJson = "/home/users/namin/dataTuple/NtupleTools/dataTuple/Cert_271036-274240_13TeV_PromptReco_Collisions16_JSON.txt" 0.804/fb
# goldenJson = "/home/users/namin/dataTuple/NtupleTools/dataTuple/Cert_271036-274421_13TeV_PromptReco_Collisions16_JSON.txt" # 2.07/fb
# goldenJson = "/home/users/namin/dataTuple/NtupleTools/dataTuple/Cert_271036-274443_13TeV_PromptReco_Collisions16_JSON.txt" # 2.66/fb
# goldenJson = "/home/users/namin/dataTuple/NtupleTools/dataTuple/Cert_271036-275125_13TeV_PromptReco_Collisions16_JSON.txt" # 3.99/fb
# goldenJson = "/home/users/namin/dataTuple/2016C/NtupleTools/dataTuple/Cert_271036-275783_13TeV_PromptReco_Collisions16_JSON_NoL1T.txt" # 5.76/fb
# goldenJson = "/home/users/namin/dataTuple/2016C/NtupleTools/dataTuple/Cert_271036-275783_13TeV_PromptReco_Collisions16_JSON.txt" # 6.26/fb
# goldenJson = "/home/users/namin/dataTuple/2016C/NtupleTools/dataTuple/Cert_271036-276097_13TeV_PromptReco_Collisions16_JSON_NoL1T_v2.txt" # 7.65/fb no L1T certification
# goldenJson = "/home/users/namin/dataTuple/2016D/NtupleTools/dataTuple/Cert_271036-276384_13TeV_PromptReco_Collisions16_JSON_NoL1T.txt" # 9.24/fb no L1T certification
goldenJson = "/home/users/namin/dataTuple/2016D/NtupleTools/dataTuple/Cert_271036-276811_13TeV_PromptReco_Collisions16_JSON_NoL1T.txt" # 12.9/fb ICHEP no L1T certification

# if(len(sys.argv) > 1):
#     goldenJson = sys.argv[1]
#     print "Using JSON:",goldenJson

goldenLumis = LumiList(compactList=json.loads(open(goldenJson,"r").read()))



# parse lumiMap
dLumiMap =  {}
with open("lumis/lumis_skim.csv", "r") as fhin:
    for line in fhin:
        line = line.strip()
        try:
            run,ls,ts,deliv,recorded = line.split(",")
            run = int(run)
            ls = int(ls)
            recordedPB = float(recorded)
            dLumiMap[(run,ls)] = recordedPB
        except: pass


goldenIntLumi = getLumiFromLL(goldenLumis)
print "Using %s with %.2f/pb" % (goldenJson, goldenIntLumi)


dLinks = {
        "SingleMuon": [
            ("/SingleMuon/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleMuon/full_JSON_Run2016B_SingleMuon_MINIAOD_PromptReco-v1.txt"),
            ("/SingleMuon/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleMuon/full_JSON_Run2016B_SingleMuon_MINIAOD_PromptReco-v2.txt"),
            ("/SingleMuon/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleMuon/full_JSON_Run2016C_SingleMuon_MINIAOD_PromptReco-v2.txt"),
            ("/SingleMuon/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleMuon/full_JSON_Run2016D_SingleMuon_MINIAOD_PromptReco-v2.txt"),
            ],
        "SinglePhoton": [
            ("/SinglePhoton/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SinglePhoton/full_JSON_Run2016B_SinglePhoton_MINIAOD_PromptReco-v1.txt"),
            ("/SinglePhoton/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SinglePhoton/full_JSON_Run2016B_SinglePhoton_MINIAOD_PromptReco-v2.txt"),
            ("/SinglePhoton/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SinglePhoton/full_JSON_Run2016C_SinglePhoton_MINIAOD_PromptReco-v2.txt"),
            ("/SinglePhoton/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SinglePhoton/full_JSON_Run2016D_SinglePhoton_MINIAOD_PromptReco-v2.txt"),
            ],
        "SingleElectron": [
            ("/SingleElectron/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleElectron/full_JSON_Run2016B_SingleElectron_MINIAOD_PromptReco-v1.txt"),
            ("/SingleElectron/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleElectron/full_JSON_Run2016B_SingleElectron_MINIAOD_PromptReco-v2.txt"),
            ("/SingleElectron/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleElectron/full_JSON_Run2016C_SingleElectron_MINIAOD_PromptReco-v2.txt"),
            ("/SingleElectron/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/SingleElectron/full_JSON_Run2016D_SingleElectron_MINIAOD_PromptReco-v2.txt"),
            ],
        "MuonEG": [
            ("/MuonEG/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/MuonEG/full_JSON_Run2016B_MuonEG_MINIAOD_PromptReco-v1.txt"),
            ("/MuonEG/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/MuonEG/full_JSON_Run2016B_MuonEG_MINIAOD_PromptReco-v2.txt"),
            ("/MuonEG/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/MuonEG/full_JSON_Run2016C_MuonEG_MINIAOD_PromptReco-v2.txt"),
            ("/MuonEG/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/MuonEG/full_JSON_Run2016D_MuonEG_MINIAOD_PromptReco-v2.txt"),
            ],
        "DoubleEG": [
            ("/DoubleEG/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/DoubleEG/full_JSON_Run2016B_DoubleEG_MINIAOD_PromptReco-v1.txt"),
            ("/DoubleEG/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/DoubleEG/full_JSON_Run2016B_DoubleEG_MINIAOD_PromptReco-v2.txt"),
            ("/DoubleEG/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/DoubleEG/full_JSON_Run2016C_DoubleEG_MINIAOD_PromptReco-v2.txt"),
            ("/DoubleEG/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/nick/json_lists/full_JSON_Run2016D_DoubleEG_MINIAOD_PromptReco-v2.txt"),
            ],
        "DoubleMuon": [
            ("/DoubleMuon/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/DoubleMuon/full_JSON_Run2016B_DoubleMuon_MINIAOD_PromptReco-v1.txt"),
            ("/DoubleMuon/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/DoubleMuon/full_JSON_Run2016B_DoubleMuon_MINIAOD_PromptReco-v2.txt"),
            ("/DoubleMuon/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/DoubleMuon/full_JSON_Run2016C_DoubleMuon_MINIAOD_PromptReco-v2.txt"),
            ("/DoubleMuon/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/nick/json_lists/full_JSON_Run2016D_DoubleMuon_MINIAOD_PromptReco-v2.txt"),
            ],
        "HTMHT": [
            ("/HTMHT/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/HTMHT/full_JSON_Run2016B_HTMHT_MINIAOD_PromptReco-v1.txt"),
            ("/HTMHT/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/HTMHT/full_JSON_Run2016B_HTMHT_MINIAOD_PromptReco-v2.txt"),
            ("/HTMHT/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/HTMHT/full_JSON_Run2016C_HTMHT_MINIAOD_PromptReco-v2.txt"),
            ("/HTMHT/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/nick/json_lists/full_JSON_Run2016D_HTMHT_MINIAOD_PromptReco-v2.txt"),
            ],
        "JetHT": [
            ("/JetHT/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/JetHT/full_JSON_Run2016B_JetHT_MINIAOD_PromptReco-v1.txt"),
            ("/JetHT/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/JetHT/full_JSON_Run2016B_JetHT_MINIAOD_PromptReco-v2.txt"),
            ("/JetHT/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/JetHT/full_JSON_Run2016C_JetHT_MINIAOD_PromptReco-v2.txt"),
            ("/JetHT/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/nick/json_lists/full_JSON_Run2016D_JetHT_MINIAOD_PromptReco-v2.txt"),
            ],
        "MET": [
            ("/MET/Run2016B-PromptReco-v1/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/MET/full_JSON_Run2016B_MET_MINIAOD_PromptReco-v1.txt"),
            ("/MET/Run2016B-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/MET/full_JSON_Run2016B_MET_MINIAOD_PromptReco-v2.txt"),
            ("/MET/Run2016C-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/final_jsons/MET/full_JSON_Run2016C_MET_MINIAOD_PromptReco-v2.txt"),
            ("/MET/Run2016D-PromptReco-v2/MINIAOD",  "/nfs-7/userdata/dataTuple/nick/json_lists/full_JSON_Run2016D_MET_MINIAOD_PromptReco-v2.txt"),
            ],
        }



allCMS3Lumis = LumiList(compactList={})

for pd in dLinks.keys():
    cms3Lumis = LumiList(compactList={})
    fileLumis = {}
    for dataset,link in dLinks[pd]:


        # Add to total json of what we have in CMS3 for this PD
        try:
            with open(link, "r") as fhin:
                j = json.load(fhin)
        except:
            print "Error trying to read %s, %s" % (dataset, link)
            continue

        print "-"*5, dataset, "-"*5

        cms3Lumis += LumiList(compactList=j)

        allCMS3Lumis += LumiList(compactList=j)

        # Add to total json of what we DAS says there is in miniaod for this PD (key is miniaod file name, val is json)
        fileLumis.update(getDatasetFileLumis(dataset))

    # These are in the GoldenJSON but not CMS3
    inGoldenButNotCMS3 = goldenLumis - cms3Lumis
    inGoldenButNotCMS3IntLumi = getLumiFromLL(inGoldenButNotCMS3)

    # cms3LumisIntLumi = getLumiFromLL(cms3Lumis - (cms3Lumis - goldenLumis))
    cms3LumisIntLumi = getLumiFromLL(cms3Lumis & goldenLumis)
    print "We have %.2f/pb in CMS3" % (getLumiFromLL(cms3Lumis))
    print "We have %.2f/pb in Golden&CMS3 (%.1f%% of golden)" % (cms3LumisIntLumi, 100.0*cms3LumisIntLumi/goldenIntLumi)
    print "This is what is in the Golden JSON, but not the CMS3 merged (%.2f/pb):" % getLumiFromLL(inGoldenButNotCMS3)
    print inGoldenButNotCMS3
    print

    for file in fileLumis.keys():
        fileLumi = LumiList(compactList=fileLumis[file])
        # Only care about stuff in the file that is in the golden JSON
        fileLumi = fileLumi - (fileLumi - goldenLumis)
        nLumisInFile = len(fileLumi.getLumis())

        lumisWeDontHave = fileLumi - cms3Lumis
        nLumisWeDontHave = len(lumisWeDontHave.getLumis())

        # If we don't have ANY of the lumis in a file, it could be that we didn't run over the file
        # (I am thus implicitly assuming that if we have any lumis in cms3 corresponding to a file
        #  that we actually ran over the whole file and maybe didn't store some lumis due to triggers)
        if nLumisInFile == nLumisWeDontHave and nLumisInFile > 0: 
            # maybe we didn't run over this file
            print " "*5,file
            print " "*10,"File has lumis ", fileLumi,"and CMS3 is missing all of them"

    print "\n"*2

