#!/usr/bin/env python
# instructions: call as 
#     python findMissingLumis.py [json] ["datasetpattern"] 
# example: python findMissingLumis.py Cert_246908-258159_13TeV_PromptReco_Collisions15_25ns_JSON_v2.txt
# https://twiki.cern.ch/twiki/bin/view/CMS/DBS3APIInstructions

# every now and then, must do
#        ssh namin@lxplus.cern.ch /afs/cern.ch/user/n/namin/.local/bin/brilcalc lumi -u /pb  --byls -i /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Cert_271036-273450_13TeV_PromptReco_Collisions16_JSON_NoL1T.txt > lumiList.txt
# to be able to make a (run,lumi) --> int. lumi. map
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

def getDatasetFileLumis(dataset):
    url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    api=DbsApi(url=url)
    dRunLumis = {}

    files = api.listFiles(dataset=dataset)
    files = [f.get('logical_file_name','') for f in files]

    info = api.listFileLumiArray(logical_file_name=files)

    # print info[0]
    for file in info:
        fname = file['logical_file_name']
        dRunLumis[fname] = {}
        run, lumis = str(file['run_num']), file['lumi_section_num']
        if run not in dRunLumis[fname]: dRunLumis[fname][run] = []
        dRunLumis[fname][run].extend(lumis)

    for fname in dRunLumis.keys():
        for run in dRunLumis[fname].keys():
            dRunLumis[fname][run] = listToRanges(dRunLumis[fname][run])

    return dRunLumis

datasets = []
lumisCompleted = []
goldenJson = "Cert_271036-273450_13TeV_PromptReco_Collisions16_JSON_NoL1T.txt"
if(len(sys.argv) > 1):
    goldenJson = sys.argv[1]
    print "Using JSON:",goldenJson
datasetPattern = "/%s/Run2016B-PromptReco-%s/MINIAOD"
# if(len(sys.argv) > 2):
#     datasetPattern = sys.argv[2]
#     print "Using dataset pattern:",datasetPattern

for user in ['mderdzinski','namin']:
    html = urllib2.urlopen("http://uaf-7.t2.ucsd.edu/~%s/dataTupleMonitor.html" % user).readlines()
    for line in html:
        if ('Dataset: ' in line):
            datasets.append(line.split(":")[-1].replace("<BR>","").replace("<b>","").replace("</b>","").strip())
        elif ('Lumis completed: ' in line):
            lumisCompleted.append(line.split("HREF=\"")[-1].split("\">")[0].strip())

dLumiLinks = {}
for dataset, link in zip(datasets, lumisCompleted):
    dLumiLinks[dataset] = link

# print dLumiLinks

goldenLumis = LumiList(compactList=json.loads(open(goldenJson,"r").read()))

primaryDatasets = [
"DoubleEG",
"DoubleMuon",
"HTMHT",
"JetHT",
"MET",
"MuonEG",
"SingleElectron",
"SingleMuon",
"SinglePhoton",
]

# parse lumiMap
dLumiMap =  {}
with open("lumiList.txt", "r") as fhin:
    for line in fhin:
        line = line.strip()
        if "STABLE" not in line: continue
        parts = line.split("|")
        run = int(parts[1].split(":")[0])
        ls = int(parts[2].split(":")[0])
        recordedPB = float(parts[7])
        dLumiMap[(run,ls)] = recordedPB

def getLumiFromLL(d):
    totLumi = 0.0
    for run,ls in d.getLumis():
        if (run,ls) not in dLumiMap: continue
        # print run,ls, dLumiMap[(run,ls)]
        totLumi += dLumiMap[(run,ls)]
    return totLumi

goldenIntLumi = getLumiFromLL(goldenLumis)
        
for pd in primaryDatasets:
    cms3Lumis = LumiList(compactList={})
    fileLumis = {}
    for v in ["v1","v2"]:
        pdv = pd+"-"+v
        dataset = datasetPattern % (pd, v)
        lumiLink = dLumiLinks[pdv]

        print "-"*5, dataset, "-"*5

        # Add to total json of what we have in CMS3 for this PD
        cms3Lumis += LumiList(compactList=json.loads(urllib2.urlopen(lumiLink).read()))

        # Add to total json of what we DAS says there is in miniaod for this PD (key is miniaod file name, val is json)
        fileLumis.update(getDatasetFileLumis(dataset))

    # These are in the GoldenJSON but not CMS3
    inGoldenButNotCMS3 = goldenLumis - cms3Lumis
    inGoldenButNotCMS3IntLumi = getLumiFromLL(inGoldenButNotCMS3)

    cms3LumisIntLumi = getLumiFromLL(cms3Lumis)
    print "We have %.2f/pb in CMS3 (%.1f%% of golden)" % (cms3LumisIntLumi, 100.0*cms3LumisIntLumi/goldenIntLumi)
    print "This is what is in the Golden JSON, but not the CMS3 merged: (%.2f/pb)" % getLumiFromLL(inGoldenButNotCMS3)
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
