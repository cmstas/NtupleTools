#!/usr/bin/env python
# instructions: call as 
#     python findMissingLumis.py [json] ["datasetpattern"] 
# example: python findMissingLumis.py Cert_246908-258159_13TeV_PromptReco_Collisions15_25ns_JSON_v2.txt "/%s/Run2015D-PromptReco-v3/MINIAOD"
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

def getDatasetFileLumis(datasetPattern,dataset):
    dataset = datasetPattern % dataset
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
# goldenJson = "Cert_246908-257599_13TeV_PromptReco_Collisions15_25ns_JSON_v2.txt"
goldenJson = "Cert_246908-258159_13TeV_PromptReco_Collisions15_25ns_JSON_v2.txt"
if(len(sys.argv) > 1):
    goldenJson = sys.argv[1]
    print "Using JSON:",goldenJson
datasetPattern = "/%s/Run2015D-PromptReco-v3/MINIAOD"
if(len(sys.argv) > 2):
    datasetPattern = sys.argv[2]
    print "Using dataset pattern:",datasetPattern

for user in ['mderdzinski','namin']:
    html = urllib2.urlopen("http://uaf-7.t2.ucsd.edu/~%s/dataTupleMonitor.html" % user).readlines()
    for line in html:
        if ('Dataset: ' in line):
            datasets.append(line.split(":")[-1].replace("<BR>","").strip())
        elif ('Lumis completed: ' in line):
            lumisCompleted.append(line.split("HREF=\"")[-1].split("\">")[0].strip())

dLumiLinks = {}
for dataset, link in zip(datasets, lumisCompleted):
    dLumiLinks[dataset] = link

goldenLumis = LumiList(compactList=json.loads(open(goldenJson,"r").read()))

for dataset,lumiLink in dLumiLinks.items():
    print "%s (%s)" % (dataset, datasetPattern % dataset)
    cms3Lumis = LumiList(compactList=json.loads(urllib2.urlopen(lumiLink).read()))

    # These are in the GoldenJSON but not CMS3
    inGoldenButNotCMS3 = goldenLumis - cms3Lumis

    # These are the lumis in each file of the dataset
    fileLumis = getDatasetFileLumis(datasetPattern,dataset)

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

