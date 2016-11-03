#!/usr/bin/env python

import dis_client as dis
import lumi_utils as lu
import json
import sys

# list of patterns of datasets (eras) to consider
dataset_patterns = [
    "/{pd}/Run2016B-PromptReco-v1/MINIAOD",
    "/{pd}/Run2016B-PromptReco-v2/MINIAOD",
    "/{pd}/Run2016C-PromptReco-v2/MINIAOD",
    "/{pd}/Run2016D-PromptReco-v2/MINIAOD",
    "/{pd}/Run2016E-PromptReco-v2/MINIAOD",
    "/{pd}/Run2016F-PromptReco-v1/MINIAOD",
    "/{pd}/Run2016G-PromptReco-v1/MINIAOD",
    "/{pd}/Run2016H-PromptReco-v2/MINIAOD",
    "/{pd}/Run2016H-PromptReco-v3/MINIAOD",
]

# path to json to consider
fname = "Cert_271036-283685_13TeV_PromptReco_Collisions16_JSON_NoL1T.txt"

# these primary datasets will be more inclusive than the rest in terms of the number of runs they contain
pds = ["JetHT", "SingleMuon", "SingleElectron"]

# open json and make a runlumi object out of it
with open(fname, "r") as fhin:
    js = json.load(fhin)
all_run_lumis = lu.RunLumis(js)

# print for Twiki copy-paste purposes
print "---++++ !Lumis per era using JSON: %s" % fname
print "<br />%EDITTABLE{}%"
print "| *Era* | *First run* | *Last run* | *Int. lumi [/fb]* | "

for dataset_pattern in dataset_patterns:
    runs = set([])
    # for each pd in the pds above, make an inclusive set of runs
    # it could be the case that the first run is not present in a PD, so we are essentially
    # doing an OR of the few PDs above
    for pd in pds:
        dataset = dataset_pattern.format(pd=pd)
        runs.update( set(dis.query(dataset, typ="runs")["response"]["payload"]) )

    # the min and max runs we'll consider as the start and end of that era
    first_run = min(runs)
    last_run = max(runs)

    # get the integrated luminosity in the run range
    int_lumi = all_run_lumis.getIntLumi(first_run=first_run, last_run=last_run)
    era = dataset_pattern.split("/")[2]
    int_lumi_fb = int_lumi / 1000.0
    print "| !%s | %i | %i | %.3f |" % (era, first_run, last_run, int_lumi_fb)

    sys.stdout.flush()

