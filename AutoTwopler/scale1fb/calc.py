import json
import glob
import sys
import datetime

sntdir = "run2_moriond17_cms4"
cmstag = "V00-00-01"

sampledirs = glob.glob("/hadoop/cms/store/group/snt/{}/*/{}/".format(sntdir,cmstag))

print "# file created on %s" % (datetime.datetime.now())
# print "# {:153s} , {:17s} , {:9s} , {:9s} , {:8s} , {:10s}".format("dataset","tag","nevts_tot","nevts_eff","xsec","scale1fb")
print "# {:153s}  {:17s}  {:9s}  {:9s}  {:8s}  {:10s}".format("dataset","tag","nevts_tot","nevts_eff","xsec","scale1fb")
for sampledir in sampledirs:
    metadata = sampledir + "/metadata.json"
    with open(metadata,"r") as fhin:
        data = json.load(fhin)
        ijob_to_nevents = data["ijob_to_nevents"]
        tag = data["cms3tag"]
        dataset = data["dataset"]
        kfact = data["kfact"]
        xsec = data["xsec"]
        efact = data["efact"]
        file_indices = map(lambda x: x.rsplit("_",1)[-1].split(".")[0], glob.glob(sampledir+"/*.root"))
        nevts_total = 0
        nevts_eff_total = 0
        for fi in file_indices:
            nevts, nevts_eff = ijob_to_nevents[fi]
            nevts_total += nevts
            nevts_eff_total += nevts_eff
        xsec_total = kfact*xsec*efact
        scale1fb = 1000.0*xsec_total/nevts_eff_total
        # print "{:155s} , {:17s} , {:9} , {:9} , {:8.5g} , {:10.5g}".format(dataset,tag,nevts_total,nevts_eff_total,xsec_total,scale1fb)
        print "{:155s}  {:17s}  {:9}  {:9}  {:8.5g}  {:10.5g}".format(dataset,tag,nevts_total,nevts_eff_total,xsec_total,scale1fb)


