import json


# THIS ONLY MAKES THE FILELIST TO USE WITH THE AUTOTWOPLER. YOU WILL STILL NEED TO COPY THE MERGED FILE OUT OF THE FINAL HADOOP AREA (SAME AS DATASSET, BUT WITH Immaculate_<imerged#> IN FRONT OF IT

corrupt_files = ["/hadoop/cms/store/group/snt/run2_25ns_80MiniAODv1/QCD_HT700to1000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3_ext1-v1/V08-00-01/merged_ntuple_38.root"]

for cf in corrupt_files:
    metadata_fname = cf.split("merged_ntuple")[0]+"metadata.json"
    imerged = int(cf.split("merged_ntuple_")[-1].replace(".root",""))
    with open(metadata_fname,"r") as fhin:
        metadata = json.load(fhin)
        ijob_to_miniaod = metadata["ijob_to_miniaod"]
        imerged_to_ijob = metadata["imerged_to_ijob"]
        ijob_to_nevents = metadata["ijob_to_nevents"]
        dataset = metadata["dataset"]

        # print metadata["cms3tag"]
        # print metadata["gtag"]
        # print metadata["xsec"]
        # print metadata["kfact"]
        # print metadata["efact"]
        # print ",".join(metadata["sparms"])

        new_dataset = "/Immaculate_"+str(imerged)+"_"+dataset[1:]
        ijobs = imerged_to_ijob[str(imerged)]

        miniaods = sum([ijob_to_miniaod[str(ijob)] for ijob in ijobs],[])

        merged_nevents = sum([ijob_to_nevents[str(ijob)][0] for ijob in ijobs])

        print "____ put the lines below the __s into a file. note that the CMS3tag used was %s ______" % metadata["cms3tag"]
        print "____ instructions.txt line: <filename> %s %s %s %s %s ______" % \
                (metadata["gtag"], metadata["xsec"], metadata["kfact"], metadata["efact"], ",".join(metadata["sparms"]))
        print "___________________________________________________"
        print "dataset: %s" % new_dataset
        for miniaod in miniaods:
            print miniaod
        print
        print 

