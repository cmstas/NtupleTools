import dis_client
import sys, os, time
from multiprocessing.dummy import Pool as ThreadPool 


def get_status(ds):
    status = "unknown"
    try: status = dis_client.query(ds+",this", typ="mcm")["response"]["payload"]["status"]
    except: pass
    sys.stdout.write(".")
    sys.stdout.flush()
    return status

if __name__ == "__main__":

    campaigns = ["RunIISpring16MiniAODv1", "RunIISpring16MiniAODv2"]
    old_fname = "past_samples.txt"

    all_datasets = []
    for campaign in campaigns:
        output = dis_client.query("/*/*%s*/MINIAODSIM" % campaign)
        all_datasets.extend( output["response"]["payload"] )

    print "Found %i total datasets on DAS for campaigns: %s" % (len(all_datasets), ", ".join(campaigns))

    datasets = []
    cut_strs = ["RelVal", "Radion", "/GG_", "/RS", "X53", "Grav", "Tstar", "Bstar", "LQ", "Wprime", \
                "Zprime", "Bprime", "Tprime", "_FlatPt", "-gun", "DarkMatter", "DM", "ChargedHiggs", \
                "DisplacedSUSY", "GGJets", "GluGlu", "NNPDF", "LFV", "ToGG", "WToTauNu_M-", "WToMuNu_M-", \
                "WToENu_M-", "XXTo4J", "HToZATo", "SMS-T2bH", "VBFHToTauTau", "VBF_HToMuMu", "VBF_HToZZTo4L" \
                "WJetsToQQ"]
    for dataset in all_datasets:
        for cut_str in cut_strs:
            if cut_str in dataset: break
        else: datasets.append(dataset)

    print "After removing stupid samples, %i datasets remain" % len(datasets)

    old_datasets = []
    if os.path.isfile(old_fname):
        with open(old_fname, "r") as fhin:
            for line in fhin:
                line = line.strip()
                if len(line) < 2: continue
                utc, dataset = line.split()
                old_datasets.append(dataset)
    else:
        print "Looks like there's file with previously fetched datasets, so we'll have to make queries for all of them the first time. This might take a few minutes."

    datasets = list(set(datasets) - set(old_datasets))
    print "After further removing samples we've previously considered in the past, we are down to %i datasets" % len(datasets)

    statuses = []
    if datasets:
        pool = ThreadPool(8)
        statuses = pool.map(get_status, datasets)
        pool.close()
        pool.join()

    good_datasets = []
    good_datasets = sorted([ds for ds, status in zip(datasets,statuses) if status == "done"])


    twiki_data = dis_client.query("*", typ="snt")["response"]["payload"]
    twiki_datasets = [td.get("dataset_name") for td in twiki_data]

    print
    print "======= %i remaining good datasets in 'done' status =======" % len(good_datasets)
    with open(old_fname, "a") as fhout:
        for dataset in good_datasets:
            if dataset in twiki_datasets:
                print "on twiki -",
            else:
                print "          ",
            print dataset
            fhout.write("%i %s\n" % (int(time.time()), dataset))

