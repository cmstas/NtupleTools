import os
import commands
import sys
import glob
from ROOT import TFile, TTree, TChain

def check(old, new):
    ch_old = TChain("Events")
    ch_old.Add(old)

    ch_new = TChain("Events")
    ch_new.Add(new)

    n_old = ch_old.GetEntries()
    n_new = ch_new.GetEntries()
    
    size_old = os.path.getsize(old)
    size_new = os.path.getsize(new)

    # print n_old, n_new
    # print size_old, size_new

    isgood = (n_old == n_new) and (size_new > size_old)
    return isgood, (n_old, n_new), (size_old, size_new)




fnames=[
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_129.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_142.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_152.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_154.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_239.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_248.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_251.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_326.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_349.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_352.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_359.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_78.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_152.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_154.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_162.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_163.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_212.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_215.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_50.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_67.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_68.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_7.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_74.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_DoubleMuon_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_88.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_113.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_124.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_148.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_161.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_3.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_30.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_56.root",
        # "/hadoop/cms/store/group/snt/run2_data/Run2016G_MuonEG_MINIAOD_PromptReco-v1/merged//V08-00-12/merged_ntuple_62.root",
        ]


for fname in fnames:
    os.system("[ ! -d DataTuple-backup ] && git clone ssh://git@github.com/cmstas/DataTuple-backup")


    print "Working on %s" % (fname)

    sample = fname.replace("//","/").split("/")[7]
    imerged = int(fname.split("merged_ntuple_")[-1].split(".root")[0])
    output_folder = "/hadoop/cms/store/user/%s/dataTuple/%s/merged/" % (os.environ["USER"], sample)
    output_file = "%s/merged_ntuple_%i.root" % (output_folder, imerged)


    if os.path.isfile(output_file): 
        isgood, (n_old, n_new), (size_old, size_new) = check(fname, output_file)
        if isgood: 
            print "File already exists and is good, so not redoing!"
            continue
        else:
            print "Redoing since file is not good:", n_old, n_new, size_old, size_new
    else:
        print "REMAKING!"


    metadata_files = glob.glob("DataTuple-backup/*/mergedLists/%s/*_%i.txt" % (sample, imerged))

    if len(metadata_files) != 2:
        print "Error: did not find exactly 2 metadata files for %s" % fname
        continue

    os.system("cp %s ." % " ".join(metadata_files))


    nevents = -1
    with open([mf for mf in metadata_files if "metaData_" in mf][0], "r") as fh:
        for line in fh:
            if line.startswith("n:"):
                nevents = int(line.strip().split()[-1])
                break
    # with open(

    cmd = "./mergeScriptRoot6.sh $(pwd)/merged_list_{imerged}.txt $(pwd)/metaData_{imerged}.txt {output_folder}".format(imerged=imerged, output_folder=output_folder)

    print cmd

    os.system(cmd)

    isgood, (n_old, n_new), (size_old, size_new) = check(fname, output_file)
    if not isgood:
        print "BAD!"
        continue


    buff = ""
    buff += "############ DONE ###############\n"
    buff += "# After verifying event counts, which should be %i, you can execute the following 3 lines:\n" % nevents
    buff += "    new=%s/merged_ntuple_%i.root\n" % (output_folder,imerged)
    buff += "    old=%s\n" % fname
    buff += '    if [ -e "$old" ] && [ -e "$new" ]; then (rm -f $old; mv $new $old); fi \n'
    buff += "############ DONE ###############\n"

    print buff

    with open("done_stuff.txt", "a") as fhout:
        fhout.write(buff)

# imerged=199
# outputfolder=/hadoop/cms/store/user/namin/dataTuple/Run2016B_MET_MINIAOD_PromptReco-v2/merged/
# ./mergeScriptRoot6.sh $(pwd)/merged_list_${imerged}.txt $(pwd)/metaData_${imerged}.txt $outputfolder

    # print sample
    # print imerged
    # print metadata_files


    # extract sample name and merged number from filename
    # sample=$( echo $fname | sed 's#/# #g' | awk '{print $7}' )
    # imerged=$( echo $fname | sed 's#/# #g' | awk '{print $10}' | cut -d '_' -f3 | cut -d'.' -f1)

    # # is it inside the backup directory?
    # metadataFiles=$(ls -1 DataTuple-backup/*/mergedLists/${sample}/*_${imerged}.txt)

    # echo $sample
    # echo $imerged
    # echo $metadataFiles

    # # want exactly 2 metadata files (one is metaData_*.txt and other is merged_list_*.txt)
    # [ $(ntokens $metadataFiles) == "2" ] || die "didn't find two metadata files for $fname"



    # outdir="/hadoop/cms/store/user/${USER}/dataTuple/${sample}/merged/"

    # # sample=$(echo $fname | 
    # # DataTuple-backup/nick/mergedLists/Run2016B_DoubleEG_MINIAOD_PromptReco-v2/metaData_658.txt
