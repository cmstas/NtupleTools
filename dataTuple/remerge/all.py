import os
import commands
import sys
import glob


fnames=[
"/hadoop/cms/store/group/snt/run2_data/Run2016B_DoubleEG_MINIAOD_PromptReco-v2/merged/V08-00-06//merged_ntuple_209.root",
"/hadoop/cms/store/group/snt/run2_data/Run2016B_DoubleEG_MINIAOD_PromptReco-v2/merged/V08-00-06//merged_ntuple_714.root",
"/hadoop/cms/store/group/snt/run2_data/Run2016B_DoubleEG_MINIAOD_PromptReco-v2/merged/V08-00-06//merged_ntuple_749.root",
"/hadoop/cms/store/group/snt/run2_data/Run2016B_DoubleEG_MINIAOD_PromptReco-v2/merged/V08-00-06//merged_ntuple_91.root",
        ]


for fname in fnames:
    os.system("[ ! -d DataTuple-backup ] && git clone ssh://git@github.com/cmstas/DataTuple-backup")



    sample = fname.replace("//","/").split("/")[7]
    imerged = int(fname.split("merged_ntuple_")[-1].split(".root")[0])
    metadata_files = glob.glob("DataTuple-backup/*/mergedLists/%s/*_%i.txt" % (sample, imerged))

    if len(metadata_files) != 2:
        print "Error: did not find exactly 2 metadata files for %s" % fname
        continue

    os.system("cp %s ." % " ".join(metadata_files))

    output_folder = "/hadoop/cms/store/user/%s/dataTuple/%s/merged/" % (os.environ["USER"], sample)

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


    buff = ""
    buff += "############ DONE ###############\n"
    buff += "After verifying event counts, which should be %i, you can execute the following 3 lines:\n" % nevents
    buff += "    new=%s/merged_ntuple_%i.root\n" % (output_folder,imerged)
    buff += "    old=%s\n" % fname
    buff += "    rm $old; mv $new $old;\n"
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
