import os
import glob
import time
import commands
import pickle
from ROOT import TChain

def check(old, new):
    ch_old = TChain("Events")
    ch_old.Add(old)

    ch_new = TChain("Events")
    ch_new.Add(new)

    n_old = ch_old.GetEntries()
    n_new = ch_new.GetEntries()
    
    size_old = os.path.getsize(old)
    size_new = os.path.getsize(new)

    isgood = (n_old == n_new) and (size_new > 0.9999*size_old)
    return isgood, (n_old, n_new), (size_old, size_new)



def main():
    os.system("[ ! -d DataTuple-backup ] && git clone ssh://git@github.com/cmstas/DataTuple-backup")
    fnames = [
    ]
    fnames.extend( [fname.strip() for fname in open("filelist.txt","r").readlines() if len(fname.strip())>10] )

    done_candidates = []
    for fname in fnames:


        cwd = os.getcwd()
        shortname = fname.replace("//","/").split("/")[7]
        imerged = int(fname.split("merged_ntuple_")[-1].split(".root")[0])
        outputdir = "/hadoop/cms/store/user/%s/dataTuple/%s/merged/" % (os.environ["USER"], shortname)
        outputfile = "%s/merged_ntuple_%i.root" % (outputdir, imerged)

        metadata_files = glob.glob("DataTuple-backup/*/mergedLists/%s/*_%i.txt" % (shortname, imerged))
        metadata = cwd+"/"+[mf for mf in metadata_files if "metaData_" in mf][0]
        mergedlist = cwd+"/"+[mf for mf in metadata_files if "merged_list_" in mf][0]

        if (shortname, imerged) in complete_set:
            continue

        print "--> Working on %s" % (fname)

        if len(metadata_files) != 2:
            print "Error: did not find exactly 2 files for %s" % fname
            continue

        if (shortname, imerged) in running_set:
            print "Job is running on condor, skipping"
            continue

        if os.path.isfile(outputfile):
            print "Merged file already exists, not resubmitting"
            done_candidates.append( {"shortname":shortname,"imerged":imerged, "outputfile":outputfile, "fname":fname} )
            continue

        executable =  "%s/mergeScriptRoot6.sh" % (cwd)
        mergescript = "%s/mergeScript.C" % (cwd)
        addbranches = "%s/addBranches.C" % (cwd)
        sweeproot = "sweepRoot.tar.gz"
        uid=os.getuid()
        timestamp=int(time.time())

        os.system("mkdir -p logs/{shortname}/submit/std_logs/".format(shortname=shortname))

        cfg = """
universe=grid
grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu
+remote_DESIRED_Sites="T2_US_UCSD" 
executable={executable}
arguments={mergedlist} {metadata} {outputdir}
transfer_executable=True
when_to_transfer_output = ON_EXIT
transfer_input_files={mergedlist},{metadata},{executable},{mergescript},{sweeproot},{addbranches}
+Owner = undefined 
log=logs/{shortname}/submit/testlog_{timestamp}.log
output=logs/{shortname}/submit/std_logs//1e.$(Cluster).$(Process).out
error =logs/{shortname}/submit/std_logs//1e.$(Cluster).$(Process).err
notification=Never
x509userproxy=/tmp/x509up_u{uid}
should_transfer_files = yes
+DataTupleRemerge="yeah"
queue
        """.format(executable=executable,mergescript=mergescript,addbranches=addbranches,mergedlist=mergedlist,metadata=metadata,outputdir=outputdir,sweeproot=sweeproot,uid=uid,shortname=shortname,timestamp=timestamp)

        with open("submit.cmd", "w") as fhout:
            fhout.write(cfg)

        os.system("condor_submit submit.cmd")

        print "Submitted job!"

    done_candidates = [dc for dc in done_candidates if (dc["shortname"], dc["imerged"]) not in running_set]
    print "Looping through %i done candidates" % len(done_candidates)
    new_complete_set = set()
    for dc in done_candidates:
        shortname = dc["shortname"]
        imerged = dc["imerged"]
        outputfile = dc["outputfile"]
        fname = dc["fname"]


        print "--> Considering %s" % outputfile

        isgood, (n_old, n_new), (size_old, size_new) = check(fname, outputfile)

        if not isgood:
            print "Ntuple is bad, deleting"
            print "Event counts (old,new): (%i,%i)" % (n_old, n_new)
            print "Sizes (old,new): (%i,%i)" % (size_old, size_new)
            os.system("rm %s" % outputfile)
            continue

        # do copy and stuff
        old = fname.replace("/hadoop","")
        new = outputfile.replace("/hadoop","")
        print "  moving: hadoop fs -rm -f {old} && hadoop fs -mv {new} {old}".format(new=new,old=old)
        os.system("hadoop fs -rm -f {old} && hadoop fs -mv {new} {old}".format(new=new,old=old))
        

        new_complete_set.add( (shortname,imerged) )

    print "Finished %i new files" % len(new_complete_set)
    new_complete_set |= complete_set
    print "Updating completion list to have %i files total" % len(new_complete_set)
    update_complete_set(new_complete_set)




running_set = set()
def get_condor_running():
    stat, out = commands.getstatusoutput("condor_q $USER -autoformat ClusterId GridJobStatus EnteredCurrentStatus CMD ARGS -const 'DataTupleRemerge == \"yeah\"'")
    lines = out.splitlines()
    # lines = ["260440 IDLE 1475111226 /home/users/namin/dataTuple/2016G/NtupleTools/dataTuple/remerge/mergeScriptRoot6.sh /home/users/namin/dataTuple/2016G/NtupleTools/dataTuple/remerge/DataTuple-backup/mark/mergedLists/Run2016G_SinglePhoton_MINIAOD_PromptReco-v1/merged_list_41.txt /home/users/namin/dataTuple/2016G/NtupleTools/dataTuple/remerge/DataTuple-backup/mark/mergedLists/Run2016G_SinglePhoton_MINIAOD_PromptReco-v1/metaData_41.txt /hadoop/cms/store/user/namin/dataTuple/Run2016G_SinglePhoton_MINIAOD_PromptReco-v1/merged/"]

    for line in lines:
        parts = line.split()
        imerged = int(parts[4].replace(".txt","").rsplit("_",1)[-1])
        shortname = parts[6].replace("//","/").split("/")[7]

        running_set.add( (shortname,imerged) )

complete_set = set()
def get_complete_set():
    if os.path.isfile("complete.pkl"):
        with open("complete.pkl","r") as fhin:
            complete_set.update(pickle.load(fhin)["set"])
            print "Loaded set of %i completed files" % len(complete_set)

def update_complete_set(s):
    with open("complete.pkl","w") as fhout:
        pickle.dump({"set":s},fhout)


if __name__ == "__main__":
    get_complete_set()
    get_condor_running()
    main()

