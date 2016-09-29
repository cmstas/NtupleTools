import os
import glob
import time
import commands


def main():
    os.system("[ ! -d DataTuple-backup ] && git clone ssh://git@github.com/cmstas/DataTuple-backup")
    fnames = [
            "/hadoop/cms/store/group/snt/run2_data/Run2016G_SinglePhoton_MINIAOD_PromptReco-v1/merged/V08-00-12/merged_ntuple_41.root ",
            ]

    for fname in fnames:

        print "Working on %s" % (fname)

        cwd = os.getcwd()
        shortname = fname.replace("//","/").split("/")[7]
        imerged = int(fname.split("merged_ntuple_")[-1].split(".root")[0])
        outputdir = "/hadoop/cms/store/user/%s/dataTuple/%s/merged/" % (os.environ["USER"], shortname)
        outputfile = "%s/merged_ntuple_%i.root" % (outputdir, imerged)

        metadata_files = glob.glob("DataTuple-backup/*/mergedLists/%s/*_%i.txt" % (shortname, imerged))
        metadata = cwd+"/"+[mf for mf in metadata_files if "metaData_" in mf][0]
        mergedlist = cwd+"/"+[mf for mf in metadata_files if "merged_list_" in mf][0]

        if len(metadata_files) != 2:
            print "Error: did not find exactly 2 files for %s" % fname
            continue

        if os.path.isfile(outputfile):
            print "Merged file already exists, not resubmitting"
            continue

        if (shortname, imerged) in running_set:
            print "Job is running on condor, skipping"
            continue

        executable =  "%s/mergeScriptRoot6.sh" % (cwd)
        mergescript = "%s/mergeScript.C" % (cwd)
        addbranches = "%s/addBranches.C" % (cwd)
        sweeproot = "sweepRoot.tar.gz"
        uid=os.getuid()
        timestamp=int(time.time())

        os.system("mkdir -p logs/{shortname}/submit/std_logs/".format(shortname=shortname))

        sub = """
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
+DataTupleRemerge=yeah
queue
        """.format(executable=executable,mergescript=mergescript,addbranches=addbranches,mergedlist=mergedlist,metadata=metadata,outputdir=outputdir,sweeproot=sweeproot,uid=uid,shortname=shortname,timestamp=timestamp)
            
        print sub


running_set = set()
def get_condor_running():
    stat, out = commands.getstatusoutput("""condor_q $USER -autoformat ClusterId GridJobStatus EnteredCurrentStatus CMD ARGS -const 'DataTupleRemerge == "yeah"'""")
    lines = out.splitlines()
    lines = ["260440 IDLE 1475111226 /home/users/namin/dataTuple/2016G/NtupleTools/dataTuple/remerge/mergeScriptRoot6.sh /home/users/namin/dataTuple/2016G/NtupleTools/dataTuple/remerge/DataTuple-backup/mark/mergedLists/Run2016G_SinglePhoton_MINIAOD_PromptReco-v1/merged_list_41.txt /home/users/namin/dataTuple/2016G/NtupleTools/dataTuple/remerge/DataTuple-backup/mark/mergedLists/Run2016G_SinglePhoton_MINIAOD_PromptReco-v1/metaData_41.txt /hadoop/cms/store/user/namin/dataTuple/Run2016G_SinglePhoton_MINIAOD_PromptReco-v1/merged/"]

    for line in lines:
        parts = line.split()
        imerged = int(parts[4].replace(".txt","").rsplit("_",1)[-1])
        shortname = parts[6].replace("//","/").split("/")[7]

        running_set.add( (shortname,imerged) )

get_condor_running()
main()
print running_set

