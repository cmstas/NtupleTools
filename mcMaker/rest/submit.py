import os
import glob
import time
import commands
import pickle

def main(fnames, executable, outputname, shortname, pset, cmsswversion, basedir):
    
    outputdir = "%s/%s/" % (basedir, shortname)

    done_candidates = []
    for fname in fnames:


        ifile = int(fname.rsplit("_",1)[-1].split(".")[0])

        cwd = os.getcwd()

        outputfile = "%s_%i.root" % (outputname, ifile)

        if (shortname, ifile) in complete_set:
            continue

        print "--> Working on %s" % (fname)

        if (shortname, ifile) in running_set:
            print "Job is running on condor, skipping"
            continue

        if os.path.isfile(outputdir+"/"+outputfile):
            print "Merged file already exists, not resubmitting"
            done_candidates.append( {"shortname":shortname,"ifile":ifile, "outputfile":outputfile, "fname":fname} )
            continue

        uid=os.getuid()
        timestamp=int(time.time())

        os.system("mkdir -p logs/{shortname}/submit/std_logs/".format(shortname=shortname))

# universe=Vanilla
# +DESIRED_Sites="T2_US_UCSD" 
# executable={executable}
# arguments={outputdir} {outputname} {inputfilename} {ifile} {pset} {cmsswversion}
# transfer_executable=True
# when_to_transfer_output = ON_EXIT
# transfer_input_files={pset}
# +Owner = undefined 
# log=logs/{shortname}/submit/log_{timestamp}.log
# output=logs/{shortname}/submit/std_logs//1e.$(Cluster).$(Process).out
# error =logs/{shortname}/submit/std_logs//1e.$(Cluster).$(Process).err
# notification=Never
# x509userproxy=/tmp/x509up_u{uid}
# should_transfer_files = yes
# +CMSSWCondor="yeah"
# +project_Name="cmssurfandturf"
# queue

        cfg = """
universe=grid
grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu
+remote_DESIRED_Sites="T2_US_UCSD,T2_US_Nebraska,T2_US_Wisconsin,T2_US_MIT" 
executable={executable}
arguments={outputdir} {outputname} {inputfilename} {ifile} {pset} {cmsswversion}
transfer_executable=True
when_to_transfer_output = ON_EXIT
transfer_input_files={pset}
+Owner = undefined 
log=logs/{shortname}/submit/log_{timestamp}.log
output=logs/{shortname}/submit/std_logs//1e.$(Cluster).$(Process).out
error =logs/{shortname}/submit/std_logs//1e.$(Cluster).$(Process).err
notification=Never
x509userproxy=/tmp/x509up_u{uid}
should_transfer_files = yes
+CMSSWCondor="yeah"
queue
        """.format(executable=executable,outputdir=outputdir,uid=uid,inputfilename=fname,ifile=ifile,outputname=outputname,pset=pset,shortname=shortname,timestamp=timestamp,cmsswversion=cmsswversion)

        with open("submit.cmd", "w") as fhout:
            fhout.write(cfg)

        os.system("condor_submit submit.cmd")

        print "Submitted job!"

    done_candidates = [dc for dc in done_candidates if (dc["shortname"], dc["ifile"]) not in running_set]
    print "Looping through %i done candidates" % len(done_candidates)
    new_complete_set = set()
    for dc in done_candidates:
        shortname = dc["shortname"]
        ifile = dc["ifile"]
        outputfile = dc["outputfile"]
        fname = dc["fname"]

        print "--> Finished %s" % outputfile

        new_complete_set.add( (shortname,ifile) )

    print "Finished %i new files" % len(new_complete_set)
    new_complete_set |= complete_set
    print "Updating completion list to have %i files total" % len(new_complete_set)
    update_complete_set(new_complete_set)

running_set = set()
def get_condor_running():
    stat, out = commands.getstatusoutput("condor_q $USER -autoformat ClusterId GridJobStatus EnteredCurrentStatus CMD ARGS -const 'CMSSWCondor == \"yeah\"'")
    lines = out.splitlines()

    for line in lines:
        parts = line.split()
        ifile = int(parts[7])
        shortname = parts[4].strip("/").split("/")[-1]

        running_set.add( (shortname,ifile) )

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

    tags = [
            "TTWW",
            ]

    # EDIT ME where all the output will go
    basedir = "/hadoop/cms/store/user/yourusername/"
    for tag in tags:
        # EDIT ME point to all the gensim root files as the entry point
        gendir = "/hadoop/cms/store/user/yourusername/blah/blah/70423*/0000/*.root".format(basedir,tag)
        d = {
            "basedir": "{0}".format(basedir),
            "fnames" : glob.glob(gendir),
            "executable" : "condorExecutable.sh",
            "outputname" : "RAW",
            "shortname" : "{0}_RAW".format(tag),
            "pset" : "pset_raw.py",
            "cmsswversion" : "CMSSW_8_0_21", # EDIT ME, what CMSSW is associated with this step?
        }
        main(**d)
        d = {
            "basedir": "{0}".format(basedir),
            "fnames" : glob.glob("{0}/{0}_RAW/*.root".format(basedir,tag)),
            "executable" : "condorExecutable.sh",
            "outputname" : "AOD",
            "shortname" : "{0}_AOD".format(tag),
            "pset" : "pset_aod.py",
            "cmsswversion" : "CMSSW_8_0_21", # EDIT ME, what CMSSW is associated with this step?
        }
        main(**d)
        d = {
            "basedir": "{0}".format(basedir),
            "fnames" : glob.glob("{0}/{0}_AOD/*.root".format(basedir,tag)),
            "executable" : "condorExecutable.sh",
            "outputname" : "MINIAOD",
            "shortname" : "{0}_MINIAOD".format(tag),
            "pset" : "pset_miniaod.py",
            "cmsswversion" : "CMSSW_8_0_21", # EDIT ME, what CMSSW is associated with this step?
        }
        main(**d)

