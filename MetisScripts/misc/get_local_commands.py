
from metis.Utils import condor_q, do_cmd
from itertools import izip_longest

def get_global_tag(psetname):
    return do_cmd("""tac {} | grep process.GlobalTag.globaltag | head -1 | cut -d '"' -f2""".format(psetname))

def print_commands(cids, localcache=False):
    todownload = []
    for job in condor_q(cluster_id=cids, extra_columns=["taskname","jobnum","metis_retries"]):
        args = job["ARGS"]
        taskname = job["taskname"]
        jobnum = job["jobnum"]
        # if int(job["metis_retries"]) < 2: continue
        parts = args.split(" ",11)
        outdir = parts[0]
        infiles = parts[2]
        index = parts[3]
        executable = job["CMD"]
        pset = executable.replace("executable.sh","pset.py")
        globaltag = get_global_tag(pset)
        outname = "{}_{}.root".format(parts[1],index)
        psetargs = parts[-1]
        if localcache:
            todownload.append(infiles.split(","))
            infiles = infiles.replace("/store/", "file:/hadoop/cms/store/user/namin/localcache/")
        # print "mkdir -p {taskname} ; cd {taskname} ; cp ../pset.py .".format(taskname=taskname)
        # print "cmsRun pset.py inputs={infiles} output={outname} globaltag={globaltag} {psetargs} >& log_{index}.txt &".format(infiles=infiles,outname=outname,globaltag=globaltag,psetargs=psetargs,index=index)
        # print """# gfal-copy -p -f -t 4200 --verbose file://`pwd`/{outname} gsiftp://gftp.t2.ucsd.edu{outdir}{outname} --checksum ADLER32""".format(outname=outname, outdir=outdir)
        # print """# condor_rm -const 'taskname=="{taskname}" && jobnum=="{jobnum}"' """.format(taskname=taskname,jobnum=jobnum)
        # print "cd .."
        # print

        print "mkdir -p {taskname}".format(taskname=taskname)
        print "(".format(taskname=taskname)
        print "cd {taskname} ; cp ../pset.py .".format(taskname=taskname)
        print "cmsRun -n 1 pset.py inputs={infiles} output={outname} globaltag={globaltag} {psetargs} && ".format(infiles=infiles,outname=outname,globaltag=globaltag,psetargs=psetargs)
        print "gfal-copy -p -f -t 4200 --verbose file://`pwd`/{outname} gsiftp://gftp.t2.ucsd.edu{outdir}{outname} --checksum ADLER32".format(outname=outname, outdir=outdir)
        print ") >& {taskname}/log_{index}.txt &".format(taskname=taskname,index=index)
        print """# condor_rm -const 'taskname=="{taskname}" && jobnum=="{jobnum}"' """.format(taskname=taskname,jobnum=jobnum)
        print

    if todownload:
        # transpose (so first element is the first file for each job, second is second, etc.; pad with None)
        todownload = map(list,izip_longest(*todownload))
        # get single list filtering out nontruthy stuff
        todownload = filter(None,sum(todownload,[]))
        print "\n".join(todownload)


if __name__ == "__main__":
    # print_commands("546478.0 546478.9")

    selstr = ""
    # selstr = "547165.230 547454.0 547453.0  547553.0 "
    # selstr = "549298.3"

    cids = " ".join(do_cmd("condor_q -w -nobatch %s | grep Run201 | awk '{print $1}'" % selstr).split())
    # print_commands(cids)
    print_commands(cids, localcache=True)
