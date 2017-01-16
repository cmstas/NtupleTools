import os
import sys
import commands
import pycurl 
import StringIO 
import ast
import datetime
import params
import urllib2
import json
import logging
import re
import time
from collections import defaultdict


def get(cmd, returnStatus=False):
    status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def cmd(cmd, returnStatus=False):
    status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def proxy_hours_left():
    try:
        info = get("voms-proxy-info")
        hours = int(info.split("timeleft")[-1].strip().split(":")[1])
    except: hours = 0
    return hours

def copy_jecs():
    if not os.path.isfile(params.jecs):
        os.system("cp /nfs-7/userdata/JECs/%s ." % params.jecs)

def get_web_dir(overriden_params=None):
    if overriden_params:
        return "%s/public_html/%s/" % (os.getenv("HOME"), overriden_params.dashboard_name)
    else:
        return "%s/public_html/%s/" % (os.getenv("HOME"), params.dashboard_name)

def make_dashboard(web_dir=None):
    if not web_dir:
        web_dir = get_web_dir()
    if not os.path.isdir(web_dir): os.makedirs(web_dir)
    cmd("chmod 755 %s/*.py" % web_dir)
    cmd("chmod 755 -R %s" % web_dir)
    cmd("cp -rp %s/dashboard/* %s/" % (os.path.abspath(__file__).rsplit("/",1)[0],web_dir))
    # cmd("sed -i s#BASEDIR_PLACEHOLDER#%s/# %s/main.js" % (os.getcwd(), web_dir))
    cmd("sed -i \"s,^var baseDir = .*$,var baseDir = '%s';,g\" %s/main.js" % (os.getcwd()+"/", web_dir))
    print "http://uaf-6.t2.ucsd.edu/~%s/%s" % (os.getenv("USER"), web_dir.split("public_html/")[1])

def copy_json(overriden_params=None):
    web_dir = get_web_dir(overriden_params)
    if not os.path.isfile(web_dir+"/index.html"):
        make_dashboard(web_dir)
    cmd("cp data.json %s/" % web_dir)

def read_samples(instructions="instructions.txt"):
    # if user fed in a list of dicts, then just make this the 
    # already-parsed object and return it
    if type(instructions) == list:
        return instructions

    samples = []
    with open(instructions, "r") as fhin:
        for line in fhin.readlines():
            line = line.strip()
            if len(line) < 5: continue
            if line[0] == "#": continue
            parts = line.strip().split()
            if len(parts) < 5: continue

            if parts[0].strip().lower() == "baby":
                analysis, baby_tag, dataset, package, executable = parts[1:6]
                if len(parts) > 6:
                    extra = parts[6:]
                else:
                    extra = ''
                sample = {
                  "analysis": analysis,
                  "dataset": dataset,
                  "baby_tag": baby_tag,
                  "package": package,
                  "executable": executable,
                  "type": "BABY",
                  "extra": extra,
                }

            else:
                dataset, gtag, xsec, kfact, efact = parts[:5]
                sample = {
                        "dataset": dataset,
                        "gtag": gtag,
                        "kfact": float(kfact),
                        "efact": float(efact),
                        "xsec": float(xsec),
                        "type": "CMS3",
                        }
                if len(parts) >= 6: sample["sparms"] = "".join(parts[5:]).split(",")

                if dataset.endswith(".txt"):
                    d_parsed = parse_filelist(dataset)
                    if not d_parsed: continue

                    for key in d_parsed: sample[key] = d_parsed[key]

            samples.append(sample)
    return samples

def parse_filelist(filename):
    dataset = None
    files_per_job = 1
    filelist = []
    nevents, nevents_effective = None, None
    with open(filename, "r") as fhin:
        for line in fhin.readlines():
            line = line.strip()
            if len(line) < 1: continue
            if line[0] == "#": continue

            if line.startswith("dataset:"): dataset = line.split(":")[-1].strip()
            elif line.startswith("files_per_job:"): files_per_job = int(line.split(":")[-1].strip())
            elif line.startswith("nevents:"): nevents = int(line.split(":")[-1].strip())
            elif line.startswith("nevents_effective:"): nevents_effective = int(line.split(":")[-1].strip())
            elif line.endswith(".root"): filelist.append(line)

    if not dataset or not filelist:
        return { }
    else:
        # return {"dataset": dataset, "extra": {"files_per_job": files_per_job, "filelist": filelist, "nevents": nevents, "nevents_effective": nevents_effective}}
        ret = {"dataset": dataset, "extra": {"files_per_job": files_per_job, "filelist": filelist}}
        if nevents and nevents_effective:
            ret["extra"]["nevents"] = nevents
            ret["extra"]["nevents_effective"] = nevents_effective
        return ret


def proxy_renew():
    # http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/LongLivedProxy
    cert_file = "/home/users/{0}/.globus/proxy_for_{0}.file".format(os.getenv("USER"))
    if os.path.exists(cert_file): cmd("voms-proxy-init -q -voms cms -hours 120 -valid=120:0 -cert=%s" % cert_file)
    else: cmd("voms-proxy-init -hours 9876543:0 -out=%s" % cert_file)

def get_proxy_file():
    cert_file = '/tmp/x509up_u%s' % str(os.getuid()) # TODO: check that this is the same as `voms-proxy-info -path`
    return cert_file

def get_timestamp():
    # return current time as a unix timestamp
    return int(datetime.datetime.now().strftime("%s"))

def get_dbs_url(url):
    # get json from a dbs url (API ref is at https://cmsweb.cern.ch/dbs/prod/global/DBSReader/)
    #
    # 3 hours of work to figure out how the crab dbs api works and get this to work with only `cmsenv`....
    # can't use urllib2 since x509 got supported after 2.7.6
    # can't use requests because that doesn't come with cmsenv
    b = StringIO.StringIO() 
    c = pycurl.Curl() 
    cert = get_proxy_file()
    c.setopt(pycurl.WRITEFUNCTION, b.write) 
    c.setopt(pycurl.CAPATH, '/etc/grid-security/certificates') 
    c.unsetopt(pycurl.CAINFO)
    c.setopt(pycurl.SSLCERT, cert)

    c.setopt(pycurl.URL, url) 
    c.perform() 
    try:
        s = b.getvalue().replace("null","None")
        ret = ast.literal_eval(s)
        return ret
    except:
        return {}

def dataset_event_count(dataset):
    # get event count and other information from dataset

    instance = "global"
    if dataset.endswith("/USER"): instance = "phys03"
    url = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/filesummaries?dataset=%s&validFileOnly=1" % (instance,dataset)
    ret = get_dbs_url(url)
    if len(ret) > 0:
        return { "nevents": ret[0]['num_event'], "filesize": ret[0]['file_size'], "nfiles": ret[0]['num_file'], "nlumis": ret[0]['num_lumi'] }
    return None

def dataset_event_count_2(dataset):
    # alternative to dataset_event_count() (this uses dbs client as opposed to rolling our own)
    # cmd(". /cvmfs/cms.cern.ch/crab3/crab-env-bootstrap.sh >& /dev/null")
    from dbs.apis.dbsClient import DbsApi
    url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    api=DbsApi(url=url)
    output = api.listDatasets(dataset=dataset)

    if(len(output)==1):
        inp = output[0]['dataset']
        info = api.listFileSummaries(dataset=inp)[0]
        filesize = info['file_size']
        nevents = info['num_event']
        nlumis = info['num_lumi']
        files = api.listFiles(dataset=dataset, detail=1, validFileOnly=1)
        return {"nevents": nevents, "filesize": filesize, "nfiles": len(files), "nlumis": nlumis}

    return {}

def get_dataset_files(dataset):
    # return list of 3-tuples (LFN, nevents, size_in_GB) of files in a given dataset
    url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/files?dataset=%s&validFileOnly=1&detail=1" % dataset
    ret = get_dbs_url(url)
    files = []
    for f in ret:
        files.append( [f['logical_file_name'], f['event_count'], f['file_size']/1.0e9] )
    return files

def get_dataset_parent(dataset):
    # get parent of a given dataset
    ret = get_dbs_url("https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasetparents?dataset=%s" % dataset)
    if len(ret) < 1: return None
    return ret[0].get('parent_dataset', None)

def get_gen_sim(dataset):
    # recurses up the tree of parent datasets until it finds the gen_sim dataset
    while "GEN-SIM" not in dataset:
        dataset = get_dataset_parent(dataset)
        if not dataset: break
    return dataset if "GEN-SIM" in dataset else None

def get_mcm_json(dataset):
    # get McM json for given dataset
    url = "https://cms-pdmv.cern.ch/mcm/public/restapi/requests/produces/"+dataset
    mcm_json = json.load(urllib2.urlopen(url))
    return mcm_json

def get_slim_mcm_json(dataset):
    out = {}
    mcm_json = get_mcm_json(dataset)
    
    try: out['cross_section'] = mcm_json['results']['generator_parameters'][-1]['cross_section']
    except: pass

    try: out['filter_efficiency'] = mcm_json['results']['generator_parameters'][-1]['filter_efficiency']
    except: pass

    try: out['cmssw_release'] = mcm_json['results']['cmssw_release']
    except: pass

    try: out['mcdb_id'] = mcm_json['results']['mcdb_id']
    except: pass

    return out

def get_hadoop_name():
    # handle non-standard hadoop name mapping
    user = os.getenv("USER")
    if user == "dsklein": user = "dklein"
    elif user == "iandyckes": user = "gdyckes"
    elif user == "mderdzinski": user = "mderdzin"
    elif user == "rclsa": user = "rcoelhol"
    return user


def sum_dicts(dicts):
    # takes a list of dicts and sums the values
    ret = defaultdict(int)
    for d in dicts:
        for k, v in d.items():
            ret[k] += v
    return dict(ret)

def get_last_n_lines(fname, N=50):
    # get last n lines of a text file
    if os.path.isfile(fname):
        return get('tail -n %i %s' % (N, fname))
    else:
        return ""

def setup_logger():
    # set up the logger to use it within run.py and Samples.py
    logger_name = params.log_file.replace(".","_")
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(params.log_file)
    fh.setLevel(logging.INFO) # INFO level to logfile
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG) # DEBUG level to console
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('[%(asctime)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger_name

def get_crabcache_info():
    used_space = round(float(get_dbs_url("https://cmsweb.cern.ch/crabcache/info?subresource=userinfo")['result'][0]['used_space'][0]/1.0e9),2)
    file_list = get_dbs_url("https://cmsweb.cern.ch/crabcache/info?subresource=userinfo")['result'][0]['file_list']
    patt = re.compile("^[a-z0-9]*$")

    file_hashes = []
    for f in file_list:
        if not patt.match(f): continue
        file_hashes.append(f)

    return {"file_hashes": file_hashes, "used_space_GB": used_space}

def purge_crabcache(verbose=False):
    d = get_crabcache_info()
    hashkeys = d["file_hashes"]
    for hkey in hashkeys:
        url = "https://cmsweb.cern.ch/crabcache/info?subresource=fileremove&hashkey=%s" % hkey
        success = bool(get_dbs_url(url))
        if success and verbose: 
             "successfully purged file %s" % hkey


def get_actions(actions_fname="actions.txt", dataset_name=None):
    if not os.path.isfile(actions_fname): return []

    with open(actions_fname, "r") as fhin:
        actions = []
        for line in fhin:
            if ":" not in line: continue
            dataset = line.split(":")[0].strip()
            action = line.split(":")[1].strip()

            if dataset_name and dataset != dataset_name: continue
            actions.append( [dataset,action] )

    return actions

def consume_actions(dataset_name, action, actions_fname="actions.txt"):
    if not os.path.isfile(actions_fname): return

    lines = []
    with open(actions_fname, "r") as fhin:
        lines = fhin.readlines()

    with open(actions_fname, "w") as fhout:
        for line in lines:
            if ":" not in line: continue
            dataset = line.split(":")[0].strip()
            act = line.split(":")[1].strip()

            if dataset == dataset_name and act == action:
                print ">>> Consumed action for %s: %s" % (dataset, action)
                continue

            fhout.write(line)

def smart_sleep(t, files_to_watch=[]):
    # sleeps for t seconds in ten 0.1*t second chunks
    # if any file in files_to_watch is modified during one of
    # these chunks, return prematurely
    if t < 60:
        time.sleep(t)
        return

    tenth = 0.1*t
    for i in range(10):
        time.sleep(tenth)
        for fname in files_to_watch:
            if type(fname) != str: continue
            if os.path.isfile(fname) and time.time()-os.path.getmtime(fname) <= tenth:
                return
    return

def send_email(dataset):
    email = get("git config --list | grep 'user.email' | cut -d '=' -f2")
    firstname = get("git config --list | grep 'user.name' | cut -d '=' -f2 | cut -d ' ' -f1")
    if "@" not in email:
        return

    subject = "[UAFNotify] AutoTwopler job finished on %s" % datetime.datetime.now().strftime("%-m/%d at %-I:%M%p")
    dashboard_url = "http://uaf-7.t2.ucsd.edu/~%s/%s/" % (os.getenv("USER"), params.dashboard_name)

    body = "Hi {0},\n\n" \
           "    Dataset finished: {1}\n" \
           "    Monitoring page URL: {2}\n\n" \
           "Cheers!\n" \
           "A. Script"
    body = body.format(firstname, dataset, dashboard_url)

    cmd("echo '%s' | mail -s '%s' %s" % (body, subject, email))

def get_shortname_from_dataset(dataset):
    return dataset.split("/")[1]+"_"+dataset.split("/")[2]


if __name__=='__main__':

    if proxy_hours_left() < 5:
        print "Proxy near end of lifetime, renewing."
        proxy_renew()
    else:
        print "Proxy looks good"
    print get_proxy_file()

    from pprint import pprint
    # pprint(read_samples("john_instructions.txt"))
    pprint(read_samples())

    # print get_dbs_url("https://cmsweb.cern.ch/crabserver/prod/workflow?workflow=160415_063140:namin_crab_tZq_ll_4f_13TeV-amcatnlo-pythia8_TuneCUETP8M1_RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asym")
    # print get_dbs_url("https://cmsweb.cern.ch/crabserver/prod/workflow?workflow=160415_063546:namin_crab_ST_tW_top_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1_RunIIFall15MiniAODv2-PU25nsData2015v")
    # print get_dbs_url("https://cmsweb.cern.ch/crabserver/prod/task?subresource=webdirprx&workflow=160415_063546:namin_crab_ST_tW_top_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1_RunIIFall15MiniAODv2-PU25nsData2015v")
    # print get_dbs_url("https://cmsweb.cern.ch/scheddmon/095/cms696/160415_063546:namin_crab_ST_tW_top_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1_RunIIFall15MiniAODv2-PU25nsData2015v")

    # print get_crabcache_info()
    # purge_crabcache()
    # # print bool(get_dbs_url("https://cmsweb.cern.ch/crabcache/info?subresource=fileremove&hashkey=09798d25dc94880700200d9c8e9608a85d8c8a953277e997528e6a87dd2e420d"))

