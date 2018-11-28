import json
import dis_client as dis
import os

def persist_to_file(file_name):
    def decorator(original_func,**kwargs):
        try:
            cache = json.load(open(file_name, 'r'))
        except (IOError, ValueError):
            cache = {}
        def new_func(param,**kwargs):
            if param not in cache:
                cache[param] = original_func(param,**kwargs)
                json.dump(cache, open(file_name, 'w'))
            return cache[param]
        return new_func
    return decorator

def fetch_nocache(q,**kwargs):
    print "making request for {}".format(q)
    return dis.query(q,**kwargs)["response"]["payload"]
fetch = persist_to_file("cache.dat")(fetch_nocache)

def get_dict(samples):
    tdict = {}
    for s in samples:
        dsname = str(s.get("dataset_name",s.get("dataset")))
        pd = dsname.split("/",2)[1]
        era = dsname.split("/",3)[2].replace("PromptReco-","").replace("17Sep2018-","")
        k = (pd,era)
        tdict[k] = {}
        tdict[k]["nevents"] = s.get("nevents_out",s.get("nevents"))
    return tdict

def get_table(tdict, extra_pds=[]):
    all_pds = sorted(set([x[0] for x in tdict.keys()]+extra_pds))
    all_eras = sorted(set(x[1] for x in tdict.keys()))
    all_eras = [era for era in all_eras if era != "Run2018D-v1"]
    buff = ""
    buff += "<table>\n"
    buff += "  <tr>\n"
    for era in [""]+all_eras:
        buff += "    <th>{}</th>\n".format(era)
    buff += "  </tr>\n"
    for pd in all_pds:
        buff += " <tr>\n"
        buff += "    <th>{}</th>\n".format(pd)
        for era in all_eras:
            pair = (pd,era)
            cell = ""
            cls = "bad"
            if pair in tdict:
                d = tdict[pair]
                cls = "good"
                cell = d["nevents"]
                if not d.get("valid",True):
                    cls = "meh"
            buff += "    <td class=\"{}\">{}</td>\n".format(cls,cell)
        buff += "  </tr>\n"
    buff += "</table>\n"
    return buff

if __name__ == "__main__":

    # Fetch all Run2018 SNT samples (no caching)
    samples = fetch_nocache("*Run2018*  | grep dataset_name,cms3tag,nevents_out", typ="snt")
    # Separate into prompt and rereco
    tab1 = get_table(get_dict([s for s in samples if "Prompt" in s["dataset_name"]]))
    tab2 = get_table(get_dict([s for s in samples if "17Sep2018" in s["dataset_name"]]),extra_pds=["MuonEG"])

    # Fetch rereco samples from DAS. First get all (complete+incomplete), then only complete
    pds = ["EGamma","DoubleMuon","SingleMuon","MuonEG","JetHT","MET"]
    sall = sum([fetch("/{}/*-17Sep2018-*/MINIAOD,all".format(pd),detail=True) for pd in pds],[])
    sdone = sum([fetch("/{}/*-17Sep2018-*/MINIAOD".format(pd),detail=True) for pd in pds],[])
    dall = get_dict(sall)
    ddone = get_dict(sdone)
    # Append flag for completeness/validity and make table
    for k in dall.keys():
        dall[k]["valid"] = (k in ddone)
    tab3 = get_table(dall)

    buff = "<html>\n"
    buff += """
    <head>
    <style>
    body {
    font-family: Helvetica, Arial, Sans-Serif;
    }
    table {
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
        text-align: center;
        font-size: 90%;
    }
    .good {
        background-color: #a1d2a7;
    }
    .bad {
        background-color: #ffb2b5;
    }
    .meh {
        background-color: #FFD981;
    }
    </style>
    </head>
    """
    buff += "<a href='http://uaf-8.t2.ucsd.edu/~namin/dis/?query=*Run2018*++%7C+grep+dataset_name%2Clocation%2Ccms3tag%2Cnevents_out&type=snt&short=short'>DIS link</a><br>"
    buff += "<br><strong>PromptReco SNT CMS4</strong><br>"
    buff += tab1
    buff += "<br><strong>Re-reco (17Sep2018) SNT CMS4</strong><br>"
    buff += tab2
    buff += "<br><strong>Re-reco (17Sep2018) DAS MiniAOD</strong> <br><span class='meh'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>=not marked as valid on DAS yet, <span class='bad'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>=not even on DAS :(<br>"
    buff += tab3
    buff += "</html>"
    # print buff
    fname = "table.html"
    with open(fname,"w") as fh:
        fh.write(buff)
    print "Wrote {}".format(fname)

    os.system("cp {} ~/public_html/dump/data2018status.html".format(fname))
