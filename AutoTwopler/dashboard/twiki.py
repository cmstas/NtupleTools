import mechanize
import os
import sys
from collections import OrderedDict


def get_browser(page, username):
    user = os.path.dirname(os.path.realpath(__file__)).split("/")[3].strip()
    password_file = "/home/users/%s/.twikipw" % user

    if not os.path.isfile(password_file):
        print "Error: %s file does not exist. create it and chmod 600 it" % password_file
        sys.exit()

    if username.strip() == "":
        username = user

    BASE_URL = "http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/"
    if "www.t2.ucsd.edu" in page:
        # if user didn't specify fragment, but whole thing, then go with the flow
        BASE_URL = ""

    with open(password_file, "r") as fhin: password = fhin.read().strip()

    br = mechanize.Browser()
    br.addheaders = [('User-agent', 'Firefox')]
    br.set_handle_robots( False )


    try:
        br.open(BASE_URL+page)
        br.select_form(nr=0)
        br.form['username'] = username
        br.form['password'] = password
        br.submit()
    except:
        print "Can't open %s" % (BASE_URL+page)
        sys.exit()
    return br

def get_raw(page, username):
    br = get_browser(page, username)
    for link in br.links():
        if link.text.strip() == 'Raw View':
            br.follow_link(link)
            break

    resp = br.response().read()
    raw = resp.split("twikiTextareaRawView\">")[1].split("</textarea")[0]
    return raw

def get_samples(assigned_to, username, get_unmade=True, page="Autotupletest"):
    raw = get_raw(page, username)

    samples = []
    # columns = ["dataset", "filter_type", "nevents_in", "nevents_out", "xsec", "kfact", "efact", "gtag", "cms3tag", "location", "assigned", "comments"] 
    # columns_data = ["dataset", "nevents_out", "status", "gtag", "cms3tag", "unmerged_location", "location", "assigned", "comments"] 

    is_data = "Run2_Data" in page

    if is_data:

        for iline,line in enumerate(raw.split("\n")):
            if line.count("|") not in [10,11] or "Dataset*" in line: continue
        
            line = line.strip()
            if line.count("|") == 10:
                dataset, nevents_out, status, gtag, cms3tag, unmerged_location, location, assigned, comments = map(lambda x: x.strip(), line.split("|")[1:-1])
            elif line.count("|") == 11:
                dataset, nevents_out, nevents_DAS, status, gtag, cms3tag, unmerged_location, location, assigned, comments = map(lambda x: x.strip(), line.split("|")[1:-1])

            nevents_in = nevents_out

            samples.append( {
                        "dataset": dataset, "gtag": gtag,  "nevents_in": nevents_in, "nevents_out": nevents_out, 
                        "location": location, "cms3tag": cms3tag, "assigned": assigned, "comments": comments, 
                        } )

    else:

        for iline,line in enumerate(raw.split("\n")):
            if line.count("|") is not 13 or "Dataset*" in line: continue

            line = line.strip()
            dataset, filter_type, nevents_in, nevents_out, xsec, kfact, efact, gtag, cms3tag, location, assigned, comments = map(lambda x: x.strip(), line.split("|")[1:-1])
            sparms = ""
            if "sparms:" in comments.lower():
                try: sparms = comments.split("sParms:")[1].strip()
                except: pass
                if not sparms:
                    try: sparms = comments.split("sparms:")[1].strip()
                    except: pass

            if not(assigned == assigned_to or assigned_to.lower() == "all"): continue

            if get_unmade and not(location == ""): continue

            samples.append( {
                        "dataset": dataset, "gtag": gtag, "xsec": xsec, "kfact": kfact, "efact": efact, "sparms": sparms, 
                        "filter_type": filter_type, "nevents_in": nevents_in, "nevents_out": nevents_out, "location": location, 
                        "cms3tag": cms3tag, "assigned": assigned, "comments": comments, 
                        } )

    return samples


def update_samples(samples, username, page="Autotupletest", update_done=False, dryrun=False):
    br = get_browser(page, username)
    for link in br.links():
        if link.text.strip() == 'Raw Edit':
            br.follow_link(link)
            break
    br.select_form('main')
    raw = br.get_value('text')

    columns = ["dataset", "filter_type", "nevents_in", "nevents_out", "xsec", "kfact", "efact", "gtag", "cms3tag", "location", "assigned", "comments"] 
    lines_out = []
    num_updatable = 0
    for iline,line in enumerate(raw.split("\n")):
        if line.count("|") is 13 and "*Dataset*" not in line:
            line = line.strip()
            parts = map(lambda x: x.strip(), line.split("|")[1:-1])
            sample_twiki  = OrderedDict( zip(columns, parts) )

            for sample in samples:
                if not update_done and ("status" in sample and not(sample["status"] == "done")): continue

                # find matching sample (dataset, cms3tag must match), then fill in events and location
                if sample_twiki["dataset"] == sample["dataset"] and sample_twiki["cms3tag"] == sample["cms3tag"] and (update_done or sample_twiki["location"] == ""):
                    print "Found updatable entry for %s: %s" % (sample["cms3tag"], sample["dataset"])
                    if dryrun:
                        for key in ["xsec","kfact","efact"]:
                            old = sample_twiki.get(key, None)
                            new = sample.get(key, None)
                            if old is None or new is None: continue
                            if old != new:
                                print "\t%s: %s --> %s" % (key, str(old), str(new))
                        print

                    num_updatable += 1

                    sample_twiki["location"] = sample.get("finaldir", sample_twiki["location"])
                    sample_twiki["nevents_in"] = sample.get("nevents_DAS", sample_twiki["nevents_in"])
                    sample_twiki["nevents_out"] = sample.get("nevents_merged", sample_twiki["nevents_out"])
                    sample_twiki["xsec"] = sample.get("xsec", sample_twiki["xsec"])
                    sample_twiki["kfact"] = sample.get("kfact", sample_twiki["kfact"])
                    sample_twiki["efact"] = sample.get("efact", sample_twiki["efact"])
                    line = "| %s |" % " | ".join(map(str,sample_twiki.values()))
                    break

        lines_out.append(line)

    tosubmit = "\n".join(lines_out)
    # SANITY CHECK: if replacement text is less than 95% of original text, maybe we screwed up
    # if we didn't take out whitespace, adding entries would increase the size and then we could use 100%
    url = br.geturl().split("?")[0]
    if num_updatable == 0:
        print "Didn't find any updatable entries in %s" % url

    if len(tosubmit) > 0.95*len(raw) and num_updatable > 0 and not dryrun:
        br.form['text'] = "\n".join(lines_out)
        print "Updated Twiki at %s" % url.replace("/edit/","/view/")
        br.submit()

if __name__=='__main__':

    samples = [{"dataset": "/DYToEE_13TeV-amcatnloFXFX-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM",
              "cms3tag": "CMS3_V07-04-08",
              "gtag": "MCRUN2_74_V9",
              "finaldir": "final/dir/test/",
              "nevents_DAS": 312261,
              "nevents_merged": 312261,
              "status": "done",
            }]
    update_samples(samples, username="namin", page="Autotupletest")
    # print get_samples(assigned_to="Nick", username="namin", get_unmade=False, page="")
