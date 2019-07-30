import os
import sys
import commands
import glob
import pandas as pd
from tqdm import tqdm

def split_into_chunks(l,size=512):
    for i in range(0,len(l),size): yield l[i:i+size]

def get_df(dirs):
    """
    Given list a hadoop directories, return physical directory sizes in GB
    """
    sizes = {}
    for chunk in split_into_chunks(dirs):
        cmd = "hadoop fs -du -s {0}".format(" ".join(chunk)).replace("/hadoop","")
        stat,out = commands.getstatusoutput(cmd)
        for line in out.strip().splitlines():
            try:
                sizes["/hadoop"+line.split()[2]] = int(line.split()[0])*1.0e-9
            except:
                pass
    data = []
    # for path,size in tqdm(sizes.items()):
    for path,size in sizes.items():
        d = {}
        d["size"] = size
        d["path"] = path
        d["campaign"] = d["path"].split("/group/snt/",1)[1].split("/",1)[0]
        d["tag"] = d["path"].rsplit("_CMS4_",1)[-1]
        d["sample"] = d["path"].split("/group/snt/",1)[1].split("/",1)[1].rsplit("_CMS4_",1)[0]
        d["process"] = d["sample"].split("_RunII",1)[0]
        # d["metadata"] = os.path.exists(d["path"]+"/metadata.json")
        data.append(d)
    df = pd.DataFrame(data)
    df["size"] = df["size"].round(1)
    return df

if __name__ == "__main__":

    df = pd.concat([
        get_df(glob.glob("/hadoop/cms/store/group/snt/run2_mc2016_94x/*")),
        get_df(glob.glob("/hadoop/cms/store/group/snt/run2_mc2017/*")),
        get_df(glob.glob("/hadoop/cms/store/group/snt/run2_mc2018/*")),
        get_df(glob.glob("/hadoop/cms/store/group/snt/run2_data2016_94x/*")),
        get_df(glob.glob("/hadoop/cms/store/group/snt/run2_data2017/*")),
        get_df(glob.glob("/hadoop/cms/store/group/snt/run2_data2018/*")),
        get_df(glob.glob("/hadoop/cms/store/group/snt/run2_data2018/*")),
        ]).reset_index(drop=True).drop_duplicates(["path"])
    df.to_pickle("temp.pkl")

    df = pd.read_pickle("temp.pkl")
    groups = df.groupby(["sample"])
    can_save = 0.
    for name,group in groups:
        if len(group) == 1: continue
        group = group.sort_values("tag",ascending=False)
        tags = list(group["tag"].values)
        sizes = list(group["size"].values)
        paths = list(group["path"].values)
        for tag,size,path in zip(tags[1:],sizes[1:],paths[1:]):
            print "{} # ({}GB), latest tag is {} and {}GB ({})".format(path,size,tags[0],sizes[0],paths[0])
            can_save += size
    print "# in total, can save {}GB by deleting old versions".format(can_save)

