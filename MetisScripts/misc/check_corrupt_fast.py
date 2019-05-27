# pip install --user futures
import concurrent.futures
# pip install --user uproot
import uproot
import os
import sys
import glob
# pip install --user tqdm
from tqdm import tqdm

"""
Fast check to see if files are corrupt. 
Since this checks to see if uproot can open the file properly, this only really catches
corruptions due to stageout and not subtle block corruptions in the middle of files.
Take note of some dependencies at the top.
"""

def is_valid(fname):
    try:
        # return fname,len(uproot.open(fname)["Events"])
        x = uproot.open(fname)
        return fname,True
    except:
        return fname,False

if __name__ == "__main__":

    # executor = concurrent.futures.ThreadPoolExecutor(5)
    executor = concurrent.futures.ProcessPoolExecutor(10)

    # fnames = [
    #         "/hadoop/cms/store/group/snt/run2_mc2016_94x//SMS-T8bbstausnu_mStop-200to1800_XCha0p5_XStau0p75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1_MINIAODSIM_CMS4_V10-02-05//merged_ntuple_109.root",
    #         "/hadoop/cms/store/group/snt/run2_mc2016_94x//SMS-T8bbstausnu_mStop-200to1800_XCha0p5_XStau0p75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1_MINIAODSIM_CMS4_V10-02-05//merged_ntuple_86.root",
    #         "/hadoop/cms/store/group/snt/run2_mc2016_94x//SMS-T8bbstausnu_mStop-200to1800_XCha0p5_XStau0p75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1_MINIAODSIM_CMS4_V10-02-05//merged_ntuple_75.root",
    #         "/hadoop/cms/store/group/snt/run2_mc2016_94x//SMS-T8bbstausnu_mStop-200to1800_XCha0p5_XStau0p75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1_MINIAODSIM_CMS4_V10-02-05//merged_ntuple_77.root",
    #         ]
    fnames = glob.glob(
            "/hadoop/cms/store/group/snt/run2_mc2016_94x//SMS-T8bbstausnu_mStop-200to1800_XCha0p5_XStau0p75_TuneCUETP8M1_13TeV-madgraphMLM-pythia8_RunIISummer16MiniAODv3-PUSummer16v3Fast_94X_mcRun2_asymptotic_v3-v1_MINIAODSIM_CMS4_V10-02-05//*.root",
            )
    # print is_valid(fname)
    futures = tqdm(executor.map(is_valid,fnames),total=len(fnames))
    badfiles = []
    for result in futures:
        fname,valid = result
        if not valid:
            badfiles.append(fname)
    print "------ bad files ------"
    print "\n".join(badfiles)
    print "-----------------------"
