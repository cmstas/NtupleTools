import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
from pprint import pprint

def makeHist(vals, filename, title=None, nbins=50, lower=0, upper=10, logscale=False):
    if not title: title = ".".join(filename.split("/")[-1].split(".")[:-1])
    fig, ax = plt.subplots( nrows=1, ncols=1 )  # create figure & 1 axis
    if logscale: ax.set_yscale("log")
    fig.suptitle(title, fontsize=20)
    ax.hist(vals,nbins,color='green',alpha=0.8,range=[lower,upper], histtype="stepfilled")
    fig.savefig("%s" % (filename), bbox_inches='tight')
    print "Saved hist %s" % filename

# user,sample,t_before_merge,t_before_addbranch,t_after_addbranch,t_after_lcgcp,nevents
a = np.genfromtxt('timing_data.txt', delimiter=",", dtype=None, names=True, invalid_raise=False)
merge = 1.0*a["t_before_addbranch"]-a["t_before_merge"]
branch = 1.0*a["t_after_addbranch"]-a["t_before_addbranch"]
copy = 1.0*a["t_after_lcgcp"]-a["t_after_addbranch"]
total = 1.0*a["t_after_lcgcp"]-a["t_before_merge"]

nevents = a["nevents"]
print "Data for %i jobs" % len(nevents)

def longer_than(nhours):
    return "%.1f%% of jobs take longer than %i hours" % (100.0*np.sum(total > nhours*3600)/len(total), nhours)

for i in range(2,10,1): print longer_than(i)

long_jobs_idx = total > 2.0*3600

print "Long jobs (t > 2hrs) have means of:"
print "   merge: %.1f hrs" % (np.mean(merge[long_jobs_idx])/3600.0)
print "   branch %.1f hrs" % (np.mean(branch[long_jobs_idx])/3600.0)
print "   copy %.1f hrs" % (np.mean(copy[long_jobs_idx])/3600.0)
print "   total %.1f hrs" % (np.mean(total[long_jobs_idx])/3600.0)

if not os.path.isdir("timing_plots/"): os.makedir("timing_plots/")

makeHist(nevents       , "timing_plots/nevents.png" , title="nevents per merged file"                  , nbins=50 , lower=0 , upper=500000 , logscale=False)
makeHist(total/3600.0  , "timing_plots/total.png"   , title="time (hrs) to post-process a merged file" , nbins=50 , lower=0 , upper=10  , logscale=False)
makeHist(merge/3600.0  , "timing_plots/merge.png"   , title="time (hrs) to merge"                      , nbins=50 , lower=0 , upper=10  , logscale=False)
makeHist(branch/3600.0 , "timing_plots/branch.png"  , title="time (hrs) to add branches"               , nbins=50 , lower=0 , upper=10  , logscale=False)
makeHist(copy/3600.0   , "timing_plots/copy.png"    , title="time (hrs) to copy"                       , nbins=50 , lower=0 , upper=10  , logscale=False)

total = total[long_jobs_idx]
merge = merge[long_jobs_idx]
branch = branch[long_jobs_idx]
copy = copy[long_jobs_idx]
nevents = nevents[long_jobs_idx]

makeHist(nevents       , "timing_plots/long_nevents.png" , title="long jobs: nevents per merged file"                  , nbins=30 , lower=0 , upper=500000 , logscale=False)
makeHist(total/3600.0  , "timing_plots/long_total.png"   , title="long jobs: time (hrs) to post-process a merged file" , nbins=30 , lower=0 , upper=10  , logscale=False)
makeHist(merge/3600.0  , "timing_plots/long_merge.png"   , title="long jobs: time (hrs) to merge"                      , nbins=30 , lower=0 , upper=10  , logscale=False)
makeHist(branch/3600.0 , "timing_plots/long_branch.png"  , title="long jobs: time (hrs) to add branches"               , nbins=30 , lower=0 , upper=10  , logscale=False)
makeHist(copy/3600.0   , "timing_plots/long_copy.png"    , title="long jobs: time (hrs) to copy"                       , nbins=30 , lower=0 , upper=10  , logscale=False)
