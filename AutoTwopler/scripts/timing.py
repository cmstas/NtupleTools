import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

def makeHist(vals, filename, title=None, nbins=50, lower=0, upper=10, logscale=False):
    if not title: title = ".".join(filename.split("/")[-1].split(".")[:-1])
    fig, ax = plt.subplots( nrows=1, ncols=1 )  # create figure & 1 axis
    if logscale: ax.set_yscale("log")
    fig.suptitle(title, fontsize=20)
    ax.hist(vals,nbins,color='green',alpha=0.8,range=[lower,upper])
    fig.savefig("%s" % (filename), bbox_inches='tight')
    print "Saved hist %s" % filename

np.set_printoptions(linewidth=205,formatter={'float_kind':lambda x: "%.2f" % x })

# user,sample,t_before_merge,t_before_addbranch,t_after_addbranch,t_after_lcgcp,nevents
a = np.genfromtxt('timing_data.txt', delimiter=",", dtype=None, names=True, invalid_raise=False)
merge = 1.0*a["t_before_addbranch"]-a["t_before_merge"]
branch = 1.0*a["t_after_addbranch"]-a["t_before_addbranch"]
copy = 1.0*a["t_after_lcgcp"]-a["t_after_addbranch"]
total = 1.0*a["t_after_lcgcp"]-a["t_before_merge"]
nevents = a["nevents"]

def longer_than(nhours):
    return "%.1f%% of jobs take longer than %i hours" % (100.0*np.sum(total > nhours*3600)/len(total), nhours)

for i in range(2,20,2): print longer_than(i)

makeHist(nevents               , "nevents.png"  , title="nevents per merged file"                , nbins=50 , lower=0 , upper=250000)
makeHist(total                 , "total.png"    , title="time (s) to post-process a merged file" , nbins=50 , lower=0 , upper=50000)
makeHist(1000.0*total/nevents  , "total_1k.png" , title="time (s) to post-process 1k events"     , nbins=50 , lower=0 , upper=100)
makeHist(1000.0*merge/nevents  , "merge.png"    , title="time (s) to merge 1k events"            , nbins=50 , lower=0 , upper=20)
makeHist(1000.0*branch/nevents , "branch.png"   , title="time (s) to add branches to 1k events"  , nbins=50 , lower=0 , upper=20)
makeHist(1000.0*copy/nevents   , "copy.png"     , title="time (s) to copy 1k events"             , nbins=50 , lower=0 , upper=20)
