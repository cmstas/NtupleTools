import sys
sys.path.insert(0,"../")
import utils as u

if __name__=='__main__':
    pass

    """ Get proxy information and renew if necessary """

    # hours_left = u.proxy_hours_left()
    # print "%i hours left on your proxy" % hours_left
    # if hours_left < 5: u.proxy_renew()


    """ Get event count information from dataset name """

    # dataset = '/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM'
    # info = u.dataset_event_count(dataset)
    # print "Sample %s: %s" % (dataset, str(info))

    """ Get list of files in a dataset """

    # dataset = '/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM'
    # files = u.get_dataset_files(dataset)
    # for rootfile, nevents, sizeGB in files[:10]:
    #     print rootfile, nevents, sizeGB

    """ Get the GEN_SIM dataset name associated with a dataset """

    # dataset = '/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM'
    # gen_sim = u.get_gen_sim(dataset)
    # print "GEN_SIM for %s is %s" % (dataset, gen_sim)


    """ Get some basic MCM information from a given GEN_SIM dataset name """

    # gen_sim = "/TT_TuneCUETP8M1_13TeV-powheg-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM"
    # info = u.get_slim_mcm_json(gen_sim)
    # print "GEN_SIM sample %s: %s" % (gen_sim, str(info))


    """ Get all MCM information from a given dataset name """

    # dataset = "/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM"
    # info = u.get_mcm_json(dataset)
    # print info

    """ Get LHE files for dataset """

    # dataset = '/DYJetsToLL_M-50_Zpt-150toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM'
    # gen_sim = u.get_gen_sim(dataset)
    # lhe = u.get_dataset_parent(gen_sim) # LHE is usually parent to GEN_SIM. if not, can use get_slim_mcm_json() to get mcdb_id
    # files = u.get_dataset_files(lhe)
    # for rootfile, nevents, sizeGB in files[:10]:
    #     print rootfile, nevents, sizeGB


    """ Purge crab cache with magic """

    # d = u.get_crabcache_info()
    # print "you are using %.1f GB of memory on the crab cache server with %i files" % (d["used_space_GB"], len(d["file_hashes"]))
    # print "purging now"
    # u.purge_crabcache()
    # d = u.get_crabcache_info()
    # print "now you are using %.1f GB of memory on the crab cache server with %i files" % (d["used_space_GB"], len(d["file_hashes"]))



