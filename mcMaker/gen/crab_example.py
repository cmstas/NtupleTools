from CRABClient.UserUtilities import config
config = config()

name="newphysics_m9001"  # EDIT ME - a tag

config.section_('General')
config.General.requestName = name
config.General.workArea = 'crab'
config.General.transferOutputs = True
config.General.transferLogs = True

config.section_('JobType')
config.JobType.pluginName = 'PrivateMC'
config.JobType.maxMemoryMB = 3000
config.JobType.psetName = "pset_gensim_example.py"  # EDIT ME - path to pset

config.section_('Data')
config.Data.splitting = 'EventBased'
# structure splitting such that you get output in ~5 hours. gensim takes ~30s/event.
config.Data.unitsPerJob = 2500  # EDIT ME - events per job
config.Data.totalUnits = config.Data.unitsPerJob * 750  # EDIT ME - njobs
config.Data.outputPrimaryDataset = name
config.Data.publication = False
config.Data.publishDBS = 'phys03'
config.Data.outputDatasetTag = name
config.Data.ignoreLocality = True

config.section_('Site')
config.Site.whitelist = ["T2_US*"]
config.Site.storageSite = 'T2_US_UCSD'

