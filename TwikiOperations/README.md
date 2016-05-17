#Twiki Operations

### `cleaver.py`
re-run each day to get new samples for a given campaign (specified at top) that were not in DAS the day before. This cuts out samples that we don't care about (extra dimensions, graviton crap, etc.)

### `dis_client.py`
multipurpose DIS query client

### `exodus.py`
makes a new twiki from old with new campaign string (params specified at top)

### jhJ

###Twiki.py is the central script
  - /usr/bin/python twiki.py TWIKI_USERNAME --makeInstructions NAME_SAMPLES_ARE_ASSIGNED_TO will make your instructions.txt for the AutoTupler
  - /usr/bin/python twiki.py TWIKI_USERNAME --getUnmade will get all samples that are not finished in an instructions.txt file 
  - /usr/bin/python twiki.py TWIKI_USERNAME --allSamples will get all samples in an instructions.txt file 
  - /usr/bin/python twiki.py TWIKI_USERNAME --manual 1 will download the entire twiki, --manual 2 will upload it (after you've made changes).  CAREFUL!
  - /usr/bni/pthon twiki.py TWIKI_USERNAME --dataset X --ARG Y will manually update the twiki to fix ARG to be Y.  Supported ARGs are:
     -- nIn
     -- nOut
     -- CMS3tag
     -- location
     -- gtag
     -- filtEff
     -- kfactor
     -- xsec
  - Additional arguments are:
     -- --passwordFile to specify a file containing your password so you don't have to type it every time.  The permissions on this file should be strict!
     -- --whichTwiki to specify which twiki you want (1=phys14, 2=run2-25, 3=run2-50), so you don't have to type it every time

###updateSamplesThatAreRedoneInLaterTag.sh is a wrapper script
  - When a sample is re-processed, this will allow you to do a bulk update of the twiki
  - Need to set the variables at the top of the script
  - Input file to this comes from Nick's script (to be added here)

###dumpSamples.py 
  - Dump all datasets (along with person responsible, CMS3tag) to a text file

###checkSamples.py
  - Takes output of dumpSamples.py and checks validity (does the sample exist on hadoop? is there a newer tag? etc)
