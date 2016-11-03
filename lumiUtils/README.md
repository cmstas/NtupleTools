# Luminosity utililities

## Setup/background
**Only read this section if you want to host your own instance of this, otherwise, my file (the default in `lumi_utils.py`) is always current and usable.**
It's a huge pain to log into lxplus to run brilcalc since the developers seem to not want to make it easily available outside the CERN firewall. So, I have a cron job on lxplus that runs brilcalc every 24 hrs and hosts the output at http://namin.web.cern.ch/namin/lumis_skim.tar.gz. I also have a cronjob on the uaf to curl this into the file /home/users/namin/dataTuple/2016D/NtupleTools/dataTuple/lumis/lumis_skim.csv. The columns are run,lumi,timestamp,lumidelivered(pb),lumirecorded(pb). The scripts here take this file and parse it.


The file is generated on lxplus using a script (`do.sh`) with contents
```bash
/afs/cern.ch/user/n/namin/.local/bin/brilcalc lumi --begin 272000 --normtag=/afs/cern.ch/user/l/lumipro/public/normtag_file/normtag_DATACERT.json --byls -u /pb --output-style csv > lumis.csv
/usr/bin/python /afs/cern.ch/user/n/namin/lumi/skim.py
tar cvzf lumis_skim.tar.gz lumis_skim.csv
cp lumis_skim.* /afs/cern.ch/user/n/namin/www/
```
and it is run using an AFS crontab (`acrontab -e`)
`46 11 * * * lxplus.cern.ch /bin/bash /afs/cern.ch/user/n/namin/lumi/do.sh > /afs/cern.ch/user/n/namin/www/acron.log 2>&1` (normal crontab does not work on lxplus due to expiring credentials).

The file is then obtained on the UAF via a script (`fetch_lumis.sh`) with contents
```bash
curl -O http://namin.web.cern.ch/namin/lumis_skim.tar.gz --connect-timeout 60 -s
tar xzf lumis_skim.tar.gz
```
and a crontab entry like `cd /home/users/namin/dataTuple/2016D/NtupleTools/dataTuple/lumis && /bin/sh fetch_lumis.sh`

## Usage

### `dis` (Das Is Slow)
* The `get_lumi_for_eras.py` script uses an API for DIS that is explained and obtained via the `examples` link on http://uaf-8.t2.ucsd.edu/~namin/makers/disMaker/index.html
* In a nutshell, this lets you make DAS/DBS queries with a nice interface, as well as querying the SNT Twiki information
* **Get the command line script and put it in your python path (or keep it in this folder), as this is a generally useful script**
 * `curl -O https://raw.githubusercontent.com/cmstas/NtupleTools/master/AutoTwopler/scripts/dis_client.py`

### `lumi_utils.py`
* This script allows you to extract the integrated luminosity (or just the lumi json) from an official lumi JSON, SNT JSON, CMS3 ntuple, or baby file. In the latter case, I only have implementations for SS and LeptonTree babies, but it's not hard to put in support for arbitrary babies.
* Essentially, this script will analyze run,lumi pairs from the tree or JSON and compare them with the CSV file explained previously and calculate the integrated luminosity
* To show the JSON contained within file(s), and the corresponding integrated luminosity, you can do
` python lumi_utils.py "/nfs-7/userdata/ss2015/ssBabies/v8.04/DataDouble*.root" `
 * To only get the JSON, you can use the `--json` option
 * To only get the SNT JSON, you can use the `--snt` option
 * To only get the integrated luminosity value, you can use the `--lumi` option
 * Note that using a wildcard will sum the lumis in the input files
* To show the integrated luminosity in an SNT good run list, you can do
`python lumi_utils.py /home/users/namin/2016/ss/master/SSAnalysis/goodRunList/goldenJson_2016_27p2.txt --lumi` (of course, this works with official JSON lists too)
* Finally, you can also import lumi_utils.py and do some lumi json arithmetic to add and subtract run/lumis easily. There's some examples in the script already.

### `get_lumi_for_eras.py`
* This script will print out a Twiki table for first/last run and integrated luminosity for each specified era contained within a specified JSON

