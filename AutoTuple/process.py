from sys import argv
import linecache
import sys
import os
import time
import datetime
import getpass
import fileinput

user = getpass.getuser()
if (user == "dsklein"): user = dklein;
if (user == "iandyckes"): user = gdyckes;
if (user == "mderdzinski"): user = mderdzin;
user2 = getpass.getuser()
args = sys.argv
file = args[1]
lineno = int(args[2])

#Get arguments
lines = [ line.strip() for line in open(file)]
gtag = lines[0]
tag = lines[1]
parts = lines[lineno].split(' ')[0:4]
parts.append(tag)
parts.append(gtag)
parts += lines[lineno].split(' ')[4:]

#Figure out output directory
os.system('grep -m 1 -r "Looking up detailed status of task" %s | awk \'{print $10}\' | cut -c 1-13 > %s' % ('crab_'+parts[0].split('/')[1]+'_'+parts[0].split('/')[2]+'/crab.log','crab_'+parts[0].split('/')[1]+'_'+parts[0].split('/')[2]+'/jobDateTime.txt'))
timeFile = open('crab_'+parts[0].split('/')[1]+'_'+parts[0].split('/')[2]+'/jobDateTime.txt', "r")
dateTime=timeFile.readline().rstrip("\n")

completelyDone = False
dataSet = parts[0].split('/')[1]
nLoops = 0
nEventsIn = 0
temp = "temp" + parts[0].split('/')[1] + ".txt"

os.system('rm crab_status_logs/pp.txt 2>/dev/null')

while (completelyDone == False):
  #Submit all the jobs
  date=str(datetime.datetime.now().strftime('%y-%m-%d_%H:%M:%S'))
  crab_dir = 'crab_' + parts[0].split('/')[1]+'_'+parts[0].split('/')[2]
  os.system('python makeListsForMergingCrab3.py -c ' + crab_dir + ' -d /hadoop/cms/store/user/' + user + '/' + parts[0].split('/')[1] + '/' + crab_dir + '/' + dateTime + '/0000/ -o /hadoop/cms/store/user/' + user + '/' + parts[0].split('/')[1] + '/' + crab_dir + '/' + parts[4] + '/merged/ -s ' + parts[0].split('/')[1] + ' -k ' + parts[2] + ' -e 1 -x ' + parts[1] + ' --overrideCrab > ' + temp)
  file = open(temp, "r")
  if nLoops == 0:
    for line in file:
      if "Total number of events run over" in line: nEventsIn = int(line.split(":")[1])
  os.system('./submitMergeJobs.sh cfg/' + parts[0].split('/')[1] + '_cfg.sh ' + date + ' > ' + temp)  

  #See if any jobs were submitted (will be false when resubmission not needed):
  file = open(temp, "r")
  for line in file:
    if "Check status of jobs with condor_q" in line: 
      nLeft = int(line.split(" jobs submitted")[0])
  file.close()

  #If no jobs were submitted, we are done, update monitor
  if (nLeft == 0): 
    completelyDone = True
    os.system('echo "here"')
    os.system('echo "%s done" >> crab_status_logs/pp.txt' % dataSet+'_'+parts[0].split('/')[2])
    os.system('copy.sh %s %s' % (parts[0], tag))
    continue
 
  #Get ID numbers of jobs submitted
  ids = []
  done = []
  log_list = 'jobs_' + date + '.txt'
  os.system('ls -lthr /data/tmp/' + user2 + '/' + dataSet + '/' + date + '/std_logs/ > ' + log_list)
  file = open(log_list, "r")
  for line in file:
    if '.out' in line: 
      ids.append( ((line.split()[8]).split('.out')[0]).split('1e.')[1])
      done.append( False )
  
  #See if jobs are finished yet.  
  isDone = False
  while (isDone == False):
    if not False in done: continue
    for i in range(0, len(ids)):
      if done[i] == True: continue
      os.system('condor_q ' + ids[i] + ' > ' + temp)
      file = open(temp, "r")
      for line in file:
        stillRunning = False
        if ids[i] in line: stillRunning = True
      if stillRunning == False: done[i] = True
    if not False in done: isDone = True
    nFinished = done.count(True)
    #Update logs
    os.system('echo "%s %i %i %i" >> crab_status_logs/pp.txt' % (dataSet+'_'+parts[0].split('/')[2],nEventsIn,nFinished,len(done)))
    nLoops += 1
    time.sleep(180)
