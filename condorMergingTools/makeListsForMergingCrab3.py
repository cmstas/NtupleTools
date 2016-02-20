#! /usr/bin/env python

import string, random
import commands, re, os
import sys 
import xml.dom.minidom
import subprocess
from xml.dom.minidom import Node

if "CMSSW_BASE" not in os.environ:
    print "$CMSSW_BASE not set! Please do 'cmsenv' first."
    sys.exit()
                      
################### Gets List of good XML files (files corresponding to jobs that have ###################
################### completed successfully                                             ###################

def getNumEventsRun(crabpath):
    global totalNumEventsRun
    # global goodCrabXMLFiles
    #get the job number from the crab file
    resultspath = crabpath+'/results/'
    # FrameworkJobReport-1.xml  cmsRun_1.log.tar.gz       cmsRun_2.log.tar.gz       cmsRun_3.log.tar.gz
    status, tars = commands.getstatusoutput('ls -1 ' + resultspath + '*.gz')
    tars = tars.split('\n')
    for tar in tars:
        print tar
        tarBasename = tar.replace(resultspath,"")
        # jobNum = tarBasename.split("_")[-1].split(".")[0]
        cmd = "tar xzf %s -C %s" % (tar, resultspath)
        os.system(cmd)


    status, xmls = commands.getstatusoutput('ls -1 ' + resultspath + '*.xml')
    xmls = xmls.split('\n')

    for i in xmls:
        print "New: " + i
        jobNum = i.split("-")[-1]
        jobNum = jobNum.split(".")[0]
        #parse the crab file:
        print "Getting Number of Events run for Job: " + i 
        try:
            doc = xml.dom.minidom.parse(i)
        except:
            continue
        for node in doc.getElementsByTagName("EventsRead"):
            s = node.firstChild.data
            #s is in unicode
            #don't need to encode in ascii, but runs faster
            s.encode('ascii')
            totalNumEventsRun = totalNumEventsRun + int(s)


###############################################################################################################
################### Get List Of Root Files which are not corrupted and have the Events tree ###################
                
def getGoodRootFiles(datapath,outpath):
    global goodCrabXMLFiles
    global goodRootFiles
    global crabpath
    tempXMLFileList = []
    fileNumber = 1
    totalFileSize = 0
    singleFileSize = 0
    f1 = open("%s/mergeFiles/listOfAllGoodFiles.txt" %(crabpath), "w")
    fileiter = open("%s/mergeFiles/mergeLists/merged_list_%s.txt" %(crabpath,str(fileNumber)), "w")

    status, inputfiles = commands.getstatusoutput('ls -l %s' % datapath)
    inputfiles = [file.split()[-1] for file in inputfiles.split("\n") if ".root" in file]
    print inputfiles
    #this is where we parse the crab fjrs to find the file size, path to the file, and if it is good or not.
    nEntries = 0;
    nCurrentEntries = 0;
    for ntuple in inputfiles:

        fullPath = '%s/%s' % (datapath, ntuple)
        fname = ntuple
        print fullPath, fname
                
        #checks the files with sweeproot before copying. If it finds a bad file, It skips the file and does not add the events to the total # of events
        print 'Checking File %s for integrity' % fullPath
        if os.path.isfile('sweepRoot') == False:
            print "sweepRoot executable does not exist! Exiting."
            print "sweepRoot source exists in sweepRoot directory."
            sys.exit()

        cmd = "./sweepRoot -o Events -t Events %s" % fullPath
        output = commands.getoutput(cmd).split('\n')

        for sweepRootCode in output:

            if sweepRootCode.find('SUMMARY') != -1:
                print sweepRootCode

            if sweepRootCode.find('nEntries') != -1:
                nCurrentEntries = int(sweepRootCode[10:]);

            if sweepRootCode.find('SUMMARY: 1 bad, 0 good') != -1:
                print 'File: %s does not seem to be good!!!!\n' % fullPath
                commands.getoutput('rm ' + outpath+'/temp/' + fname)

            elif sweepRootCode.find('SUMMARY: 0 bad, 1 good') != -1:
                print 'File: %s looks just fine!\n' % fullPath
                singleFileSize = os.path.getsize(fullPath)# commands.getoutput(cmd)
                # totalFileSize+=os.path.getsize(path)
                totalFileSize += int(singleFileSize)
                print"single file size: %s" %(singleFileSize)
                print "total file size: %s" %(totalFileSize)
                
                # checks to see if the files are > ~4.85GB, and if they are, creates a new list
                if totalFileSize < 5200000000:
                # #checks to see if the files are > ~7.2GB for the cms2->slim, and if they are, creates a new list
                # if totalFileSize < 7730900000:
                    f1.write( '%s\n' %(fullPath) )
                    fileiter.write( '%s\n' %(fullPath) )
                    nEntries += int(nCurrentEntries)
                else:
                    fileiter.close()
                    os.system("sed -i '1s/^/nEntries: %i\\n/' %s/mergeFiles/mergeLists/merged_list_%s.txt" %(nEntries,crabpath,str(fileNumber)))
                    nEntries = nCurrentEntries
                    fileNumber += 1
                    # totalFileSize = os.path.getsize(path)
                    totalFileSize = int(singleFileSize)
                    fileiter = open("%s/mergeFiles/mergeLists/merged_list_%s.txt" %(crabpath,str(fileNumber)), "w");
                    f1.write( '%s\n' %(fullPath) )
                    fileiter.write( '%s\n' %(fullPath) )

                tempXMLFileList.append(ntuple)

    fileiter.close()
    os.system("sed -i '1s/^/nEntries: %i\\n/' %s/mergeFiles/mergeLists/merged_list_%s.txt" %(nEntries,crabpath,str(fileNumber)))
    goodCrabXMLFiles = tempXMLFileList



if( len(sys.argv)<14 ):
    print 'Usage: postProcessing.py '
    print '                              -c [name of crab directory]'
    print '                              -d [directory where root files are]'
    print '                              -o [output directory for files on hadoop]'
    print '                              -s [name of sample]'
    print '                              -k kFactor'
    print '                              -e filter Efficiency'
    print '                              -x x-Section'
    print '                              --overrideCrab if you don\'t have the crab logs (usually true)'

    sys.exit()

##global variables here
crabpath = ''
datapath = ''
outpath = ''
overrideCrab = False

    
localdirectory = commands.getstatusoutput('pwd')[1]
for i in range (0, len(sys.argv)):
    if(sys.argv[i] == "-c"):
        crabpath   = sys.argv[i+1] + "/"
    if(sys.argv[i] == "-d"):
        datapath    = sys.argv[i+1] + "/"
    if(sys.argv[i] == "-o"):
        outpath     = sys.argv[i+1] + "/"
    if(sys.argv[i] == "-s"):
        samplename  = sys.argv[i+1]
    if(sys.argv[i] == "-k"):
        kFactor     = sys.argv[i+1] 
    if(sys.argv[i] == "-e"):
        filtEff     = sys.argv[i+1] 
    if(sys.argv[i] == "-x"):
        xSection     = sys.argv[i+1] 
    if(sys.argv[i] == "--overrideCrab"):
        overrideCrab = True
                         

if( overrideCrab == False and commands.getstatusoutput('ls ' + crabpath)[0] == 256):
    print 'The crab path does not exist. Please supply a valid path'
    sys.exit()

else:
    print 'samplename: %s/%s' %(os.getcwd(), samplename)
    subprocess.call('mkdir %s/%s 2>/dev/null' %(os.getcwd(), samplename), shell=True )
    crabpath = samplename + '/'

if( commands.getstatusoutput('ls ' + datapath)[0] == 256):
    print 'The directory containing the root files does not exist. Please supply a valid path'
    sys.exit()

if( commands.getstatusoutput('ls ' + outpath)[0] == 256):
    print '*******************************************'
    print 'The directory where you want your final root files to end up does not exist'
    sys.exit()

##get list of fjrs, then look through those for output files
##sum the space used by root files, and compare that to the free space on the output disk
goodCrabXMLFiles = []
# getGoodXMLFiles(crabpath)
# checkForSpace(outpath)


# if datapath.find("pnfs") != -1 or datapath.find("hadoop") != -1:
print "Creating folders for logs and merged file lists in %s/mergeFiles" %(outpath)
cmd = "mkdir -p " + crabpath + "/mergeFiles/mergeLists; rm " + crabpath + "/mergeFiles/mergeLists/merged_list_*.txt"
# print commands.getstatusoutput("ls %s/mergeFiles/mergeLists" %(crabpath))[1]
if commands.getstatusoutput(cmd)[0] == 256 and commands.getstatusoutput("ls %s/mergeFiles/mergeLists" %(crabpath))[1]!="":
    print "The directory %s/mergeFiles/mergeLists already exists and is not empty!. Exiting!" %(crabpath)
    sys.exit()

        

##now make the sweeproot macro if it doesn't exist
##sweepRoot should already exist when being called from AutoTuple
if os.path.isfile('sweepRoot') == False:
    cmd = 'cp ../sweepRoot/Makefile .'
    commands.getstatusoutput(cmd)
    cmd = 'cp ../sweepRoot/sweepRoot.C .'
    commands.getstatusoutput(cmd)
    cmd = 'make '
    commands.getstatusoutput(cmd)

if os.path.isfile('sweepRoot') == False:
    print "sweepRoot executable does not exist! Exiting."
    print "sweepRoot source exists in sweepRoot directory."
    sys.exit()


goodRootFiles = []
rootFilesToMerge = []

totalNumEventsRun = 0
effectiveNumEventsRun = 0
if(overrideCrab):
    ### get number of events in a TChain of root files in datapath
    status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%s\")' | grep nevents" % datapath)
    totalNumEventsRun = int(macroData.split("=")[-1])
    status, macroData = commands.getstatusoutput("root -l -b -q -n 'libC/counts.C(\"%s\", true)' | grep nevents" % datapath)
    effectiveNumEventsRun = int(macroData.split("=")[-1])#accounts for negative weighted events in NLO samples
else:
    getNumEventsRun(crabpath)

getGoodRootFiles(datapath,outpath)        

print '+++++++++++++++++++++++++++++'

print "creating file metaData.txt for postprocessing..."
f = open("%smergeFiles/metaData.txt" %(crabpath),"w")
f.write("n: %s\neffN: %s\nk: %s\nf: %s\nx: %s\n" %(str(totalNumEventsRun),str(effectiveNumEventsRun),kFactor,filtEff,xSection))
cmd = "ls %s/mergeFiles/mergeLists/ | awk '{print $1}'" %(crabpath) 
output = commands.getoutput(cmd).split('\n')
for listName in output:
    f.write("file: %s\n" %(listName))
    
f.close()

print "Total number of events run over: ",totalNumEventsRun

#check if number of files in mergelists equals number of input files
#if not, delete all the mergelists and exit with error
print "Checking file count..."
totalFiles = 0
status, mergelists = commands.getstatusoutput('ls -l %s/mergeFiles/mergeLists/' % crabpath)
mergelists = [file.split()[-1] for file in mergelists.split("\n") if "merged_list" in file]
for list in mergelists:
    status, linecount = commands.getstatusoutput('wc -l %s/mergeFiles/mergeLists/%s | awk \'{print $1}\' ' % (crabpath,list))
    nFiles = int(linecount)
    #subtract 1 if first line is nEntries
    status, containsEntries = commands.getstatusoutput('grep nEntries %s/mergeFiles/mergeLists/%s' % (crabpath,list))
    if containsEntries.find('nEntries') != -1:
        nFiles = int(linecount) - 1
    totalFiles += nFiles
status, inputfiles = commands.getstatusoutput('ls -l %s' % datapath)
inputfiles = [file.split()[-1] for file in inputfiles.split("\n") if ".root" in file]
if totalFiles == len(inputfiles):
    print "Number of input files matches mergelists."
if totalFiles != len(inputfiles):
    print "Number of input files does not match mergelists! Deleting mergelists and exiting..."
    os.system("rm  %s/mergeFiles/mergeLists/merged_list*" % crabpath)
    os.system("rm  %s/mergeFiles/metaData.txt" % crabpath)
    sys.exit(1)


print "Writing cfg file to cfg/%s.sh" %(samplename)
f_cfg = open("cfg/%s_cfg.sh" %(samplename),"w")
f_cfg.write("export inputListDirectory=%s/mergeFiles/mergeLists/\n" %(crabpath))
f_cfg.write("export mData=%s/mergeFiles/metaData.txt\n" %(crabpath))
f_cfg.write("export outputDir=%s\n" %(outpath))
f_cfg.write("export dataSet=%s\n" %(samplename))
f_cfg.write("export workingDirectory=%s\n" %(localdirectory))
if (os.environ["SCRAM_ARCH"].startswith('slc6_amd64_gcc49')): 
    f_cfg.write("export executableScript=%s/libsh/mergeScriptRoot6.sh\n" %(localdirectory))
else:    
    f_cfg.write("export executableScript=%s/libsh/mergeScript.sh\n" %(localdirectory))
f_cfg.close()

print "All file lists are created and stored in %smergeFiles/mergeLists/\n preparing to create jobs to postprocess ntuples..." %(crabpath)
