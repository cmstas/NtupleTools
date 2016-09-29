
put the list of files to remerge into the submit.py list near the top. 
the sript with loop through the files and submit merge jobs to condor. re-running the script multiple times will check when files finish. when they fnish, the script will check nevents and filesize. if found to be consistent, the script will copy over the files to the central hadoop area. a pickle file with a list of completed files is kept so that the state is preserved when running the script multiple times. this does mean that you need to delete the file if you ever desire to remake the same file again.
