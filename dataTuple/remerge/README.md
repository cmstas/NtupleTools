put the list of files to remerge into the all.py list near the top. the script will loop through them serially and remerge locally. the files will be copied to your hadoop directory, so when done,
check the done_stuff.txt file and execute those commands to move them into the central snt area


# MAKE SURE THE ORIGINAL AND NEW FILES EXIST BEFORE COPYING THEM OVER AND THAT THE FILES LOOK SIMILAR IN SIZE
for thing in $(grep -E "(new|old)=" done_stuff.txt | cut -d= -f2 | xargs -n 2); do ls -l $thing; done

