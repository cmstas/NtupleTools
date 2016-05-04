#!/bin/bash

echo "Running monitor.sh"

if [ ! -d $BASEPATH ]
then
  echo "BASEPATH in monitor.sh does not exist!"
  exit 1
fi

OUT=$BASEPATH/dataTupleMonitor.html

sed 's./. .g' $BASEPATH/input.txt | awk '{print $1}' > listOfDatasets.txt #replace "/" by " " and then print out the first column

echo "Last updated: `date` <BR><BR>" > $OUT
echo "" >> $OUT

echo "User running the cron job: $USER <BR><BR>" >> $OUT
echo "" >> $OUT

echo "List of datasets being processed: <BR>" >> $OUT
for i in `cat listOfDatasets.txt`
do
  echo "$i<BR>" >> $OUT
done
echo "<BR>" >> $OUT

NJOBSRUNNING=0
NJOBSIDLE=0
NJOBSHELD=0
i=0

#preliminary stuff
echo "<HTML>" >> $OUT
echo "<body>" >> $OUT

while read line
do
  i=$(( $i + 1 ))
  DATASET=`echo $line | tr '/' ' ' | awk '{print $1}'` 
  VERSION=`echo $line | tr '/' ' ' | awk '{print $2}' | tr '-' ' ' | awk '{print $3}'`
  ERA=`echo $line | tr '/' ' ' | awk '{print $2}' | tr '-' ' ' | awk '{print $1}'`
  NTOTAL=`cat masterList.txt | grep $DATASET | wc -l`
  NCOMPLETED=`cat $BASEPATH/completedList.txt | grep $DATASET | wc -l`
  name="alex"
  if [ "$USER" == "jgran" ]; then name="jason"; fi
  if [ "$USER" == "namin" ]; then name="nick"; fi
  if [ "$USER" == "mderdzinski" ]; then name="mark"; fi
  cp /nfs-7/userdata/dataTuple/$name/json_lists/full_JSON_${ERA}_${DATASET}_MINIAOD_PromptReco-$VERSION.txt /home/users/$USER/public_html/json_$i.txt
  if [ -e runningList.txt ]; then NJOBSRUNNING=`cat runningList.txt | grep $DATASET | wc -l`; fi
  if [ -e idleList.txt ]; then NJOBSIDLE=`cat idleList.txt | grep $DATASET | wc -l`; fi
  if [ -e heldList.txt ]; then NJOBSHELD=`cat heldList.txt | grep $DATASET | wc -l`; fi
  echo "Dataset: $DATASET <BR>" >> $OUT
  echo "Number of files in dataset: $NTOTAL <BR>" >> $OUT
  echo "Number of files processed: $NCOMPLETED <BR>" >> $OUT
  echo "Number of jobs running: $NJOBSRUNNING <BR>" >> $OUT
  echo "Number of jobs idle: $NJOBSIDLE <BR>" >> $OUT
  echo "Lumis completed: <A HREF=\"http://uaf-7.t2.ucsd.edu/~$USER/json_$i.txt\">JSON</A><BR>" >> $OUT
  echo "<BR>" >> $OUT
done < $BASEPATH/input.txt

#Terminal stuff
echo "</body>" >> $OUT
echo "</HTML>" >> $OUT

rm listOfDatasets.txt

cp $OUT /home/users/$USER/public_html/dataTupleMonitor.html
