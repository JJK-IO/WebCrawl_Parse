#!/bin/bash
#Does not split the output into multiple files
#Will need to change the source file and the match fields when finished
#Flag NR for each match.
#Create a file for matches in range
#i=0
#for f in awktest.csv; do
#   let i++;
#    gawk '/2/{flag=1;next}/2/{flag=0}flag' "$f" > output"${i}".csv
#done

#The method needs to output domains to a file starting from a string match ("clickbait") up to the next string match (but not including the next match). The method should then reset at the location of the next clickbait match.
echo First, here is a test .csv file we are working with...
cat awktest.csv
echo
echo The following line outputs all rows between the rows with a matching string of "2"
gawk -F, '/2/{flag=1;next}/2/{flag=0}flag' awktest.csv



clickbait.riskanalytics.com
brokerdomain.adperson.bro.brobor.com
advertizman.adplace.com
clickmeimanaddude.com
somethingsomethingsomethingdarkside.ad.com
clickbait.riskanalytics.com
brokerdomain.adperson.bro.brobor.com
advertizman.adplace.com
placeofthings.ad.com
clickbait.riskanalytics.com
anotherbroker.somewhere.com
ad.google.com
advertizman.adplace.com
clickbait.riskanalytics.com

[ 'brokerdomain.adperson.bro.brobor.com', 'advertizman.adplace.com', 2 ]
[ 'brokerdomain.adperson.bro.brobor.com', 'clickmeimanaddude.com', 1 ]
[ 'brokerdomain.adperson.bro.brobor.com', 'somethingsomethingsomethingdarkside.ad.com', 1 ]
[ 'brokerdomain.adperson.bro.brobor.com', 'placeofthings.ad.com', 1 ]
[ 'anotherbroker.somewhere.com', 'ad.google.com', 1 ]
[ 'anotherbroker.somewhere.com', 'advertizman.adplace.com', 1 ]