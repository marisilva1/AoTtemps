# AoTtemps

This repo will focus on filtering and sorting the Chicago public tarball data.
So far I have not used wget to obtain the dataset, and currently have the portion I need already downloaded.

Steps to manipulate data:
1. Slice date range
2. Remove outliers (outlierScript)
3. Split into separate folders based on node (nodeSplit)  
**(Check to see if all nodes span this time frame?)**
4. Add lats/lons to each of the data from the node metadata file?
5. Use the dataReduction.py tool to average the separate nodes over 15-minute intervals
6. Do analysis - reorganize the data into separate columns for temperature, pressure, and humidity values if necessary, or just for cleanliness (or do this step sooner?)

```bash
## skeleton bash script, assuming I learn to use wget
#! usr/bin/env bash

# get public dataset
wget http://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/AoT_Chicago.public.latest.tar

# get the name of the directory the tarball will extract to
extraction=`tar -tf AoT_Chicago.public.latest.tar`
tar xf AoT_Chicago.public.latest.tar

# run the slicing tool
python slice-date-range.py $extraction $startDate $endDate

# cd into sliced data dir, get name of dir for next part
cd $extraction.from-$startDate-to-$endDate
newDir= #SOMETHING don't know what to put here yet

# extract the data csv
gunzip data.csv.gz

# run outlierScript, nodeSplit, and dataReduction:
python outlierScript.py -i $newDir
newerDir = something
python nodeSplit -i $newerDir
newestDir = something
# need to repeat dataReduction for every node .csv file... change that script a bit????
python dataReduction.py -i $newestDir
