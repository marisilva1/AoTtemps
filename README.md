# AoTtemps

This repo will focus on filtering and sorting the Chicago public tarball data.
So far I have not used wget to obtain the dataset, and currently have the portion I need already downloaded.

Steps to manipulate data:
1. Slice date range
2. Remove outliers (outlierScript)
3. Split into separate folders based on node 
**(Check to see if all nodes span this time frame?)**
4. Use the dataReduction.py tool to average the separate nodes over 15-minute intervals
5. Do analysis - reorganize the data into separate columns for temperature, pressure, and humidity values if necessary, or just for cleanliness (or do this step sooner?)

... still only working on removing outliers but I'm close
