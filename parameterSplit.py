parameterSplit.py
#this script will read in a .csv dataset, sort based on node_id, and create multiple separate files for each node.
#try to pull out the lats and lons of each node, maybe ALSO take out any nodes that don't span the entire time period in this script
#setup is the same, reading in the data is the same, then we need to sort and write into DIFFERENT FILES.

from collections    import OrderedDict
from datetime  import timedelta
import subprocess
import argparse
import datetime
import shutil
import errno
import time
import csv
import io
import os
import re

def setup():
    global inputFile
    global dirPath
    global subDir
    global fileName
    global printLines
    global lines
    global fileName

    #set up command line arguments
    parser = argparse.ArgumentParser(description="average and reduce .csv file data sets from data.csv.gz")
    parser.add_argument('-i','--input', dest='path', help="Path to unpackaged complete node data set. (e.g. '-i /home/name/AoT_Chicago.complete.2018-06-19')")
    args = parser.parse_args()

    #check that path exists and contains the necessary files
    if os.path.exists(args.path):
        dirPath = str(args.path)
        if not os.path.isfile(str(dirPath+"/data.csv")) or not os.path.isfile(str(dirPath+"/nodes.csv")) or not os.path.isfile(str(dirPath+"/sensors.csv")) or not os.path.isfile(str(dirPath+"/provenance.csv")) or not os.path.isfile(str(dirPath+"/README.md")):
            print("Error: Files missing from input directory path.")
            exit(1)
    else:
        print("Error: Path does not exist. Specify full path to unpackaged complete node data set")
        exit(1);

    #remove trailing slash if user includes it
    if (str(dirPath[-1:]) == "/"):
        dirPath = dirPath[:-1]
    else:
        #set the input file (full path to file)
        inputFile = dirPath+"/data.csv"

        #create path names
        dirList = dirPath.split("/")
        parentDir = dirList[len(dirList)-1]
        subDir = dirPath + "/" + parentDir + "_split_by_parameter"
        fileName = subDir + "/data.csv"

#Sort data into separate nodes!
def sortData():
    global inputFile
    global outputFiles
    global fieldNames
    global printLines
    global lines
    global nodeTitle
    global count
    global nodeID
    global keys
    global node
    global outputDict
    global fileNames
    global nodeList
    global file

    valStr = ' '
    count = 0
    nodeID=[]
    outputDict = {}
    fileNames = []
    nodeList = []

    with open(inputFile, "r") as file:
        #create a csv reader object (it is an Ordered Dictionary of all the rows from the .csv file)
        #populate the fieldNames variable to place at the top of the output .csv file
        #replace any null values so that no errors occur
        reader = csv.DictReader(x.replace('\0', 'NullVal') for x in file)
        fieldNames = reader.fieldnames

        #sensor needs to have node_id (don't know why it wouldn't but keep this as a safety measure)
        for i in range(0,len(fieldNames)):
            if (fieldNames[i] == "parameter"):
                parameterName = "parameter"

        if (parameterName != "parameter"):
            print("Error: Could not find appropriate value header. CSV file headers must include 'parameter' for this tool to function.")
            exit(1)

        #go through each line of the input .csv file, make a list of each of the different node IDs included in the dataset
        #the lists are stored in a dictionary with keys corresponding to each different node_id
        for row in reader:
            parameter = str(row[parameterName])
            nodeList.append(parameter)
            outputDict.setdefault(parameter, []).append(row.copy())

            #create new file paths for each unique node
            if parameter not in fileNames:
                fileNames.append(subDir + "/" + parameter + ".csv")

        outputFiles = fileNames

def writeFile():
    global outputFile
    global subDir
    global fieldNames
    global outputDict
    global dirPath
    global hrfTitle
    global fileName
    global nodeDir
    global keys
    global fileNames

    #create the sub directory that will contain each data and the copied metadata files
    if not os.path.exists(os.path.dirname(fileName)):
        try:
            for i in range(0, len(fileNames)):
                os.makedirs(os.path.dirname(fileNames[i]))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

    for key, val in outputDict.items():
        keys = val[0].keys()
        file = subDir + '/' + key + '.csv'
        open(file,'w').close()
        with open(file, 'w') as f:
            writer=csv.writer(f)
            writer.writerow(keys)
            for v in val:
                writer.writerow(v.values())

def copyDigestFiles():

    global dirPath
    global subDir
    global modifierText

    #copy the metadata files that are from the parent directory
    try:
        shutil.copyfile(dirPath+"/nodes.csv",subDir+"/nodes.csv",follow_symlinks=True)
        shutil.copyfile(dirPath+"/provenance.csv",subDir+"/provenance.csv",follow_symlinks=True)
        shutil.copyfile(dirPath+"/sensors.csv",subDir+"/sensors.csv",follow_symlinks=True)
        shutil.copyfile(dirPath+"/README.md",subDir+"/README.md",follow_symlinks=True)
    except shutil.Error as e:
        print('Error: %s' % e)
    except IOError as e:
        print('Error: %s' % e.strerror)

    #modify the README, create new README, delete old README
    modifierText = """## NOTE: This README has been modifed by nodeSplit.py, and the data included in this directory is sorted based on the parameters included in the dataset.\n
Within this README, the 'data.csv.gz' archive is referred to as the compressed CSV containing the sensor data file (data.csv). No rows have been removed or altered, just reorganized.
All other metadata mentioned in this README remains the same, except for the provenance metadata and the list of columns in data.csv.gz. Since this file no longer exists, these columns are incorrect.
The provenance.csv file contains the provenance for the original data set. Provenance for newly filtered data:
New Provenance - This data was reduced and combined with the original digest metadata on """ + str(datetime.datetime.utcnow()) + ". It has been modifed by the nodeSplit.py sorting tool.\n\n"

    newReadme = subDir + "/parameterREADME.md"
    oldReadme = subDir+"/README.md"
    with open (newReadme,'w') as n, open (oldReadme, "r") as o:
        text = o.read()
        n.write(modifierText+text)

    try:
        subprocess.run(["rm " + oldReadme.replace(" ","\ ")], shell=True, check=True)
        subprocess.run(["mv " + newReadme.replace(" ","\ ") + " " + subDir + "/README.md" ], shell=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

if __name__ == "__main__":

    #variable instantiations
    global inputFile
    global fieldNames
    global period
    global printLines
    global lines
    global dirPath
    global subDir
    global hrfTitle
    global fileName


    inputFile = ''
    outputFile = ''
    fieldNames = []
    printLines = False
    lines = 0
    dirPath = ''
    subDir = ''
    hrfTitle = ''
    fileName = ''

    #begin generating data
    print("Generating...")

    #begin timer for benchmarking
    timerStart = time.time()

    #get user input, reduce and store data, write new .csv file, output directory
    setup()
    sortData()
    writeFile()
    copyDigestFiles()

    #end timer and show run time
    timerEnd = time.time()
    runTime = timerEnd-timerStart
    print("Done. \n")
    print("Took %.2fs to complete." % runTime)
