#outlierScripy.py
#this script should simply take out rows of data that do not contain values within a reasonable range.
#should create a new .csv file with all the same keys but with some rows of values removed.
#so I "finished" the script on 7/27/18, but it isn't working yet so it's not finished I'm lying

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
    #might not need all of these but declaring them won't hurt I don't think...
    global inputFile
    global outputFile
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
        subDir = dirPath + "/" + parentDir + "_no_outliers"
        fileName = subDir + "/data.csv"

        #set the output file (full path to file)
        outputFile = fileName

#take out the outliers
def createData():
    global inputFile
    global fieldNames
    global outputDict
    global printLines
    global lines
    global hrfTitle
    global count
    
    valStr = ' '
    count = 0
    
    with open(inputFile, "r") as file:
        #create a csv reader object (it is an Ordered Dictionary of all the rows from the .csv file)
        #populate the fieldNames variable to place at the top of the output .csv file
        #replace any null values so that no errors occur (does not apply to me I don't think)
        reader = csv.DictReader(x.replace('\0', 'NullVal') for x in file)
        fieldNames = reader.fieldnames

        #sensor values can come from the original data set or the moving average tool; otherwise, the tool cannot function
        for i in range(0,len(fieldNames)):
            if (fieldNames[i] == "value_hrf"):
                hrfTitle = "value_hrf"
            elif (fieldNames[i] == "value_hrf_moving_average"):
                hrfTitle = "value_hrf_moving_average"

        if (hrfTitle != "value_hrf" and hrfTitle != "value_hrf_moving_average"):
                    print("Error: Could not find appropriate value header. CSV file headers must include either 'value_hrf' or 'value_hrf_moving_average'.")
                    exit(1)

        #go through each line of the input .csv file
        #does 'fieldNames' mean I can put in any name - so like 'pressure' and it will recognize that in a row?
        for row in reader:
            parameter = row["parameter"]
            value = row["value_hrf"]
            count = 0

            #keep temps between -20 and 50 celsius
            if parameter == "temperature":
                if value < -20 | value > 50:
                    del row
                    count = count+1

            #no humidity below 10 or above 100, in g/m^3
            elif parameter == "humidity":
                if value < 10 | value > 100:
                    del row
                    count = count+1

            #no pressure below 100 or above 1100 in millibars
            elif parameter == "pressure":
                if value < 100 | value > 1100:
                    del row
                    count = count+1

            else:
                print("Error: apparently you have a strange parameter that is not temperature, pressure, or humidity...")

# MAKE TEMPORARY DIRECTORY
            #create the temporary dictionary (uses each sensor's value_hrf (or value_hrf_moving_average) as part of the value for each of the keys in outputDict) to hold
            #the sum, count, min, and max of the sensor hrf values over the specified time period
            if ',' in row[hrfTitle]:
                newVal = '"'+ row[hrfTitle] +'"' #corrects error if commas are included in a sensor value
            else:
                newVal = row[hrfTitle]

            temp = {'value_hrf':newVal}

            #delete the value_raw, value_hrf_sum, and value_hrf_count columns if they exist
            try:
                del row['value_raw']
            except:
                pass
            try:
                del row['value_hrf_sum']
            except:
                pass
            try:
                del row['value_hrf_count']
            except:
                pass

            #iterate through the dictionary to generate the outputDict for each time period

            #if the key already exists (meaning the row has already been made and now the current value_hrf (or value_hrf_moving_average) just needs to be added to the sum and the count value needs to be incremented)
            #then try to set the value of the dictionary key (key is in the format: timestamp,node_id,subsystem,sensor,parameter) - skips any values that are 'NA' or are a mix of letters and numbers
            #dictionary value is another dictionary in the format: {'sum':sum,'count':count,'min':min,'max':max}
            #else just update the outputDict with the temporary dictionary created above that contains the first value_hrf(or value_hrf_moving_average) for the current key
            if valStr in outputDict:
                try:
                    #calculate min and max
                    currMax = max(float(newVal),float(outputDict[valStr]['max']))
                    currMin = min(float(newVal),float(outputDict[valStr]['min']))

                    outputDict[str(valStr)] = {'sum':str(float(outputDict[valStr]['sum'])+float(temp['sum'])),'count':outputDict[str(valStr)]['count']+1,'min':currMin,'max':currMax}

                    #if there are more than *beginMinMaxCalcs* values in the averaging period, add min and max to output file
                    if float(outputDict[str(valStr)]['count']) > beginMinMaxCalcs:
                        minmax = True
                except ValueError:
                    pass
            else:
                outputDict[str(valStr)] = temp

        valStr = ''

        print("%d rows with outlying values removed." % count)
        
def writeFile():

    global outputFile
    global fieldNames
    global outputDict
    global dirPath
    global hrfTitle
    global fileName

    #create the sub directory that will contain the reduced data and the copied metadata files
    if not os.path.exists(os.path.dirname(fileName)):
        try:
            os.makedirs(os.path.dirname(fileName))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

    #erase whatever is currently in output csv file
    open(outputFile,'w').close()

    #update the output csv file's first line with the field names (removing 'value_hrf' (or 'value_hrf_moving_average') and 'value_raw') in the following format:
    #timestamp,node_id,subsystem,sensor,parameter,value_raw,value_hrf
    with open (outputFile,'w') as f:

        #remove un used field names
        try:
            fieldNames.remove('value_raw')
        except:
            pass
        try:
            fieldNames.remove('value_hrf_count')
        except:
            pass
        try:
            fieldNames.remove('value_hrf_sum')
        except:
            pass

        for i in range(0,len(fieldNames)):
            f.write(str(fieldNames[i])+',')

    with open (outputFile,'a') as f:
        #include the new values in the dictionary of outputDict only. No need for new calculations, just translate what was saved into the temp to outputDict I hope
        for key,val in outputDict.items():
            #write the whole row with the outputDict key (timestamp,node_id,subsystem,sensor,parameter) and the outputDict values (value_raw and value_hrf)
            f.write('\n'+str(val)+'\n')
            
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
        modifierText = """## NOTE: This README has been modifed by outlierScript.py, and the data included in this directory is now filtered to be within reasonal value ranges.\n
    Within this README, the 'data.csv.gz' archive is referred to as the compressed CSV containing the sensor data file (data.csv). The data.csv file from this compressed archive has been replaced by the reduced data.csv.
    All other metadata mentioned in this README remains the same, except for the provenance metadata and the list of columns in data.csv.gz. Since this file no longer exists, these columns are incorrect.
    The columns remain the same but some of the rows have been removed if the values they contained were unreasonable with respect to the range of the parameter. \n
    Temperatures are between -20 and 50 Celsius, humidity is kept between 10 and 100 g/m^3, and pressure is kept between 100 and 1100 millibars.
    The provenance.csv file contains the provenance for the original data set. Provenance for newly filtered data:
    New Provenance - This data was reduced and combined with the original digest metadata on """ + str(datetime.datetime.utcnow()) + ". It has been modifed by the outlierScript.py outlier filtering tool.\n\n"

        newReadme = subDir + "/reducedREADME.md"
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
    global outputFile
    global fieldNames
    global outputDict
    global period
    global printLines
    global lines
    global hrfTitle
    global dirPath
    global subDir
    global fileName
    
    inputFile = ''
    outputFile = ''
    fieldNames = []
    outputDict = OrderedDict()
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
    createData()
    writeFile()
    copyDigestFiles()

    #end timer and show run time
    timerEnd = time.time()
    runTime = timerEnd-timerStart
    print("Done. \n")
    print("Took %.2fs to complete." % runTime)
