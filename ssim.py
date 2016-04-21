#!/usr/bin/python
import os, sys, getopt, json, subprocess
from os import path

def getSettings(target):
    toCut = len(target.split("/")[-1])
    settings = os.getcwd() + "/" + target[:-(toCut)] + "ssim.manifest"
    return settings

def getSettingsData(path):
    try:
        with open(path, "r") as data_file:
            data = json.load(data_file)
    except IOError:
        print "Could not find 'ssim.manifest' file! Please make sure that you have placed it in the same directory as the jar file you wish to submit."
        sys.exit(0)

    return data

def sparkSubmit(data, target):
    sparkBinDir = data["spark_bin_dir"]
    defaultMasterSetting = data["default_master_setting"]
    jarDependencies = data["dependencies"]
    className = data["class_name"]

    sparkParams = "--master " + defaultMasterSetting

    if (len(jarDependencies) > 0):
        sparkParams += " --jars "
        for dependency in jarDependencies:
            if (dependency == jarDependencies[0]):
                sparkParams += dependency;
            else:
                sparkParams += "," + dependency;
    
    sparkParams += " --class " + className + " " + target
    sparkCommand = sparkBinDir + "/spark-submit " + sparkParams

    os.system("bash " + sparkCommand)

if (len(sys.argv) > 1):
    manifestPath = getSettings(sys.argv[1])
    data = getSettingsData(manifestPath)
    
    sparkSubmit(data, sys.argv[1])
else:
    print "Usage: ssim.py [target_jar] [master_setting]"
    sys.exit(0) 