import os
from os import listdir
from os.path import isfile, join


mypath = "/data/ad4push"
onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]

for files in onlyfiles:
        print("processingFile: " + files)
        if("exportDevices_517" in files):
                y=files[18:22]
                m=files[22:24]
                d=files[24:26]
                date = y+"/"+m+"/"+d
                print(date)
                os.system("hadoop fs -mkdir -p /data/input/ad4push/devices_android/daily/"+date+"/")
                os.system("sed '/^$/d' "+mypath+"/"+files+" | sed -n 'H;g;/^[^\"]*\"[^\"]*\(\"[^\"]*\"[^\"]*\)*$/d; s/^\\n//; y/\\n/ /; p; s/.*//; h' >"+mypath+"/cleaned/"+files)
                os.system("hadoop fs -copyFromLocal "+mypath+"/cleaned/"+files+" /data/input/ad4push/devices_android/daily/"+date)
        elif("exportDevices_515" in files):
                y=files[18:22]
                m=files[22:24]
                d=files[24:26]
                date = y+"/"+m+"/"+d
                print(date)
                os.system("hadoop fs -mkdir -p /data/input/ad4push/devices_ios/daily/"+date+"/")
                os.system("sed '/^$/d' "+mypath+"/"+files+" | sed -n 'H;g;/^[^\"]*\"[^\"]*\(\"[^\"]*\"[^\"]*\)*$/d; s/^\\n//; y/\\n/ /; p; s/.*//; h' >"+mypath+"/cleaned/"+files)
                os.system("hadoop fs -copyFromLocal "+mypath+"/cleaned/"+files+" /data/input/ad4push/devices_ios/daily/"+date)