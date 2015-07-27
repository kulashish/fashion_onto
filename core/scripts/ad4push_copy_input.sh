#!/bin/sh
copyDRInFiles() {
	rootFolder="copy"
	re=$1
	echo $re
	if [ -z $re ] 
	then
		re=`date +%Y%m%d`
	fi
	
	echo "Regular expression passed: $re"
	for i in `ls $rootFolder | grep $re`
	do	
		dE=`echo $(expr ${#i} - 4)`
		dS=`echo $(expr ${#i} - 11)`
		deviceStart=`echo $(expr ${#i} - 15)`
		deviceEnd=`echo $(expr ${#i} - 13)`
		yyyymmdd=`echo $i | cut -c $dS-$dE`
		device=`echo $i | cut -c $deviceStart-$deviceEnd`
		if [ "$device" = "517" ]
		then
			dName="android"
		else
			dName="ios"
		fi

		date -d $yyyymmdd "+%Y%m%d" > /dev/null 2>&1
  		res=$?
		if [ $res = "1" ]
		then
			echo "DateParse Error - File: $i. Date fetched: $yyyymmdd"
		else
			yyyy=`echo $yyyymmdd | cut -c 1-4`
			mm=`echo $yyyymmdd | cut -c 5-6`
			dd=`echo $yyyymmdd | cut -c 7-8`
			cmd="hadoop fs -copyFromLocal $i /data/input/ad4push/reactions_$dName/daily/$yyyy/$mm/$dd/"
			echo "Running command for File: $i:\r\n\t\t $cmd"
			eval $cmd
		fi
	done
}

copyDRInFiles $1 

