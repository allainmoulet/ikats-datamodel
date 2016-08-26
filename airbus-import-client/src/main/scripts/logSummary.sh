#!/bin/bash

usage()
{
	echo "---- logSummary.sh : usage with the import tool ----------------------------"
	echo "----------------------------------------------------------------------------"
	echo "1) logSummary.sh"
	echo "  simple use: shows the global summary from the default log, datamanager.log"
	echo "2) logSummary.sh {logfile}"
	echo "   - with {logfile}: the log file path"
	echo "   shows the global summary from the specified log file"
	echo "3) logSummary.sh {file} last"
	echo "   - with {logfile}: the log file path"
	echo "   shows the summary of the last run of import tool, from the specified file"
	exit 1
}

FILE=./datamanager.log
if [ $# -ge 1 ]
then
	FILE=$1
	if [ ! -e $FILE ]
	then
	   usage
	fi
fi
if [ $# -ge 2 ]
then
    if [ $2 == 'last' ]
    then
        LAST_STARTED_IMPORT=`grep -n 'Starting AirbusMainClient' $FILE | tail -1 | sed -e 's/:.*//' `    
    else
    	usage
    fi
fi


echo '---------------------------------------------------------'
echo "--- logSummary applied on import tool log file: $FILE"
echo "---------------------------------------------------------"
if [ -z $LAST_STARTED_IMPORT ]
then
	grep -E 'Starting AirbusMainClient|Successful import|Failed import|AirbusMainClient::main ended' $FILE | sed -e 's/\[fr.cs.ikats.*\]//' | sed -e 's/|.*|//'
else
	tail -n +$LAST_STARTED_IMPORT $FILE | grep -E 'Starting AirbusMainClient|Successful import|Failed import|AirbusMainClient::main ended' | sed -e 's/\[fr.cs.ikats.*\]//' | sed -e 's/|.*|//'
fi

