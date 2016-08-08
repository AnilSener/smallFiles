#!/usr/bin/env bash

#to put in cron
#sudo less /var/log/cron -> take hour
# sudo vim /etc/crontab
#  52  05  *  *  * cloudera /home/cloudera/IdeaProjects/smallFiles-scala-2-10-5-spark-1-6-0/execute_small_files_poc.sh 5
PROPERTIES_FILE=application.properties
SMALL_POC_HOME=$(pwd)
#HDFS_POC_DIR=poc-madrid/small-files-poc/
USER=$(./properties_parser.sh HADOOP_USER $PROPERTIES_FILE)
#USER=dt1dusr
INPUT=/user/${USER}/input/Week_W201604/Farmatic/V/2
OUTPUT=/user/${USER}/output
REP=3
if [ -x "$1" ];
then REP=$1
fi

${SMALL_POC_HOME}/execute_data_collection.sh
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo "well done!"
echo "well done!" >>  ${SMALL_POC_HOME}/LAUNCHING_SPARK_SUBMIT.FLAG
echo "input:  " ${INPUT}
echo "output: " ${OUTPUT}

spark-submit --master yarn-client  --conf "spark.authenticate.secret=true" ${SMALL_POC_HOME}/target/scala-2.10/smallFiles_Data_Acquisition.jar $REP $INPUT $OUTPUT
