#!/usr/bin/env bash

echo "in execute_data_collection.sh"
#exit 0
# @author ANIL SENER
PROPERTIES_FILE=application.properties
SHARED_SOURCE_DIR=$(./properties_parser.sh SHARED_SOURCE_DIR $PROPERTIES_FILE)
SHARED_SOURCE_DIR_USER=$(./properties_parser.sh SHARED_SOURCE_DIR_USER $PROPERTIES_FILE)
SHARED_SOURCE_DIR_PASSWORD=$(./properties_parser.sh SHARED_SOURCE_DIR_PASSWORD $PROPERTIES_FILE)
SOURCE_DIR=$(./properties_parser.sh SOURCE_DIR $PROPERTIES_FILE)
#EXEC_DIR=$(pwd)
if mount | grep "${SHARED_SOURCE_DIR} on ${SOURCE_DIR}"; then
	echo "Specified Source Directory is already mounted"
else
	sudo mount -t cifs -o username=$SHARED_SOURCE_DIR_USER,password=$SHARED_SOURCE_DIR_PASSWORD $SHARED_SOURCE_DIR $SOURCE_DIR
	echo "Specified Source Directory mounting completed."
fi
#rm -f "source_list.txt";
#find $SOURCE_DIR -name \*.zip -exec sh -c 'echo ${1};echo "$(zipinfo -2 "$1" )"'  _ {}  >>"source_list.txt" \;

#groovy -cp /opt/nifi-client/build/libs/nifi-client-0.3-all.jar  com.imshealthcare.NiFi_Data_Collection_Client.groovy $EXEC_DIR
java -jar build/libs/smallFiles_Data_Collection.jar
GROOVY_STATUS=${?}
echo "NIFI Client exited with: ${GROOVY_STATUS}"
if [ $GROOVY_STATUS -eq 0 ]
then
	echo "Data Collection Process is finalized successfully."
	exit 0
else
	echo "Data Collection Process is terminated with failure."
	exit 1
fi