#!/usr/bin/env sh
rm /tmp/logdata
touch /tmp/logdata
tail -f /tmp/logdata | nc -lk 7777 &
TAIL_NC_PID=$!
#cont=10000000
#until [ $cont -lt 1 ];
#do
#    echo counter: $cont
#    cont=`expr $cont - 1`
    cat /home/cloudera/IdeaProjects/test-small-files-streaming/V0314616.2.04 >> /tmp/logdata
    #sleep 1
#done
kill $TAIL_NC_PID