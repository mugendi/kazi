#!/bin/sh

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "$DIR/logs/server.log";

#clear logs
rm -rf "$DIR/logs/*";

#stop any forever processes
forever stopall;

echo 'Waiting for some 10 seconds';
sleep 10;

if [ $(ps -e -o uid,cmd | grep $UID | grep node | grep -v grep | wc -l | tr -s "\n") -eq 0 ]
then
    export PATH=/usr/local/bin:$PATH

	#start job server
	$(forever start  -o "$DIR/logs/server.log"  --sourceDir "$DIR" server.js >> "$DIR/logs/kazi-server.txt" 2>&1);
	
	# #start job clients
	$(forever start  -o "$DIR/logs/client.log"  --sourceDir "$DIR" client.js >> "$DIR/logs/kazi-client.txt" 2>&1);
	
	#set up some node jobs
	$(/usr/bin/node "$DIR/jobs.js" >> "$DIR/logs/kazi-jobs.txt" 2>&1);
	
fi
