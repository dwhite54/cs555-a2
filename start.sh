#!/bin/bash
while read machine
do scp cs555-a2.jar ${machine} && ssh ${machine} "java -jar cs555-a2.jar" &
done < $1
