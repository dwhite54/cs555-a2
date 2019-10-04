#!/bin/bash
while read machine
	do ssh ${machine} "echo $HOST" &
done < $1
