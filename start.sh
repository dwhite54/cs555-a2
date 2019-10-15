#!/bin/bash
peerPort=50293
discoveryPort=50292
projHome=/s/red/b/nobackup/data/portable/cs555-a2
scp cs555-a2.jar chip.cs.colostate.edu:$projHome
tmux new -s cs555a2 -d
tmux send-keys -t cs555a2 "ssh -t chip.cs.colostate.edu 'cd $projHome; java -jar cs555-a2.jar --mode discovery --discovery-port $discoveryPort --discovery-machine chip.cs.colostate.edu'" Enter
tmux split-window -t cs555a2
tmux send-keys -t cs555a2 "ssh -t chip.cs.colostate.edu 'cd $projHome; java -jar cs555-a2.jar --mode storedata --discovery-port $discoveryPort --discovery-machine chip.cs.colostate.edu'" Enter
totalPanes=8
sleep 1
while read machine
do
	for i in $(seq 1 "$2")
	do
		if [ $totalPanes -eq 8 ]; then
		  tmux new-window -t cs555a2
		  totalPanes=0
		else
		  tmux split-window -t cs555a2
		  tmux select-layout -t cs555a2 even-vertical
		fi
		  sleep 0.1
		tmux send-keys -t cs555a2 "ssh -t ${machine}.cs.colostate.edu 'cd $projHome; java -jar cs555-a2.jar --mode peer --discovery-port $discoveryPort --discovery-machine chip.cs.colostate.edu'" Enter
		totalPanes=$((totalPanes+1))
	done
done < $1
tmux attach -t cs555a2
