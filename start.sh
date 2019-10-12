#!/bin/bash
peerPort=50293
discoveryPort=50292
projHome=/s/red/b/nobackup/data/portable/cs555-a2
scp cs555-a2.jar chip.cs.colostate.edu:$projHome
tmux new -s cs555a2 -d
tmux send-keys -t cs555a2 "ssh -t chip.cs.colostate.edu 'cd $projHome; java -jar cs555-a2.jar --mode discovery --discovery-port $discoveryPort --discovery-machine chip.cs.colostate.edu --peer-port $peerPort'" Enter
tmux split-window -t cs555a2
tmux select-layout -t cs555a2 tiled
tmux send-keys -t cs555a2 "ssh -t chip.cs.colostate.edu 'cd $projHome; java -jar cs555-a2.jar --mode storedata --discovery-port $discoveryPort --discovery-machine chip.cs.colostate.edu --peer-port $peerPort'" Enter
while read machine
do
  tmux split-window -t cs555a2
  tmux select-layout -t cs555a2 tiled
  tmux send-keys -t cs555a2 "ssh -t ${machine}.cs.colostate.edu 'cd $projHome; java -jar cs555-a2.jar --mode peer --discovery-port $discoveryPort --discovery-machine chip.cs.colostate.edu --peer-port $peerPort'" Enter
done < $1
tmux attach -t cs555a2