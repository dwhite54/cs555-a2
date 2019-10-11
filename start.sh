#!/bin/bash
tmux new -s cs555a2 -d
tmux send-keys -t cs555a2 "cd $(pwd); java -jar cs555-a2.jar --mode discovery --discovery-port 50293 --discovery-machine $(hostname) --peer-port 50292" Enter
while read machine
do
  #sleep 1
  tmux split-window -t cs555a2
  tmux select-layout -t cs555a2 tiled
  tmux send-keys -t cs555a2 "ssh -t ${machine} 'cd $(pwd); java -jar cs555-a2.jar --mode peer --discovery-port 50293 --discovery-machine $(hostname) --peer-port 50292'" Enter
done < $1
tmux split-window -t cs555a2
tmux select-layout -t cs555a2 tiled
tmux send-keys -t cs555a2 "cd $(pwd); java -jar cs555-a2.jar --mode storedata --discovery-port 50293 --discovery-machine $(hostname) --peer-port 50292" Enter
tmux attach -t cs555a2