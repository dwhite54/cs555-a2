package com.cs555.a2;
/*
16-bit hex IDs
create ID from command line or auto-generate

Discovery node
peers notify me on enter/exit of system
I maintain info about each peer, including ID, host:port, nickname
I notify peers to change their IDs upon collision
So, functions I provide are 1) get 1 random node from set and 2) detect/resolve collisions
I cannot tell new nodes about all current nodes

Leaf set
track 2L neighbors, L bigger and L smaller
L = 1

Routing table
as in class, but only 4 rows (16-bit / 4-bits per hex)
nullable values
doesn't track self
route/search by prefix
values are IP and ID of 1 peer matching prefix

Adding new peers
New node X asks A to join
A uses table to route to ID closest to X
Along the way, each node is telling X relevant routing table info (from entry peer, intermediate, destination)
Each node involved shares what's in its table down to the row which prefix-matches X's ID\
X's leaf set is the same as the one it was routed to, save for a single entry
Once X's leaf set and routing table are created, they send that info to all nodes in leaf set and routing table

Storing
store content at nodes whose ID is closest to the content ID

Diagnostics
print routing table and leaf set
print list of files
print message for every routing step, including hop number
    I'll add to/from also
Discover prints ID, host:port on add/remove, provides command for this also
See appendix B for more

Use a script
no object serialization
no 3rd party
use java.util.logging instead of system.out.print

 */

public class Main {



    public static void main(String[] args) {
	// write your code here
    }
}
