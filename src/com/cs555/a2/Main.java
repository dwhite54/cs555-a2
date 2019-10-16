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

import org.w3c.dom.ranges.RangeException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class Main {
    public enum Mode {
        DISCOVERY,
        STOREDATA,
        PEER
    }

    private static Mode mode;
    private static int discoveryPort = 0;
    private static int peerPort = 0;
    private static String discoveryMachine = "";
    private static char peerId = Helper.GenerateID();

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        if (args.length == 0 || args[0].equals("--help")) {
            printUsage();
            System.exit(0);
        }
        for (int i = 0; i < args.length; i++) {
            try {
                switch (args[i]) {
                    case "--discovery-port":
                        discoveryPort = Integer.parseInt(args[i+1]);
                        break;
                    case "--discovery-machine":
                        discoveryMachine = args[i+1];
                        break;
                    case "--peer-port":
                        peerPort = Integer.parseInt(args[i+1]);
                        break;
                    case "--peer-id":
                        peerId = (char)Integer.parseInt(args[i+1], 16);
                        break;
                    case "--mode":
                        switch (args[i+1].toLowerCase()) {
                            case "discovery":
                                mode = Mode.DISCOVERY;
                                break;
                            case "storedata":
                                mode = Mode.STOREDATA;
                                break;
                            case "peer":
                                mode = Mode.PEER;
                                break;
                            default:
                                throw new IllegalArgumentException("Invalid mode");
                        }
                        break;
                }
            } catch (RangeException | IllegalArgumentException e) {  //catch range and parsing errors
                System.out.println("Error parsing arguments");
                printUsage();
                e.printStackTrace();
            }
        }

        switch (mode) {
            case DISCOVERY:
                DiscoveryNode d = new DiscoveryNode(discoveryPort);
                d.run();
                break;
            case STOREDATA:
                StoreData s = new StoreData(discoveryMachine, discoveryPort);
                s.run();
                break;
            case PEER:
                Peer p = new Peer(discoveryMachine, discoveryPort, peerPort, peerId);
                p.run();
                break;
        }
    }

    private static void printUsage() {
        System.out.println("Options:");
        System.out.println("\t--mode: [client,chunkserver,controller], omit this and provide --chunk-machines to start all processes.\n");
        System.out.println("\t--controller-port: port the controller will communicate with");
        System.out.println("\t--controller-machine: machine the controller will run on");
        System.out.println("\t--chunk-port: port the chunk servers will communicate with");
        System.out.println("\t--chunk-machines: (optional) comma-delimited list of machines the chunk servers will run on");
        System.out.println("\t--debug: (optional) print extra debug output");
        System.out.println("\t--replication: (optional) use replication (omit to use erasure coding, do not mix)");
    }
}
