package com.cs555.a2;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import static com.cs555.a2.Helper.BpH;
import static com.cs555.a2.Helper.HpID;

class Peer {
    static class PeerInfo {
        char ID;
        String address;
        int port;

        @Override
        public String toString() {
            return Integer.toHexString(ID) + " " + address + ":" + port;
        }
    }

    private static ServerSocket ss;
    private static boolean shutdown = false;
    private static String discoveryMachine;
    private static int discoveryPort;
    private static PeerInfo me = null;
    private static PeerInfo[][] routingTable = new PeerInfo[HpID][BpH * 4];
    private static ConcurrentHashMap<Character, String> files = new ConcurrentHashMap<>();
    private static PeerInfo largerLeaf = null;
    private static PeerInfo smallerLeaf = null;
    private static boolean hasLeft = false;

    Peer(String discoveryMachine, int discoveryPort, int peerPort, char ID) throws IOException {
        Peer.discoveryMachine = discoveryMachine;
        Peer.discoveryPort = discoveryPort;
        Peer.me = new PeerInfo();
        Peer.me.ID = ID;
        Peer.me.address = InetAddress.getLocalHost().getCanonicalHostName();
        largerLeaf = me;
        smallerLeaf = me;
        boolean failed = false;
        do {
            if (peerPort == 0 || failed) {
                peerPort = Helper.rng.nextInt(10000) + 50000;
                print("Trying to establish server on port " + peerPort);
            }
            Peer.me.port = peerPort;
            try {
                ss = new ServerSocket(me.port);
                failed = false;
            } catch (SocketException e) {
                failed = true;
            }
        } while (failed);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Close();
                mainThread.join();
            } catch (InterruptedException e) {
                System.exit(-1);
            }
        }));
    }

    private static void print(String s) {
        System.out.printf("Peer %s@%s:%d: %s%n", Integer.toHexString(me.ID), me.address, me.port, s);
    }

    private static void printTable() {
        print("ROUTING TABLE");
        System.out.print("|  ");
        for (int i = 0; i < BpH * 4; i++)
            System.out.print(Integer.toHexString(i) + "   |  ");
        System.out.println();
        for (PeerInfo[] row : routingTable) {
            System.out.print("| ");
            for (PeerInfo cell : row) {
                if (cell != null)
                    System.out.print(Integer.toHexString(cell.ID) + " | ");
                else
                    System.out.print("NULL | ");
            }
            System.out.println();
        }
    }

    private static void printFiles() {
        if (files.isEmpty())
            print("FILES: <EMPTY>");
        else {
            print("FILES: ");
            for (var file : files.entrySet())
                System.out.print(Integer.toHexString(file.getKey()) + ": " + file.getValue());
            System.out.println();
        }
    }

    private static void printLeaves() {
        String printString = "Leaves:";
        printString += String.format(" smaller: %s@%s:%d",
                Integer.toHexString(smallerLeaf.ID), smallerLeaf.address, smallerLeaf.port);
        printString += String.format(" larger: %s@%s:%d",
                Integer.toHexString(largerLeaf.ID), largerLeaf.address, largerLeaf.port);
        print(printString);
    }

    private static void printTrace(ArrayList<Character> joinIDs, ArrayList<String> joinNodes) {
        if (joinIDs.size() != joinNodes.size()) {
            print("Bad call of printTrace");
        }
        for (int i = 0; i < joinIDs.size(); i++)
            print("Trace: " + Integer.toHexString(joinIDs.get(i)) + "@" + joinNodes.get(i));
    }

    private static void initRouting() {
        for (int i = 0; i < routingTable.length; i++)
            for (int j = 0; j < routingTable[i].length; j++)
                if (j == Helper.getValueAtHexIdx(me.ID, i))
                    routingTable[i][j] = me;
    }

    private static void Close() {
        notifyDiscoveryOfDeath();
        try {
            ss.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        shutdown = true;
    }

    void run() throws IOException {
        joinSelf();

        // running infinite loop for getting
        // client request
        Thread mT = new Peer.SendHandler();
        mT.start();
        while (!shutdown)
        {
            Socket s = null;
            try
            {
                s = ss.accept();
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                Thread t = new ReceiveHandler(s, in, out);
                t.start();
            }
            catch (Exception e){
                if (s != null){
                    s.close();
                }
                e.printStackTrace();
            }
        }
    }

    private static PeerInfo readPeer(DataInputStream in) throws IOException {
        PeerInfo peer = new PeerInfo();
        peer.ID = in.readChar();
        peer.address = in.readUTF();
        peer.port = in.readInt();
        return peer;
    }

    private static void writePeer(DataOutputStream out, PeerInfo peer) throws IOException {
        out.writeChar(peer.ID);
        out.writeUTF(peer.address);
        out.writeInt(peer.port);
    }

    private static void writeEmptyPeer(DataOutputStream out) throws IOException {
        out.writeChar(0);
        out.writeUTF("");
        out.writeInt(0);
    }

    private void joinSelf() {
        ArrayList<String> joinNodes = new ArrayList<>();
        ArrayList<Character> joinIDs = new ArrayList<>();
        try (
                Socket discoverySocket = new Socket(discoveryMachine, discoveryPort);
                DataInputStream din = new DataInputStream(discoverySocket.getInputStream());
                DataOutputStream dout = new DataOutputStream(discoverySocket.getOutputStream())
        ) {
            dout.writeUTF("join");
            dout.writeChar(me.ID);
            while (din.readBoolean()) {  //while collision detected
                me.ID = Helper.GenerateID();
                print("Collision detected, trying new ID " + Integer.toHexString(me.ID));
                dout.writeChar(me.ID);
            }
            initRouting();
            dout.writeUTF(me.address);
            dout.writeInt(me.port);
            joinIDs.add(din.readChar());
            joinNodes.add(din.readUTF() + ":" + din.readInt());
            if (!joinNodes.get(0).equals(":0")) { //else we are the first
                print("Joining via " + Integer.toHexString(joinIDs.get(0)) + "@" + joinNodes.get(0));

                //route self through table, getting rows of routing table along the way
                int hop = 0;
                boolean done = false;
                while (!done) { // increment in inner for
                    String[] joinAddress = joinNodes.get(joinNodes.size() - 1).split(":");
                    try (
                            Socket peerSocket = new Socket(joinAddress[0], Integer.parseInt(joinAddress[1]));
                            DataInputStream pin = new DataInputStream(peerSocket.getInputStream());
                            DataOutputStream pout = new DataOutputStream(peerSocket.getOutputStream())
                    ) {
                        pout.writeUTF("join");
                        pout.writeChar(me.ID);
                        pout.writeInt(++hop);
                        int p = pin.readInt(); // length of prefix match
                        print("Matched " + p + " rows");
                        for (int i = 0; i <= p; i++) {
                            for (int j = 0; j < BpH*4; j++) {
                                PeerInfo newPeer = readPeer(pin);
                                addToRouting(newPeer);
                            }
                        }
                        String nextAddress = pin.readUTF() + ":" + pin.readInt();
                        if (nextAddress.equals(joinNodes.get(joinNodes.size() - 1))) {//the one we just talked to is the best match
                            //now we need the leaf set from the last one we talked to (closest)
                            smallerLeaf = readPeer(pin);
                            largerLeaf = readPeer(pin);
                            done = true;
                        } else {
                            joinIDs.add(pin.readChar());
                            joinNodes.add(nextAddress);
                        }
                    } catch (IOException e) {
                        dout.writeBoolean(false);
                        e.printStackTrace();
                        return;
                    }
                }
                propagateSelf();

            } else {
                print("We are the first in the network.");
            }
            dout.writeBoolean(true);
        } catch (IOException e) {
            print("Failed getting join node from discovery.");
            e.printStackTrace();
        }
        print("Join Complete");
        printTable();
        printLeaves();
        printFiles();
        printTrace(joinIDs, joinNodes);
    }

    private void propagateSelf() throws IOException {
        HashSet<Character> IDs = new HashSet<>();

        if (smallerLeaf != null) sendContents(smallerLeaf, IDs);
        if (largerLeaf != null) sendContents(largerLeaf, IDs);

        for (PeerInfo[] row : routingTable) {
            for (PeerInfo cell : row) {
                if (cell != null && cell.ID != me.ID) {
                    sendContents(cell, IDs);
                }
            }
        }
    }

    private void sendContents(PeerInfo peerInfo, HashSet<Character> IDs) throws IOException {
        if (!IDs.contains(peerInfo.ID)) {
            IDs.add(peerInfo.ID);
            try (
                    var s = new Socket(peerInfo.address, peerInfo.port);
                    var in = new DataInputStream(s.getInputStream());
                    var out = new DataOutputStream(s.getOutputStream())
            ) {
                out.writeUTF("propagate");
                writePeer(out, me);
                writePeer(out, smallerLeaf);
                writePeer(out, largerLeaf);
                writeRouting(out);
                int numFiles = in.readInt();
                for (int i = 0; i < numFiles; i++) {
                    char fileID = in.readChar();
                    String filename = in.readUTF();
                    int fileSize = in.readInt();
                    byte[] fileContents = new byte[fileSize];
                    in.readFully(fileContents);
                    File file = new File(Paths.get(Helper.peerStoragePath, filename).toString());
                    new FileOutputStream(file).write(fileContents);
                    files.put(fileID, filename);
                }
            }
        }
    }

    private static void sendRing(String msg, int hop) {
        try (
                var s = new Socket(largerLeaf.address, largerLeaf.port);
                var out = new DataOutputStream(s.getOutputStream())
        ) {
            out.writeUTF("ring");
            out.writeUTF(msg + " -> " + Integer.toHexString(largerLeaf.ID));
            out.writeInt(hop);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class SendHandler extends Thread {
        @Override
        public void run() {
            boolean exit = false;
            Scanner scanner = new Scanner(System.in);
            String input;
            System.out.println("Type a command and hit return. To see available commands, type 'help'");
            while (!exit) {
                System.out.print(">");
                input = scanner.nextLine();
                String[] inputs = input.split(" ");
                switch (inputs[0]) {
                    case "quit":
                    case "exit":
                    case "bye":
                        exit = true;
                        break;
                    case "showtable":
                        printTable();
                        break;
                    case "":
                    case "showrouting":
                        printTable();
                    case "showleaves":
                        printLeaves();
                        break;
                    case "showfiles":
                        printFiles();
                        break;
                    case "ring":
                        sendRing(Integer.toHexString(me.ID), 0);
                        break;
                    default:
                        System.out.println("Invalid verb");
                    case "help":
                        printHelp();
                        break;
                }
            }
            System.exit(0);
        }

        private void printHelp() {
            System.out.println("Available commands:");
            System.out.println("\t[quit,exit,bye]: Exit the program.");
            System.out.println("\tshowtable: print routing table");
            System.out.println("\tshowleaves: print leaf set");
            System.out.println("\tshowrouting: print routing table and leaf set");
            System.out.println("\tshowfiles: print filenames stored here");
            System.out.println("\thelp: print this list.");
        }
    }

    // ClientHandler class
    static class ReceiveHandler extends Thread
    {
        final DataInputStream in;
        final DataOutputStream out;
        final Socket s;

        // Constructor
        ReceiveHandler(Socket s, DataInputStream in, DataOutputStream out)
        {
            this.s = s;
            this.in = in;
            this.out = out;
        }

        @Override
        public void run()
        {
            String ring = "";
            int ringHop = -1;
            try {
                String cmd = in.readUTF();
                switch (cmd) {
                    case "insert":
                        handleInsert(in.readChar(), in.readInt());
                        break;
                    case "join":
                        handleJoin(in.readChar(), in.readInt());
                        break;
                    case "propagate":
                        print("Received 'propagate' command");
                        handlePropagate();
                        break;
                    case "ring":
                        ring = in.readUTF();
                        ringHop = in.readInt();
                        break;
                }
                this.in.close();
                this.out.close();
                this.s.close();
            } catch (IOException e) {
                print("unknown error");
                e.printStackTrace();
            }
            if (ring.length() > 0) {
                if (ring.substring(0, 4).equals(Integer.toHexString(me.ID)) || ringHop >= 500) {
                    System.out.println("RING END");
                    System.out.println(ring);
                    System.out.println(++ringHop);
                } else {
                    sendRing(ring, ++ringHop);
                }
            }
        }

        private void handleInsert(char ID, int hop) throws IOException {
            PeerInfo bestPeer;
            bestPeer = route(ID);
            out.writeUTF(bestPeer.address);
            out.writeInt(bestPeer.port);
            if (bestPeer.ID == me.ID) {
                print(String.format("Received 'insert' request for ID %s, hop %d, and I am closest.",
                        Integer.toHexString(ID), hop));
                String filename = in.readUTF();
                int fileSize = in.readInt();
                byte[] fileContents = new byte[fileSize];
                in.readFully(fileContents);
                Path fileHome = Paths.get(Helper.peerStoragePath);
                if (Files.notExists(fileHome))
                    Files.createDirectories(fileHome);
                File file = new File(Paths.get(Helper.peerStoragePath, filename).toString());
                print(String.format("Writing file %s named %s size %d",
                        Integer.toHexString(ID), filename, fileContents.length));
                new FileOutputStream(file).write(fileContents);
                files.put(ID, filename);

            } else {
                out.writeChar(bestPeer.ID);
                print(String.format("Received 'insert' request for ID %s, hop %d, routing to %s@%s",
                        ID, hop, Integer.toHexString(bestPeer.ID), bestPeer.address));
            }
        }

        private void handleJoin(char ID, int hop) throws IOException {
            PeerInfo bestPeer;
            int p = Helper.getLongestCommonPrefixLength(me.ID, ID);
            out.writeInt(p);
            for (int i = 0; i <= p; i++) {
                for (PeerInfo cell : routingTable[i]) {
                    if (cell == null) {
                        writeEmptyPeer(out);
                    } else {
                        writePeer(out, cell);
                    }
                }
            }
            bestPeer = route(ID);
            out.writeUTF(bestPeer.address);
            out.writeInt(bestPeer.port);
            //if we are closest to this one, share leaf set and propagate
            if (bestPeer.ID == me.ID) {
                print(String.format("Received 'join' request for ID %s, hop %d, and I am closest.",
                        Integer.toHexString(ID), hop));
                // assume new is within bounds of our leaf set (otherwise would not be closest to us)
                if (Helper.isBetween(me.ID, ID, largerLeaf.ID)) {  //closer to larger
                    writePeer(out, me);
                    writePeer(out, largerLeaf);
                } else if (Helper.isBetween(largerLeaf.ID, ID, me.ID)) {  // new is anticlockwise of us
                    writePeer(out, smallerLeaf);
                    writePeer(out, me);
                } else { //error condition
                    print("INVALID ROUTE--ID ROUTED TO ME BUT ID NOT WITHIN MY LEAF SET");
                    writePeer(out, me);
                    writePeer(out, me);
                }
                //we don't update our leaves yet, as that message will come in the "joincomplete"
            } else {
                print(String.format("Received 'join' request for ID %s, hop %d, routing to %s@%s:%d",
                        Integer.toHexString(ID), hop, Integer.toHexString(bestPeer.ID), bestPeer.address, bestPeer.port));
                out.writeChar(bestPeer.ID);
            }
        }

        private void handlePropagate() throws IOException {
            // a node joined and is broadcasting its information
            //then we take their leaves and add them to our routing
            // we take their routing table and add each value in it to ours if we don't already have one
            PeerInfo newNode = readPeer(in);
            PeerInfo newSmallerLeaf = readPeer(in);
            PeerInfo newLargerLeaf = readPeer(in);
            boolean smallerResult = addToRouting(newSmallerLeaf);
            boolean largerResult = addToRouting(newLargerLeaf);
            boolean routingResult = readAndUpdateRouting(in);
            if (smallerResult || largerResult || routingResult) {
                print("Table changed.");
                printTable();
            }

            boolean leavesChanged = false;
            //if closer than left, replace left with new
            // (we don't need to worry about propagating this to OUR leaves, since they receive this same message)
            if (Helper.isBetween(smallerLeaf.ID, newNode.ID, me.ID)) {
                smallerLeaf = newNode;
                leavesChanged = true;
            }

            //if closer than right, replace right with new
            if (Helper.isBetween(me.ID, newNode.ID, largerLeaf.ID)) {
                largerLeaf = newNode;
                leavesChanged = true;
            }

            if (leavesChanged) {
                print("Leaves changed.");
                printLeaves();
            }

            //finally, if they are now closer to any files of ours, we give them those (could be done above)
            ArrayList<Character> filesToMove = new ArrayList<>();
            for (char fileID : files.keySet()) {
                if (Helper.ringDistance(me.ID, fileID) > Helper.ringDistance(newNode.ID, fileID)) {
                    filesToMove.add(fileID);
                }
            }

            out.writeInt(filesToMove.size());
            for (char fileID : filesToMove) {
                String filename = files.get(fileID);
                out.writeChar(fileID);
                out.writeUTF(filename);
                File file = new File(Paths.get(Helper.peerStoragePath, filename).toString());
                byte[] fileContents = new FileInputStream(file).readAllBytes();
                out.writeInt(fileContents.length);
                print(String.format("New peer closer to file %s named %s size %d, migrating.",
                        Integer.toHexString(fileID), filename, fileContents.length));
                out.write(fileContents);
                if (file.delete())
                    files.remove(fileID);
                else
                    print("error removing file");
            }
        }

        private PeerInfo route(char ID) throws IOException {
            if (Helper.isBetween(smallerLeaf.ID, ID, largerLeaf.ID)) {
                return getClosestLeaf(ID);
            } else  //if beyond leaf of closest side, use table
                return routeWithTable(ID);
        }

        private PeerInfo routeWithTable(char ID) throws IOException {
            //route with routing table
            int p = Helper.getLongestCommonPrefixLength(ID, me.ID);
            int colIdx;
            if (p < 0) { // no match found--illegal routing table
                out.writeUTF(""); // this will indicate internal error
                throw new IOException("Illegal routing table.");
            } else if (p == routingTable.length) {  // match (should've been caught by leaf search)
                return me;
            }
            colIdx = Helper.getValueAtHexIdx(ID, p);
            PeerInfo bestPeer = routingTable[p][colIdx];
            if (bestPeer == null) {
                //need to find a peer closer than us
                //search leaf set and current row
                bestPeer = getClosestLeaf(ID);
                for (PeerInfo rowPeer : routingTable[p]) {
                    if (rowPeer != null && Helper.ringDistance(rowPeer.ID, ID) < Helper.ringDistance(bestPeer.ID, ID))
                        bestPeer = rowPeer;
                }
            }
            return bestPeer;
        }

        private PeerInfo getClosestLeaf(char ID) {
            int meDist = Helper.ringDistance(me.ID, ID);
            int largerLeafDist = Helper.ringDistance(largerLeaf.ID, ID);
            int smallerLeafDist = Helper.ringDistance(smallerLeaf.ID, ID);
            if ((meDist <= largerLeafDist && meDist <= smallerLeafDist)) {
                return me;
            } else if (largerLeafDist < smallerLeafDist) {
                return largerLeaf;
            } else {
                return smallerLeaf;
            }
        }
    }

    private static void notifyDiscoveryOfDeath() {
        if (!hasLeft) {
            try (
                    Socket discoverySocket = new Socket(discoveryMachine, discoveryPort);
                    DataOutputStream dout = new DataOutputStream(discoverySocket.getOutputStream())
            ) {
                dout.writeUTF("leave");
                dout.writeShort(me.ID);
                hasLeft = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean addToRouting(PeerInfo peer) {
        if (peer.address.equals("")) return false;
        int prefixLength = Helper.getLongestCommonPrefixLength(me.ID, peer.ID);
        if (prefixLength == routingTable.length) return false;  //same ID
        int colIdx = Helper.getValueAtHexIdx(peer.ID, prefixLength);
        int myColIdx = Helper.getValueAtHexIdx(me.ID, prefixLength);
        if (myColIdx != colIdx) {  //only update if it's not our spot
            routingTable[prefixLength][colIdx] = peer;
            return true;
        } else
            return false;
    }

    private static boolean readAndUpdateRouting(DataInputStream in) throws IOException {
        boolean didChange = false;
        for (int i = 0; i < HpID; i++) {
            for (int j = 0; j < BpH*4; j++) {
                PeerInfo newPeer = readPeer(in);
                if (!newPeer.address.equals("") && routingTable[i][j] != null) { //TODO rule? randomness?
                    if (addToRouting(newPeer)) {
                        didChange = true;
                    }
                }
            }
        }
        return didChange;
    }

    private static void writeRouting(DataOutputStream out) throws IOException {
        for (PeerInfo[] row : routingTable) {
            for (PeerInfo cell : row) {
                if (cell == null) {
                    writeEmptyPeer(out);
                } else {
                    writePeer(out, cell);
                }
            }
        }
    }
}
