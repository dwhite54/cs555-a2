package com.cs555.a2;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import static com.cs555.a2.Helper.BpH;
import static com.cs555.a2.Helper.HpID;

class Peer {
    static class PeerInfo {
        char ID;
        String address;
        int port;
    }

    private static ServerSocket ss;
    private static boolean shutdown = false;
    private static String discoveryMachine;
    private static int discoveryPort;
    private static PeerInfo me = null;
    private static PeerInfo[][] routingTable = new PeerInfo[HpID][BpH * 4];
    private static ConcurrentHashMap<Character, String> files = new ConcurrentHashMap<>();
    private static PeerInfo rightLeaf = null;
    private static PeerInfo leftLeaf = null;
    private static boolean hasLeft = false;

    Peer(String discoveryMachine, int discoveryPort, int peerPort, char ID) throws IOException {
        Peer.discoveryMachine = discoveryMachine;
        Peer.discoveryPort = discoveryPort;
        Peer.me = new PeerInfo();
        Peer.me.ID = ID;
        Peer.me.address = InetAddress.getLocalHost().getCanonicalHostName();
        Peer.me.port = peerPort;
        ss = new ServerSocket(me.port);
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
        if (me != null)
            System.out.printf("Peer %s@%s:%d: %s%n",
                    Integer.toHexString(me.ID), me.address, me.port, s);
        else
            System.out.println("Peer NULL: " + s);
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
        if (leftLeaf != null)
            printString += String.format(" left: %s@%s:%d",
                    Integer.toHexString(leftLeaf.ID), leftLeaf.address, leftLeaf.port);
        else
            printString += " left: null";
        if (rightLeaf != null)
            printString += String.format(" right: %s@%s:%d",
                    Integer.toHexString(rightLeaf.ID), rightLeaf.address, rightLeaf.port);
        else
            printString += " right: null";
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
            if (!joinNodes.get(0).equals("")) { //else we are the first
                print("Joining via " + Integer.toHexString(joinIDs.get(0)) + "@" + joinNodes.get(0));

                //route self through table, getting rows of routing table along the way
                int rowIdx = 0;
                int hop = 0;
                while (rowIdx < routingTable.length) { // increment in inner for
                    int myColIdx = Helper.getValueAtHexIdx(me.ID, rowIdx);
                    String[] joinAddress = joinNodes.get(joinNodes.size() - 1).split(":");
                    try (
                            Socket peerSocket = new Socket(joinAddress[0], Integer.parseInt(joinAddress[1]));
                            DataInputStream pin = new DataInputStream(peerSocket.getInputStream());
                            DataOutputStream pout = new DataOutputStream(peerSocket.getOutputStream())
                    ) {
                        pout.writeUTF("join");
                        pout.writeChar(me.ID);
                        pout.writeInt(++hop);
                        pout.writeInt(rowIdx); // row index we want from the peer
                        int p = pin.readInt(); // length of prefix match
                        print("Matched " + p + " rows");
                        for (; rowIdx <= p; rowIdx++) {
                            for (int colIdx = 0; colIdx < HpID; colIdx++) {
                                char newID = pin.readChar();
                                String newAddress = pin.readUTF();
                                if (colIdx != myColIdx && newAddress.equals("")) {
                                    routingTable[rowIdx][colIdx] = null;
                                } else {
                                    PeerInfo newPeer = new PeerInfo();
                                    newPeer.ID = newID;
                                    newPeer.address = newAddress;
                                    routingTable[rowIdx][colIdx] = newPeer;
                                }
                            }
                            rowIdx++;
                        }
                        String nextAddress = pin.readUTF() + pin.readInt();
                        if (nextAddress.equals(joinNodes.get(joinNodes.size() - 1))) {//the one we just talked to is the best match
                            //now we need the leaf set from the last one we talked to (closest)
                            leftLeaf = new PeerInfo();
                            leftLeaf.ID = pin.readChar();
                            leftLeaf.address = pin.readUTF();
                            leftLeaf.port = pin.readInt();
                            rightLeaf = new PeerInfo();
                            rightLeaf.ID = pin.readChar();
                            rightLeaf.address = pin.readUTF();
                            rightLeaf.port = pin.readInt();
                            break;
                        }
                        joinIDs.add(pin.readChar());
                        joinNodes.add(nextAddress);
                    } catch (IOException e) {
                        dout.writeBoolean(false);
                        e.printStackTrace();
                        return;
                    }
                }
                propagateSelf(din);

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

    private void propagateSelf(DataInputStream din) throws IOException {
        int hop = 0;
        for (PeerInfo[] row : routingTable) {
            for (PeerInfo cell : row) {
                if (cell != null) {
                    sendContents(din, ++hop, cell);
                }
            }
        }
        if (leftLeaf != null) sendContents(din, ++hop, leftLeaf);
        if (rightLeaf != null) sendContents(din, ++hop, rightLeaf);
    }

    private void sendContents(DataInputStream din, int hop, PeerInfo peerInfo) throws IOException {
        try (
                var s = new Socket(peerInfo.address, peerInfo.port);
                var out = new DataOutputStream(s.getOutputStream())
        ) {
            out.writeUTF("propagate");
            out.writeChar(me.ID);
            out.writeInt(++hop);
            out.writeUTF(me.address);
            out.writeInt(me.port);
            writeRouting(out);
            out.writeChar(leftLeaf.ID);
            out.writeUTF(leftLeaf.address);
            out.writeInt(leftLeaf.port);
            out.writeChar(rightLeaf.ID);
            out.writeUTF(rightLeaf.address);
            out.writeInt(rightLeaf.port);
            int numFiles = din.readInt();
            for (int i = 0; i < numFiles; i++) {
                char fileID = din.readChar();
                String filename = din.readUTF();
                int fileSize = din.readInt();
                byte[] fileContents = new byte[fileSize];
                int bytesRead = din.read(fileContents);
                if (bytesRead != fileSize)
                    print("Error in file transmission");
                else {
                    File file = new File(Paths.get(Helper.peerStoragePath, filename).toString());
                    new FileOutputStream(file).write(fileContents);
                    files.put(fileID, filename);
                }
            }
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
                    case "showrouting":
                        printTable();
                    case "showleaves":
                        printLeaves();
                        break;
                    case "showfiles":
                        printFiles();
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
            try {
                String cmd = in.readUTF();
                char ID = in.readChar();
                int hop = in.readInt();
                switch (cmd) {
                    case "insert":
                        handleInsert(ID, hop);
                        break;
                    case "join":
                        handleJoin(ID, hop);
                        break;
                    case "propagate":
                        print("Received 'propagate' command");
                        PeerInfo newNode = new PeerInfo();
                        newNode.ID = ID;
                        newNode.address = in.readUTF();
                        newNode.port = in.readInt();
                        handlePropagate(newNode);
                        break;
                }
                this.in.close();
                this.out.close();
                this.s.close();
            } catch (IOException e) {
                print("unknown error");
                e.printStackTrace();
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
                if (in.read(fileContents) == fileSize) {
                    Path fileHome = Paths.get(Helper.peerStoragePath);
                    if (Files.notExists(fileHome))
                        Files.createDirectories(fileHome);
                    File file = new File(Paths.get(Helper.peerStoragePath, filename).toString());
                    print(String.format("Writing file %s named %s size %d",
                            Integer.toHexString(ID), filename, fileContents.length));
                    new FileOutputStream(file).write(fileContents);
                    files.put(ID, filename);
                }
            } else {
                out.writeChar(bestPeer.ID);
                print(String.format("Received 'insert' request for ID %s, hop %d, routing to %s@%s",
                        ID, hop, Integer.toHexString(bestPeer.ID), bestPeer.address));
            }
        }

        private void handleJoin(char ID, int hop) throws IOException {
            PeerInfo bestPeer;
            int rowIdx = in.readInt();
            int p = Helper.getLongestCommonPrefixLength(me.ID, ID);
            out.writeInt(p);
            for (int i = rowIdx; i <= p; i++) {
                for (PeerInfo cell : routingTable[i]) {
                    if (cell == null) {
                        out.writeChar(0);
                        out.writeUTF("");
                    } else {
                        out.writeChar(cell.ID);
                        out.writeUTF(cell.address);
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
                int meDist = Helper.ringDistance(me.ID, ID);
                // assume new is within bounds of our leaf set (otherwise would not be closest to us)
                if (meDist > 0) {  // new is clockwise of us
                    out.writeChar(me.ID);
                    out.writeUTF(me.address);
                    out.writeInt(me.port);
                    if (rightLeaf != null) {
                        out.writeChar(rightLeaf.ID);
                        out.writeUTF(rightLeaf.address);
                        out.writeInt(rightLeaf.port);
                    } else {
                        out.writeChar(me.ID);
                        out.writeUTF(me.address);
                        out.writeInt(me.port);
                    }
                } else {  // new is anticlockwise of us
                    if (leftLeaf != null) {
                        out.writeChar(leftLeaf.ID);
                        out.writeUTF(leftLeaf.address);
                        out.writeInt(leftLeaf.port);
                    } else {
                        out.writeChar(me.ID);
                        out.writeUTF(me.address);
                        out.writeInt(me.port);
                    }
                    out.writeChar(me.ID);
                    out.writeUTF(me.address);
                    out.writeInt(me.port);
                }
                //we don't update our leaves yet, as that message will come in the "joincomplete"
            } else {
                print(String.format("Received 'join' request for ID %s, hop %d, routing to %s@%s:%d",
                        ID, hop, Integer.toHexString(bestPeer.ID), bestPeer.address, bestPeer.port));
                out.writeChar(bestPeer.ID);
            }
        }

        private void handlePropagate(PeerInfo newNode) throws IOException {
            // a node joined and is broadcasting its information
            // we take their routing table and add each value in it to ours if we don't already have one
            if (readAndUpdateRouting(in)) {
                print("Table changed.");
                printTable();
            }

            //then we take their leaves and add them to our routing
            PeerInfo newLeftLeaf = new PeerInfo();
            newLeftLeaf.ID = in.readChar();
            newLeftLeaf.address = in.readUTF();
            newLeftLeaf.port = in.readInt();

            PeerInfo newRightLeaf = new PeerInfo();
            newRightLeaf.ID = in.readChar();
            newRightLeaf.address = in.readUTF();
            newRightLeaf.port = in.readInt();
            if (addToRouting(newLeftLeaf) || addToRouting(newRightLeaf)) {
                print("Leaves changed.");
                printLeaves();
            }

            // if they're closer than one of our leaves, we replace that leaf with them
            // (we don't need to worry about propagating this to OUR leaves, since they receive this same message)
            int newDist = Helper.ringDistance(me.ID, newNode.ID);

            //if closer than left, replace left with new
            //if closer than right, replace right with new
            if (newDist < 0 && Helper.ringDistance(leftLeaf.ID, newNode.ID) >= 0)
                leftLeaf = newNode;
            else if (newDist >= 0 && Helper.ringDistance(rightLeaf.ID, newNode.ID) <= 0)
                rightLeaf = newNode;

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
                file.delete();
                files.remove(fileID);
            }
            //TODO print contents if changed
        }

        private PeerInfo route(char ID) throws IOException {
            //if i don't have it but within leaf set range, forward to closest leaf (could be me)
            int meDist = Helper.ringDistance(me.ID, ID);
            if (leftLeaf != null) {
                int leftDist = Helper.ringDistance(leftLeaf.ID, ID);
                if (meDist < 0 && leftDist >= 0) { // to the left and within leaf set
                    if (-meDist < leftDist) {
                        return me;
                    } else {
                        return leftLeaf;
                    }
                }
            } else if (rightLeaf != null) {
                int rightDist = Helper.ringDistance(rightLeaf.ID, ID);
                if (meDist >= 0 && rightDist <= 0) { // to the right and within leaf set
                    if (meDist < -rightDist) {
                        return me;
                    } else {
                        return rightLeaf;
                    }
                }
            }

            return routeWithTable(ID);
        }

        private PeerInfo routeWithTable(char ID) throws IOException {
            //route with routing table
            int rowIdx = Helper.getLongestCommonPrefixLength(ID, me.ID);
            int colIdx;
            if (rowIdx < 0) { // no match found--illegal routing table
                out.writeUTF(""); // this will indicate internal error
                throw new IOException("Illegal routing table.");
            } else if (rowIdx == routingTable.length - 1) {  // match (should've been caught by leaf search)
                return me;
            }
            colIdx = Helper.getValueAtHexIdx(ID, rowIdx + 1);
            PeerInfo bestPeer = routingTable[rowIdx][colIdx];
            if (bestPeer == null) {
                //need to find a peer closer than us
                //search leaf set and current row
                bestPeer = getClosestLeaf(ID);
                if (bestPeer == null)
                    bestPeer = me;
                else {
                    for (PeerInfo rowPeer : routingTable[rowIdx]) {
                        if (rowPeer != null && rowPeer.ID != me.ID &&
                                Math.abs(Helper.ringDistance(rowPeer.ID, ID)) <
                                        Math.abs(Helper.ringDistance(bestPeer.ID, ID)))
                            bestPeer = rowPeer;
                    }
                }
            }
            return bestPeer;
        }

        private PeerInfo getClosestLeaf(char ID) {
            if (leftLeaf == null && rightLeaf == null) // all null, no dice!
                return null;
            else if (leftLeaf == null) // right isn't null, left is
                return rightLeaf;
            else if (rightLeaf == null) //left isn't null, right is
                return leftLeaf;
            else { //both non-null, do comparison
                int leftDist = Math.abs(Helper.ringDistance(leftLeaf.ID, ID));
                int rightDist = Helper.ringDistance(rightLeaf.ID, ID);
                if (Math.abs(leftDist) < Math.abs(rightDist))
                    return leftLeaf;
                else
                    return rightLeaf;
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
        int rowIdx = Helper.getLongestCommonPrefixLength(me.ID, peer.ID);
        if (rowIdx == routingTable.length - 1) return false;  //same ID
        int colIdx = Helper.getValueAtHexIdx(peer.ID, rowIdx + 1);
        routingTable[rowIdx][colIdx] = peer;
        return true;
    }

    private static boolean readAndUpdateRouting(DataInputStream in) throws IOException {
        boolean didChange = false;
        for (int i = 0; i < HpID; i++) {
            for (int j = 0; j < BpH*4; j++) {
                PeerInfo newPeer = new PeerInfo();
                newPeer.ID = in.readChar();
                newPeer.address = in.readUTF();
                newPeer.port = in.readInt();
                if (!newPeer.address.equals("") && Helper.rng.nextBoolean()) {
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
                    out.writeChar((char)0);
                    out.writeUTF("");
                    out.writeInt(0);
                } else {
                    out.writeChar(cell.ID);
                    out.writeUTF(cell.address);
                    out.writeInt(cell.port);
                }
            }
        }
    }
}
