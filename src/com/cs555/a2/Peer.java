package com.cs555.a2;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import static com.cs555.a2.Helper.BpH;
import static com.cs555.a2.Helper.HpID;

class Peer {
    static class PeerInfo {
        char ID;
        String address;
    }

    private ServerSocket ss;
    private boolean shutdown = false;
    private static int peerPort;
    private static String discoveryMachine;
    private static int discoveryPort;
    private static PeerInfo me = new PeerInfo();
    private static PeerInfo[][] routingTable = new PeerInfo[HpID][BpH * 4];
    private static ConcurrentHashMap<Character, String> files = new ConcurrentHashMap<>();
    private static PeerInfo rightLeaf = new PeerInfo();
    private static PeerInfo leftLeaf = new PeerInfo();
    private static final String pre = "Peer" + Integer.toHexString(me.ID) + ": ";

    Peer(String discoveryMachine, int discoveryPort, int peerPort, char ID) throws IOException {
        Peer.discoveryMachine = discoveryMachine;
        Peer.discoveryPort = discoveryPort;
        Peer.peerPort = peerPort;
        Peer.me.ID = ID;
        Peer.me.address = InetAddress.getLocalHost().getHostName();
        ss = new ServerSocket(Peer.peerPort);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Close();
                mainThread.join();
            } catch (InterruptedException | IOException e) {
                System.exit(-1);
            }
        }));
    }

    private static void print(String s) {
        System.out.println(pre+s);
    }

    private static void printTable() {
        print("ROUTING TABLE");
        for (PeerInfo[] row : routingTable) {
            System.out.print("| ");
            for (PeerInfo cell : row) {
                System.out.print(Integer.toHexString(cell.ID) + " | ");
            }
            System.out.println();
        }
    }

    private static void printFiles() {
        print("FILES");
        for (var file : files.entrySet())
            print(Integer.toHexString(file.getKey()) + ": " + file.getValue());
    }

    private void Close() throws IOException {
        notifyDiscoveryOfDeath();
        ss.close();
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
        notifyDiscoveryOfDeath();
    }

    private void joinSelf() {
        String joinNode;
        try (
                Socket discoverySocket = new Socket(discoveryMachine, discoveryPort);
                DataInputStream din = new DataInputStream(discoverySocket.getInputStream());
                DataOutputStream dout = new DataOutputStream(discoverySocket.getOutputStream())
        ) {
            dout.writeUTF("join");
            dout.writeShort(me.ID);
            while (din.readBoolean()) {  //while collision detected
                me.ID = Helper.GenerateID();
                print("Collision detected, trying new ID " + Integer.toHexString(me.ID));
                dout.writeShort(me.ID);
            }
            joinNode = din.readUTF();
            if (!joinNode.equals("")) { //else we are the first

                //route self through table, getting rows of routing table along the way
                int rowIdx = 0;
                while (rowIdx < routingTable.length) { // increment in inner for
                    int myColIdx = Helper.getValueAtHexIdx(me.ID, rowIdx);
                    try (
                            Socket peerSocket = new Socket(joinNode, peerPort);
                            DataInputStream pin = new DataInputStream(peerSocket.getInputStream());
                            DataOutputStream pout = new DataOutputStream(peerSocket.getOutputStream())
                    ) {
                        int p;
                        pout.writeUTF("join");
                        pout.writeChar(me.ID);
                        pout.writeInt(rowIdx); // row index we want from the peer
                        p = pin.readInt(); // length of prefix match
                        for (; rowIdx < p; rowIdx++) {
                            for (int colIdx = 0; colIdx < HpID; colIdx++) {
                                char newID = pin.readChar();
                                String newAddress = pin.readUTF();
                                if (colIdx == myColIdx) {
                                    routingTable[rowIdx][colIdx] = me;
                                } else if (newAddress.equals("")) {
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
                        String nextNode = pin.readUTF();
                        if (nextNode.equals(joinNode)) {//the one we just talked to is the best match
                            //now we need the leaf set from the last one we talked to (closest)
                            leftLeaf.ID = pin.readChar();
                            leftLeaf.address = pin.readUTF();
                            rightLeaf.ID = pin.readChar();
                            rightLeaf.address = pin.readUTF();
                            break;
                        }
                        joinNode = nextNode;
                    } catch (IOException e) {
                        dout.writeBoolean(false);
                        e.printStackTrace();
                        return;
                    }
                }

                //now update all nodes in our routing table and leaf sets to use us
                for (PeerInfo[] row : routingTable) {
                    for (PeerInfo cell : row) {
                        if (cell != null) {
                            try (
                                    var s = new Socket(cell.address, peerPort);
                                    var out = new DataOutputStream(s.getOutputStream())
                            ) {
                                out.writeUTF("propagate");
                                out.writeChar(me.ID);
                                out.writeUTF(me.address);
                                writeRouting(out);
                                out.writeChar(leftLeaf.ID);
                                out.writeUTF(leftLeaf.address);
                                out.writeChar(rightLeaf.ID);
                                out.writeUTF(rightLeaf.address);
                            } catch (IOException e) {
                                dout.writeBoolean(false);
                                e.printStackTrace();
                                return;
                            }
                        }
                    }
                }
            }
            dout.writeBoolean(true);
        } catch (IOException e) {
            print("Failed getting join node from discovery.");
            e.printStackTrace();
        }
    }

    private static class SendHandler extends Thread {
        @Override
        public void run() {
            boolean exit = false;
            while (!exit) {
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
                notifyDiscoveryOfDeath();
            }
        }

        private void printHelp() {
            System.out.println("Available commands:");
            System.out.println("\t[quit,exit,bye]: Exit the program.");
            System.out.println("\tshowtable: print routing table");
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
                switch (cmd) {
                    case "insert":
                        handleInsert(ID);
                        break;
                    case "join":
                        handleJoin(ID);
                        break;
                    case "propagate":
                        PeerInfo newNode = new PeerInfo();
                        newNode.ID = ID;
                        newNode.address = in.readUTF();
                        handlePropagate();
                        updateLeaves(newNode);
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

        private void handleInsert(char ID) throws IOException {
            PeerInfo bestPeer;
            bestPeer = route(ID);
            out.writeUTF(bestPeer.address);
            if (bestPeer.ID == me.ID) {
                String filename = in.readUTF();
                int fileSize = in.readInt();
                byte[] fileContents = new byte[fileSize];
                if (in.read(fileContents) == fileSize) {
                    Path chunkHome = Paths.get(Helper.peerStoragePath);
                    if (Files.notExists(chunkHome))
                        Files.createDirectories(chunkHome);
                    File file = new File(Paths.get(Helper.peerStoragePath, filename).toString());
                    new FileOutputStream(file).write(fileContents);
                    files.put(ID, filename);
                }
            }
        }

        private void handleJoin(char ID) throws IOException {
            PeerInfo bestPeer;
            int rowIdx = in.readInt();
            int p = Helper.getLongestCommonPrefixLength(me.ID, ID);
            out.writeInt(p);
            for (int i = rowIdx; i < p; i++) {
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
            out.writeUTF(bestPeer.address); //TODO null reference
            //if we are closest to this one, share leaf set and propagate
            if (bestPeer.ID == me.ID) {
                int meDist = Helper.ringDistance(me.ID, ID);
                // assume new is within bounds of our leaf set (otherwise would not be closest to us)
                if (meDist > 0) {  // new is clockwise of us
                    out.writeChar(me.ID);
                    out.writeUTF(me.address);
                    out.writeChar(rightLeaf.ID);
                    out.writeUTF(rightLeaf.address);
                } else {  // new is anticlockwise of us
                    out.writeChar(leftLeaf.ID);
                    out.writeUTF(leftLeaf.address);
                    out.writeChar(me.ID);
                    out.writeUTF(me.address);
                }
                //we don't update our leaves yet, as that message will come in the "joincomplete"
            }
        }

        private void handlePropagate() throws IOException {
            readAndUpdateRouting(in);

            PeerInfo newLeftLeaf = new PeerInfo();
            newLeftLeaf.ID = in.readChar();
            newLeftLeaf.address = in.readUTF();
            addToRouting(newLeftLeaf);

            PeerInfo newRightLeaf = new PeerInfo();
            newRightLeaf.ID = in.readChar();
            newRightLeaf.address = in.readUTF();
            addToRouting(newRightLeaf);
        }

        private void updateLeaves(PeerInfo newNode) {
            int meDist = Helper.ringDistance(me.ID, newNode.ID);

            //if closer than left, replace left with new
            //if closer than right, replace right with new
            if (meDist < 0 && Helper.ringDistance(leftLeaf.ID, newNode.ID) >= 0)
                leftLeaf = newNode;
            else if (meDist >= 0 && Helper.ringDistance(rightLeaf.ID, newNode.ID) <= 0)
                rightLeaf = newNode;
            //assuming we don't need to check our leaf set against anything else
            //TODO handle file movements
        }

        private PeerInfo route(char ID) throws IOException {
            //if i don't have it but within leaf set range, forward to closest leaf (could be me)
            int meDist = Helper.ringDistance(me.ID, ID);
            int leftDist = Helper.ringDistance(leftLeaf.ID, ID);
            int rightDist = Helper.ringDistance(rightLeaf.ID, ID);
            if (meDist < 0 && leftDist >= 0) { // to the left and within leaf set
                if (-meDist < leftDist) {
                    return me;
                } else {
                    return leftLeaf;
                }
            } else if (meDist >= 0 && rightDist <= 0) { // to the right and within leaf set
                if (meDist < -rightDist) {
                    return me;
                } else {
                    return rightLeaf;
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
                for (PeerInfo rowPeer : routingTable[rowIdx]) {
                    if (rowPeer != null && rowPeer.ID != me.ID &&
                            Math.abs(Helper.ringDistance(rowPeer.ID, ID)) <
                                    Math.abs(Helper.ringDistance(bestPeer.ID, ID)))
                        bestPeer = rowPeer;
                }
            }
            return bestPeer;
        }

        private PeerInfo getClosestLeaf(char ID) {
            int leftDist = Math.abs(Helper.ringDistance(leftLeaf.ID, ID));
            int rightDist = Helper.ringDistance(rightLeaf.ID, ID);
            if (Math.abs(leftDist) < Math.abs(rightDist))
                return leftLeaf;
            else
                return rightLeaf;
        }
    }

    private static void notifyDiscoveryOfDeath() {
        try (
                Socket discoverySocket = new Socket(discoveryMachine, discoveryPort);
                DataOutputStream dout = new DataOutputStream(discoverySocket.getOutputStream())
        ) {
            dout.writeUTF("leave");
            dout.writeShort(me.ID);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void addToRouting(PeerInfo peer) {
        int rowIdx = Helper.getLongestCommonPrefixLength(me.ID, peer.ID);
        if (rowIdx == routingTable.length - 1) return;  //same ID
        int colIdx = Helper.getValueAtHexIdx(peer.ID, rowIdx + 1);
        routingTable[rowIdx][colIdx] = peer;
    }

    private static void readAndUpdateRouting(DataInputStream in) throws IOException {
        for (int i = 0; i < HpID; i++) {
            for (int j = 0; j < BpH*4; j++) {
                PeerInfo newPeer = new PeerInfo();
                newPeer.ID = in.readChar();
                newPeer.address = in.readUTF();
                if (!newPeer.address.equals("")) {
                    addToRouting(newPeer);
                }
            }
        }
    }

    private static void writeRouting(DataOutputStream out) throws IOException {
        for (PeerInfo[] row : routingTable) {
            for (PeerInfo cell : row) {
                if (cell == null) {
                    out.writeChar((char)0);
                    out.writeUTF("");
                } else {
                    out.writeChar(cell.ID);
                    out.writeUTF(cell.address);
                }
            }
        }
    }
}
