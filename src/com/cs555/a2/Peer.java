package com.cs555.a2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
    private static boolean isJoined;
    private static PeerInfo me = new PeerInfo();
    private static PeerInfo[][] routingTable = new PeerInfo[Helper.BpID / 4][16];
    private static ConcurrentHashMap<Character, String> files = new ConcurrentHashMap<>();
    private static ConcurrentSkipListMap<Character, PeerInfo> leaves = new ConcurrentSkipListMap<>();
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
        int i = 0;
        for (PeerInfo[] row : routingTable) {
            print("ROW " + i);
            for (PeerInfo cell : row) {
                print(Integer.toHexString(cell.ID) + "@" + cell.address + ":" + peerPort);
            }
        }
    }

    private void Close() throws IOException {
        ss.close();
        shutdown = true;
    }

    void run() throws IOException, InterruptedException {
        // running infinite loop for getting
        // client request
        Thread mT = new Peer.SendHandler();
        mT.start();
        mT.wait(); //wait for join to complete
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

    private static class SendHandler extends Thread {
        @Override
        public void run() {
            String joinNode = getJoinNode();
            joinSelf(joinNode);
            notify();

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
                            print("table");
                            break;
                        case "showfiles":
                            print("files");
                            break;
                        default:
                            System.out.println("Invalid verb");
                        case "help":
                            printHelp();
                            break;
                    }
                }
            }
        }

        private void printHelp() {
            System.out.println("Available commands:");
            System.out.println("\t[quit,exit,bye]: Exit the program.");
            System.out.println("\tshowtable: print routing table");
            System.out.println("\tshowfiles: print filenames stored here");
            System.out.println("\thelp: print this list.");
        }

        private void joinSelf(String joinNode) {
            for (int i = 0; i < routingTable.length;) { // increment in inner for
                int myColIdx = Helper.getValueAtHexIdx(me.ID, i);
                try (
                        Socket s = new Socket(joinNode, peerPort);
                        DataInputStream in = new DataInputStream(s.getInputStream());
                        DataOutputStream out = new DataOutputStream(s.getOutputStream())
                ) {
                    int p = 0;
                    out.writeUTF("join");
                    out.writeChar(me.ID);
                    out.writeInt(i); // row index we want from the peer
                    p = in.readInt(); // length of prefix match
                    for (; i < p; i++) {
                        for (int j = 0; j < HpID; j++) {
                            char newID = in.readChar();
                            String newAddress = in.readUTF();
                            if (j == myColIdx) {
                                routingTable[i][j] = me;
                            } else if (newAddress.equals("")) {
                                routingTable[i][j] = null;
                            } else {
                                PeerInfo newPeer = new PeerInfo();
                                newPeer.ID = newID;
                                newPeer.address = newAddress;
                                routingTable[i][j] = newPeer;
                            }
                        }
                        i++;
                    }
                    joinNode = in.readUTF();
                } catch(IOException e){
                    e.printStackTrace();
                    dumpStack();
                }
            }
        }

        private String getJoinNode() {
            String joinNode = null;
            try (
                    Socket s = new Socket(discoveryMachine, discoveryPort);
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream())
            ) {
                out.writeUTF("join");
                out.writeShort(me.ID);
                if (in.readBoolean()) {
                    joinNode = in.readUTF();
                } else { //collision detected
                    me.ID = Helper.GenerateID();
                    getJoinNode();
                }
            } catch (IOException e) {
                print("Failed getting join node from discovery.");
                e.printStackTrace();
                dumpStack();
            }
            return joinNode;
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
                        //check if it's in our data
                        if (files.containsKey(ID)) {
                            out.writeBoolean(true); // I have the file
                            out.write(new FileInputStream(files.get(ID)).readAllBytes());
                        } else {
                            out.writeBoolean(false); // I will route you to the file
                            out.writeUTF(route(ID));
                        }

                        break;
                    case "join":
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
                        out.writeUTF(route(ID));
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

        private String route(char ID) throws IOException {
            //if i don't have it but within leaf set range, forward to closest leaf
            if (Math.abs(ID - me.ID) < Integer.max(
                    Math.abs(leaves.firstKey() - me.ID), Math.abs(leaves.lastKey() - me.ID))) {
                PeerInfo closestLeaf = getClosestLeaf(ID);
                return closestLeaf.address;
            } else { //finally we give it to the routing table
                return routeWithTable(ID);
            }
        }

        private String routeWithTable(char ID) throws IOException {
            //route with routing table
            int rowIdx = Helper.getLongestCommonPrefixLength(ID, me.ID);
            int colIdx;
            if (rowIdx < 0) { // no match found--illegal routing table
                out.writeUTF(""); // this will indicate internal error
                throw new IOException("Illegal routing table.");
            } else if (rowIdx + 1 >= HpID) { // complete match
                colIdx = Helper.getValueAtHexIdx(ID, rowIdx);
            } else {
                colIdx = Helper.getValueAtHexIdx(ID, rowIdx + 1);
            }
            PeerInfo bestPeer = routingTable[rowIdx][colIdx];
            if (bestPeer == null) {
                //need to find a peer closer than us
                //search leaf set and current row
                bestPeer = getClosestLeaf(ID);
                for (PeerInfo rowPeer : routingTable[rowIdx]) {
                    if (rowPeer != null && rowPeer.ID != me.ID &&
                            Math.abs(rowPeer.ID - ID) < Math.abs(bestPeer.ID - ID))
                        bestPeer = rowPeer;
                }
            }
            return bestPeer.address;
        }

        private PeerInfo getClosestLeaf(char ID) {
            PeerInfo bestPeer;
            bestPeer = new PeerInfo();
            bestPeer.ID = leaves.firstKey();
            for (char leafID : leaves.keySet()) {
                if (Math.abs(leafID - ID) < Math.abs(bestPeer.ID - ID))
                    bestPeer = leaves.get(leafID);
            }
            return bestPeer;
        }
    }
}
