package com.cs555.a2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import static com.cs555.a2.Helper.HpID;
import static com.cs555.a2.Helper.BpH;

public class Peer {
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
    private static ConcurrentHashMap<Character, PeerInfo> leaves = new ConcurrentHashMap<>();

    public Peer(String discoveryMachine, int discoveryPort, int peerPort) throws IOException {
        this(discoveryMachine, discoveryPort, peerPort, Helper.GenerateID());
    }

    public Peer(String discoveryMachine, int discoveryPort, int peerPort, char ID) throws IOException {
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

    private void Close() throws IOException {
        ss.close();
        shutdown = true;
    }

    public void run() throws IOException, InterruptedException {
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
            int i = 0; //row of routing; ith peer we've contacted to get routing info
            do {
                int myColIdx = getValueAtHexIdx(me.ID, i);
                try (
                        Socket s = new Socket(joinNode, peerPort);
                        DataInputStream in = new DataInputStream(s.getInputStream());
                        DataOutputStream out = new DataOutputStream(s.getOutputStream());
                ) {
                    int p = 0;
                    do {
                        out.writeUTF("join");
                        out.writeChar(me.ID);
                        out.writeInt(i); // row index we want from the peer
                        if (in.readBoolean()) { //accepting joins
                            p = in.readInt(); // length of prefix match
                            for (int j = 0; j < HpID; j++) {
                                char newID = in.readChar();
                                String newAddress = in.readUTF();
                                if (j == myColIdx) {
                                    routingTable[i][j] = me;
                                } else {
                                    PeerInfo newPeer = new PeerInfo();
                                    newPeer.ID = newID;
                                    newPeer.address = newAddress;
                                    routingTable[i][j] = newPeer;
                                }
                            }
                            joinNode = in.readUTF();
                        }
                        i++;
                    } while (i < p);
                } catch(IOException e){
                    e.printStackTrace();
                    dumpStack();
                }
            } while (i < HpID); //TODO

            notify();

            //TODO start a loop for listening to log requests, etc
        }

        private void processJoin(String joinNode) {
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
                String host = s.getInetAddress().getHostName();
                String cmd = in.readUTF();
                char ID = in.readChar();
                switch (cmd) {
                    case "find":
                        //check if it's in our data
                        if (files.containsKey(ID)) {
                            out.writeBoolean(true); // I have the file
                            out.write(new FileInputStream(files.get(ID)).readAllBytes());
                        } else {
                            out.writeBoolean(false); // I will route you to the file
                            //if leaf set best, route to one of them
                            if (leaves.containsKey(ID)) {
                                out.writeUTF(leaves.get(ID).address);
                            } else {
                                //route with routing table
                                int rowIdx = getLongestCommonPrefixLength(ID, me.ID);
                                int colIdx;
                                if (rowIdx < 0) { // no match found--illegal routing table
                                    out.writeUTF(me.address); // this will indicate internal error
                                    System.out.println("Illegal routing table, terminating.");
                                    dumpStack();
                                    return;
                                } else if (rowIdx + 1 >= HpID) { // complete match
                                    colIdx = getValueAtHexIdx(ID, rowIdx);
                                }
                                colIdx = getValueAtHexIdx(ID, rowIdx + 1);
                                PeerInfo bestPeer = routingTable[rowIdx][colIdx];
                                if (bestPeer == null) {
                                    //need to find a peer closer than us
                                    //search leaf set and current row
                                    bestPeer = new PeerInfo();
                                    bestPeer.ID = me.ID;
                                    for (char leafID : leaves.keySet()) {
                                        if (Math.abs(leafID - ID) < Math.abs(bestPeer.ID - ID))
                                            bestPeer = leaves.get(leafID);
                                    }
                                    for (PeerInfo rowPeer : routingTable[rowIdx]) {
                                        if (rowPeer != null && rowPeer.ID != me.ID &&
                                                Math.abs(rowPeer.ID - ID) < Math.abs(bestPeer.ID - ID))
                                            bestPeer = rowPeer;
                                    }
                                }
                                out.writeUTF(bestPeer.address);
                            }
                        }

                        break;
                    case "join":
                        break;
                }
                this.in.close();
                this.out.close();
                this.s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //length of longest common prefix
    //returns -1 if no match
    private static int getLongestCommonPrefixLength(char ID1, char ID2) {
        int mask = 0;
        int i = 0;
        while (i < HpID) {
            int shift = (HpID - i - 1) * BpH;
            mask += (0xf << shift);
            if ((ID1 & mask) != (ID2 & mask))
                return i - 1;
            i++;
        }
        return i;
    }

    //return hex/integer value at ith position in hex string of ID
    private static int getValueAtHexIdx(char ID, int i) {
        if (i >= Helper.HpID - 1) throw new IndexOutOfBoundsException();
        int shift = (Helper.HpID - i - 1) * Helper.BpH;
        int mask = 0xf << shift;
        return (mask & ID) >> shift;
    }


}
