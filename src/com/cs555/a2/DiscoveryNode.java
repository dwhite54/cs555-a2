package com.cs555.a2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

class DiscoveryNode {
    static class PeerInfo {
        InetAddress address;
        String name;
    }

    private static final ConcurrentHashMap<Character, PeerInfo> peers = new ConcurrentHashMap<>();

    private static ServerSocket ss;
    private static boolean shutdown = false;
    private static final String pre = "Discovery: ";
    private static int peerPort;

    DiscoveryNode(int discoveryPort, int peerPort) throws IOException {
        ss = new ServerSocket(discoveryPort);
        peerPort = peerPort;
        final Thread mT = new InfoWriter();
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Close();
                mainThread.join(1000);
                if (mainThread.isAlive())
                    mainThread.interrupt();
                mT.join(1000);
                if (mT.isAlive())
                    mT.interrupt();

            } catch (InterruptedException | IOException e) {
                System.exit(-1);
            }
        }));

        mT.start();
    }

    private static class InfoWriter extends Thread {
        @Override
        public void run() {
            while (!shutdown) {
                Scanner scanner = new Scanner(System.in);
                scanner.nextLine();
                print("Known peers are:");
                for (var entry : peers.entrySet()){
                    var val = entry.getValue();
                    String ID = Integer.toHexString(entry.getKey());
                    print(String.format("%s@%s:%d id: %s", val.name, val.address, peerPort, ID));
                }
            }
        }
    }

    private static void print(String s) {
        System.out.println(pre+s);
    }

    private void Close() throws IOException {
        ss.close();
        shutdown = true;
    }

    void run() throws IOException {
        // running infinite loop for getting
        // client request
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
        public void run() {
            try {
                String cmd = in.readUTF();
                char randID;
                switch (cmd) {
                    case "join":
                        synchronized (peers) {
                            char ID = in.readChar();
                            while (peers.containsKey(ID)) {
                                print("Collision detected");
                                out.writeBoolean(true);
                                ID = in.readChar();
                            }
                            out.writeBoolean(false);
                            randID = getRandomPeer();
                            out.writeChar(randID);
                            out.writeUTF(peers.get(randID).address.getHostName());
                            print("Dispatched random peer, waiting for join completion...");
                            PeerInfo newPeer = new PeerInfo();
                            newPeer.address = s.getInetAddress();
                            newPeer.name = Helper.getNickname(ID);
                            if (in.readBoolean()) {
                                print(String.format("Peer joined: %s %s:%d, nickname: %s",
                                        Integer.toHexString(ID), newPeer.address, peerPort, newPeer.name));
                                peers.put(ID, newPeer);
                            }
                        }
                        break;
                    case "get":
                        if (peers.isEmpty())
                            out.writeBoolean(false);
                        else {
                            out.writeBoolean(true);
                            randID = getRandomPeer();
                            out.writeChar(randID);
                            out.writeUTF(peers.get(randID).address.getHostName());
                        }
                    case "leave":
                        synchronized (peers) {
                            char leaveID = in.readChar();
                            PeerInfo leavingPeer = peers.get(leaveID);
                            if (leavingPeer == null)
                                print("Removing peer which is not present " + Integer.toHexString(leaveID));
                            else {
                                print(String.format("Peer left: %s %s:%d, nickname: %s",
                                        Integer.toHexString(leaveID), leavingPeer.address, peerPort, leavingPeer.name));
                                peers.remove(leaveID);
                            }
                        }
                        break;
                }
            } catch (IOException e) {
                e.printStackTrace();
                dumpStack();
            }
        }
    }

    private static char getRandomPeer(){
        int i = 0;
        if (!peers.isEmpty()) {
            int randInt = Helper.rng.nextInt(peers.size());
            for (char ID : peers.keySet()) {
                if (i == randInt)
                    return ID;
                i++;
            }
        }
        return 0;
    }
}
