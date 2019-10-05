package com.cs555.a2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Peer {
    static class PeerInfo {
        public char ID;
        public InetAddress address;
    }

    private ServerSocket ss;
    private boolean shutdown = false;
    private int peerPort;
    private static String discoveryMachine;
    private static int discoveryPort;
    private static char ID;
    static boolean isJoined;
    private PeerInfo[][] routingTable = new PeerInfo[Helper.IDSpaceBits / 4][16];

    public Peer(String discoveryMachine, int discoveryPort, int peerPort) throws IOException {
        this(discoveryMachine, discoveryPort, peerPort, Helper.GenerateID());
    }

    public Peer(String discoveryMachine, int discoveryPort, int peerPort, char ID) throws IOException {
        this.discoveryMachine = discoveryMachine;
        this.discoveryPort = discoveryPort;
        this.peerPort = peerPort;
        this.ID = ID;
        ss = new ServerSocket(this.peerPort);
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
            processJoin(joinNode);
            notify();

            //then start a loop for listening to log requests, etc
        }

        private void processJoin(String joinNode) {
            //big fat TODO
        }

        private String getJoinNode() {
            String joinNode = null;
            try (
                    Socket s = new Socket(discoveryMachine, discoveryPort);
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream())
            ) {
                out.writeUTF("join");
                out.writeShort(ID);
                if (in.readBoolean()) {
                    joinNode = in.readUTF();
                    isJoined = true;
                } else { //collision detected
                    ID = Helper.GenerateID();
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
                switch (in.readUTF()) {
                    case "find":
                        handleFind();
                        break;
                    case "join":
                        handleJoin();
                        break;
                }
                this.in.close();
                this.out.close();
                this.s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void handleJoin() {
        }

        private void handleFind() {
        }
    }
}
