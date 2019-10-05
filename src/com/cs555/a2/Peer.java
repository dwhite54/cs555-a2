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
    private int discoveryPort;
    private PeerInfo[][] routingTable = new PeerInfo[Helper.IDSpaceBits / 4][16];

    public Peer(int discoveryPort, int peerPort) throws IOException {
        this.discoveryPort = discoveryPort;
        this.peerPort = peerPort;
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

    public void run() throws IOException {
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

    private static class SendHandler extends Thread {
        @Override
        public void run() {

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
