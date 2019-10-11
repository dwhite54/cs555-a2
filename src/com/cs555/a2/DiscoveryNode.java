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

    private ConcurrentHashMap<Character, PeerInfo> peers = new ConcurrentHashMap<>();

    private ServerSocket ss;
    private boolean shutdown = false;
    private static final String pre = "Discovery: ";
    private static int peerPort;

    DiscoveryNode(int discoveryPort, int peerPort) throws IOException {
        ss = new ServerSocket(discoveryPort);
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

    private void print(String s) {
        System.out.println(pre+s);
    }

    private void Close() throws IOException {
        ss.close();
        shutdown = true;
    }

    void run() throws IOException {
        while (!shutdown)
        {
            Socket s = null;
            try
            {
                s = ss.accept();
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                String cmd = in.readUTF();
                char ID = in.readChar();
                switch(cmd) {
                    case "join":
                        if (peers.containsKey(ID))
                            out.writeBoolean(false); // collision detected
                        else {
                            out.writeBoolean(true);
                            out.writeUTF(getRandomPeer());

                            PeerInfo newPeer = new PeerInfo();
                            newPeer.address = s.getInetAddress();
                            newPeer.name = Helper.getNickname(ID);
                            print("Peer joined: " + newPeer.address + ":" + peerPort + ", nickname: " + newPeer.name);
                            peers.put(ID, newPeer);
                        }
                        break;
                    case "get":
                        if (peers.isEmpty())
                            out.writeBoolean(false);
                        else {
                            out.writeBoolean(true);
                            out.writeUTF(getRandomPeer());
                        }
                    case "leave":
                        peers.remove(ID);
                        break;
                }
            }
            catch (Exception e){
                if (s != null){
                    s.close();
                }
                e.printStackTrace();
            }
        }
    }

    private String getRandomPeer(){
        int i = 0;
        int randInt = Helper.rng.nextInt(peers.size());
        for (PeerInfo machine : peers.values()) {
            if (i == randInt)
                return machine.address.getHostName();
            i++;
        }
        return "";
    }

    private class InfoWriter extends Thread {
        @Override
        public void run() {
            while (!shutdown) {
                Scanner scanner = new Scanner(System.in);
                scanner.nextLine();
                print("Known peers are:");
                for (var entry : peers.entrySet()){
                    var val = entry.getValue();
                    String key = Integer.toHexString(entry.getKey());
                    print(val.name + "@" + val.address + " id: " + key);
                }
            }

        }
    }
}
