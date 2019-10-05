package com.cs555.a2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class DiscoveryNode {
    static class PeerInfo {
        public InetAddress address;
        public String name;
    }

    private ConcurrentHashMap<Character, PeerInfo> peers = new ConcurrentHashMap<>();

    private ServerSocket ss;
    private boolean shutdown = false;

    public DiscoveryNode(int discoveryPort) throws IOException {
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

    private void Close() throws IOException {
        ss.close();
        shutdown = true;
    }

    public void run() throws IOException {
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

                            int i = 0;
                            for (PeerInfo machine : peers.values()) {
                                if (i == Helper.rng.nextInt(peers.size()))
                                    out.writeUTF(machine.address.getHostName());
                                i++;
                            }

                            PeerInfo newPeer = new PeerInfo();
                            newPeer.address = s.getInetAddress();
                            newPeer.name = Helper.getNickname(ID);
                            peers.put(ID, newPeer);
                        }
                        break;
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

    private class InfoWriter extends Thread {
        @Override
        public void run() {
            while (!shutdown) {
                Scanner scanner = new Scanner(System.in);
                scanner.nextLine();
                System.out.println("Known peers are:");
                for (var entry : peers.entrySet()){
                    var val = entry.getValue();
                    String key = Integer.toHexString(entry.getKey());
                    System.out.println(val.name + "@" + val.address + " id: " + key);
                }
            }

        }
    }
}
