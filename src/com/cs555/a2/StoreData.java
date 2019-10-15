package com.cs555.a2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Scanner;

class StoreData {
    private final String discoveryMachine;
    private final int discoveryPort;
    private static final String pre = "StoreData: ";

    StoreData(String discoveryMachine, int discoveryPort) {
        this.discoveryMachine = discoveryMachine;
        this.discoveryPort = discoveryPort;
    }

    private static void print(String s) {
        System.out.println(pre+s);
    }

    void run() throws NoSuchAlgorithmException {
        Scanner scanner = new Scanner(System.in);
        String input;
        boolean exit = false;
        boolean result = false;
        print("Type a command and hit return. To see available commands, type 'help'");
        while (!exit) {
            print(">");
            input = scanner.nextLine();
            String[] inputs = input.split(" ");
            switch (inputs[0]) {
                case "quit":
                case "exit":
                case "bye":
                    exit = true;
                    break;
                case "read":
                    print("Not implemented");
                    break;
                case "write":
                    if (inputs.length == 2) {
                        result = processWrite(inputs[1]);
                        break;
                    }
                default:
                    print("Invalid verb");
                case "help":
                    printHelp();
                    break;
            }

            if (result) {
                print("Success.");
            } else {
                print("Failure.");
            }
        }
    }

    private boolean processWrite(String filename) throws NoSuchAlgorithmException {
        byte[] contents;
        try (FileInputStream fileInputStream = new FileInputStream(filename)) {
            contents = fileInputStream.readAllBytes();
        } catch (IOException e) {
            print("Error reading file from disk");
            return false;
        }
        char contentID = Helper.getDigest(filename.getBytes());
        char peerID;
        String peerHost;
        print("File digest: " + Integer.toHexString(contentID));
        try (
                Socket s = new Socket(discoveryMachine, discoveryPort);
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream())
        ) {
            out.writeUTF("get");
            if (in.readBoolean()) {
                peerID = in.readChar();
                peerHost = in.readUTF() + ":" + in.readInt();
                print("Connecting to random peer " + peerHost);
            }
            else {
                print("Discovery has no peers");
                return false;
            }
        } catch (IOException e) {
            print("Error opening socket connection to discovery node " + discoveryMachine + ":" + discoveryPort);
            e.printStackTrace();
            return false;
        }

        ArrayList<Character> joinIDs = new ArrayList<>();
        joinIDs.add(peerID);
        ArrayList<String> joinAddresses = new ArrayList<>();
        joinAddresses.add(peerHost);
        int i = 0;
        while (true) {
            String[] peerAddress = peerHost.split(":");
            try (
                    Socket s = new Socket(peerAddress[0], Integer.parseInt(peerAddress[1]));
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream())
            ) {
                out.writeUTF("insert");
                out.writeChar(contentID);
                out.writeInt(++i);
                String newPeerHost = in.readUTF() + ":" + in.readInt();
                if (newPeerHost.equals(peerHost)) {  // this node has the file
                    out.writeUTF(filename);
                    out.writeInt(contents.length);
                    out.write(contents);
                    break;
                } else {
                    joinIDs.add(in.readChar());
                    peerHost = newPeerHost;
                    joinAddresses.add(peerHost);
                }
            } catch (IOException e) {
                print("Error opening socket connection to peer " + peerHost);
                e.printStackTrace();
                return false;
            }
        }
        printTrace(joinIDs, joinAddresses);
        return true;
    }

    private static void printTrace(ArrayList<Character> joinIDs, ArrayList<String> joinNodes) {
        if (joinIDs.size() != joinNodes.size()) {
            print("Bad call of printTrace");
        }
        for (int i = 0; i < joinIDs.size(); i++)
            print("Trace: " + Integer.toHexString(joinIDs.get(i)) + "@" + joinNodes.get(i));
    }

    private void printHelp() {
        print("Available commands:");
        print("\t[quit,exit,bye]: Exit the program.");
        print("\twrite foo.bar: ");
        print("\tread foo.bar");
        print("\thelp: print this list.");
    }
}
