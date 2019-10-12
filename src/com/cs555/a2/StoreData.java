package com.cs555.a2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

class StoreData {
    private final String discoveryMachine;
    private final int discoveryPort;
    private final int peerPort;
    private static final String pre = "StoreData: ";

    StoreData(String discoveryMachine, int discoveryPort, int peerPort) {
        this.discoveryMachine = discoveryMachine;
        this.discoveryPort = discoveryPort;
        this.peerPort = peerPort;
    }

    private void print(String s) {
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
        String peerHost;
        try (
                Socket s = new Socket(discoveryMachine, discoveryPort);
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream())
        ) {
            out.writeUTF("get");
            if (in.readBoolean())
                peerHost = in.readUTF();
            else {
                print("Discovery has no peers");
                return false;
            }
        } catch (IOException e) {
            print("Error opening socket connection to discovery node " + discoveryMachine + ":" + discoveryPort);
            e.printStackTrace();
            return false;
        }

        while (true) {
            try (
                    Socket s = new Socket(peerHost, peerPort);
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream())
            ) {
                out.writeUTF("insert");
                out.writeChar(contentID);
                String newPeerHost = in.readUTF();
                if (newPeerHost.equals(peerHost)) {  // this node has the file
                    out.writeUTF(filename);
                    out.writeInt(contents.length);
                    out.write(contents);
                    break;
                } else {
                    peerHost = newPeerHost;
                }
            } catch (IOException e) {
                print("Error opening socket connection to peer " + peerHost + ":" + peerPort);
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    private void printHelp() {
        print("Available commands:");
        print("\t[quit,exit,bye]: Exit the program.");
        print("\twrite foo.bar: ");
        print("\tread foo.bar");
        print("\thelp: print this list.");
    }
}
