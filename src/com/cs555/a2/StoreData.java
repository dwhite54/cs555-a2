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

    private boolean processWrite(String fileName) throws NoSuchAlgorithmException {
        byte[] contents;
        try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
            contents = fileInputStream.readAllBytes();
        } catch (IOException e) {
            print("Error reading file from disk");
            return false;
        }
        char contentID = Helper.getDigest(contents);
        String peerHost;
        try (
                Socket discoverySocket = new Socket(discoveryMachine, discoveryPort);
                DataInputStream din = new DataInputStream(discoverySocket.getInputStream());
                DataOutputStream dout = new DataOutputStream(discoverySocket.getOutputStream())
        ) {
            dout.writeUTF("get");
            if (din.readBoolean())
                peerHost = din.readUTF();
            else {
                print("Discovery has no peers");
                return false;
            }
        } catch (IOException e) {
            print("Error opening socket connection to discovery node " + discoveryMachine + ":" + discoveryPort);
            e.printStackTrace();
            return false;
        }
        boolean found = false;
        while (!found) {
            try (
                    Socket peerSocket = new Socket(peerHost, peerPort);
                    DataInputStream pin = new DataInputStream(peerSocket.getInputStream());
                    DataOutputStream pout = new DataOutputStream(peerSocket.getOutputStream())
            ) {
                pout.writeUTF("insert");
                pout.writeChar(contentID);
                if (pin.readBoolean()) {  // this node has the file, so update it
                    pout.write(contents);
                    found = true;
                } else {
                    String newPeerHost = pin.readUTF();
                    if (peerHost.equals("") || peerHost.equals(newPeerHost)) {
                        print("internal peer routing error");
                        return false;
                    }
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
