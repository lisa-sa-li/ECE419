package app_kvClient;

import client.KVStore;

import logging.LogSetup;
import java.net.UnknownHostException;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.SocketException;

import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

public class KVClient implements IKVClient, Runnable {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private KVStore kvStore;
    private boolean stop = false;
    String hostname;
    int port;

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        if (this.kvStore != null) {
            throw new IOException("Connection is already established");
        }
        try {
            this.kvStore = new KVStore(hostname, port);
            this.kvStore.connect();
        } catch (IOException e) {
            this.kvStore = null;
            throw e;
        }
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public KVStore getStore() {
        return this.kvStore;
    }

    private void disconnect() {
        if (this.kvStore != null) {
            this.kvStore.disconnect();
            this.kvStore = null;
        }
    }

    private void printMsgFromServer(JSONMessage msg) {
        String key = msg.getKey();
        String value = msg.getValue();
        StatusType status = msg.getStatus();

        if (status == StatusType.SERVER_STOPPED) {
            System.out.println("The server is closed and can't be written to or read from.");
        } else if (status == StatusType.SERVER_WRITE_LOCK) {
            System.out.println("The server is locked for write. Only get is possible right now.");
        } else if (value == null || value.isEmpty()) {
            System.out.println(status + "\t key: " + key);
        } else {
            System.out.println(status + "\t key: " + key + " & value: " + value);
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.trim().split("\\s+");

        if (tokens[0].equals("-1")) {
            // Shorthand to connect (easter egg)
            if (tokens.length == 2) {
                cmdLine = "connect 127.0.0.1 " + tokens[1] + "\n";
            } else {
                cmdLine = "connect 127.0.0.1 8080\n";
            }
            tokens = cmdLine.trim().split("\\s+");
        }

        try {
            switch (tokens[0]) {
                case "quit":
                    stop = true;
                    disconnect();
                    System.out.println(PROMPT + "Application exit!");
                    logger.info("Application exit.");
                    break;
                case "connect":
                    if (tokens.length == 3) {
                        try {
                            String serverHostName = tokens[1];
                            int serverPort = Integer.parseInt(tokens[2]);
                            newConnection(serverHostName, serverPort);
                            logger.info("Connection established to " + serverHostName + ":" + tokens[2]);
                        } catch (NumberFormatException nfe) {
                            logger.error("No valid address. Port must be a number! ", nfe);
                        } catch (UnknownHostException e) {
                            logger.error("Unknown Host! ", e);
                        } catch (IOException e) {
                            logger.error("Something went wrong! ", e);
                        }
                    } else {
                        logger.error("Invalid number of parameters!");
                    }
                    break;
                case "put":
                    if (this.kvStore != null) {
                        String key = tokens[1];
                        if (key.length() <= 20 && key.length() > 0) { // Exact size of key bytes
                            if (tokens.length >= 3) {
                                // CREATE or UPDATE key,value
                                StringBuilder val = new StringBuilder();
                                for (int i = 2; i < tokens.length; i++) {
                                    val.append(tokens[i]);
                                    if (i != tokens.length - 1) {
                                        val.append(" ");
                                    }
                                }
                                String valStr = val.toString();
                                if (valStr.length() <= 120000) {
                                    try {
                                        JSONMessage msg = this.kvStore.put(tokens[1], valStr);
                                        printMsgFromServer(msg);
                                    } catch (SocketException se) {
                                        try {
                                            this.kvStore.connect();
                                            JSONMessage msg = this.kvStore.put(tokens[1], valStr);
                                            printMsgFromServer(msg);
                                        } catch (Exception e) {
                                            System.out.println("Socket connection to server is closed.");
                                        }
                                    } catch (Exception e) {
                                        System.out.println("Error putting key: " + e);
                                    }
                                } else {
                                    logger.error("Value length must be max 120000."); // Exact size of val bytes
                                }
                            } else if (tokens.length == 2) {
                                // DELETE key, value pair
                                try {
                                    JSONMessage msg = this.kvStore.put(tokens[1], "");
                                    printMsgFromServer(msg);
                                } catch (SocketException se) {
                                    try {
                                        this.kvStore.connect();
                                        JSONMessage msg = this.kvStore.put(tokens[1], "");
                                        printMsgFromServer(msg);
                                    } catch (Exception e) {
                                        System.out.println("Socket connection to server is closed.");
                                    }
                                } catch (Exception e) {
                                    System.out.println("Error deleting key: " + e);
                                }
                            } else {
                                logger.error("No message is passed!");
                            }
                        } else {
                            logger.error("Key length must be between 1 and 20.");
                        }
                    } else {
                        System.out.println("Not connected to any store.");
                        logger.warn("Not connected to any store.");
                    }
                    break;
                case "get":
                    if (tokens.length > 2) {
                        logger.error("Invalid number of parameters!");
                    } else {
                        if (this.kvStore != null) {
                            String key = tokens[1];
                            if (key.length() <= 20 && key.length() > 0) {
                                try {
                                    JSONMessage msg = this.kvStore.get(key);
                                    printMsgFromServer(msg);
                                } catch (SocketException se) {
                                    try {
                                        this.kvStore.connect();
                                        JSONMessage msg = this.kvStore.get(key);
                                        printMsgFromServer(msg);
                                    } catch (Exception e) {
                                        System.out.println("Socket connection to server is closed.");
                                    }
                                } catch (Exception e) {
                                    System.out.println("Error getting key: " + e);
                                }
                            } else {
                                logger.error("Key length must be between 1 and 20.");
                            }
                        } else {
                            System.out.println("Not connected to any store.");
                            logger.warn("Not connected to any store.");
                        }
                    }
                    break;
                case "recover":
                    if (tokens.length > 2) {
                        logger.error("Invalid number of parameters!");
                    } else {
                        if (this.kvStore != null) {
                            String key = tokens[1];
                            if (key.length() <= 20 && key.length() > 0) {
                                try {
                                    JSONMessage msg = this.kvStore.recover(key);
                                    printMsgFromServer(msg);
                                } catch (SocketException se) {
                                    try {
                                        this.kvStore.connect();
                                        JSONMessage msg = this.kvStore.recover(key);
                                        printMsgFromServer(msg);
                                    } catch (Exception e) {
                                        System.out.println("Socket connection to server is closed.");
                                    }
                                } catch (Exception e) {
                                    System.out.println("Error recovering key: " + e);
                                }
                            } else {
                                logger.error("Key length must be between 1 and 20.");
                            }
                        } else {
                            System.out.println("Not connected to any store.");
                            logger.warn("Not connected to any store.");
                        }
                    }
                    break;
                case "disconnect":
                    if (this.kvStore != null) {
                        disconnect();
                        System.out.println(PROMPT + "Disconnected.");
                        logger.info("Disconnected.");
                    } else {
                        System.out.println(PROMPT + "Nothing was connected.");
                        logger.warn("Nothing was connected.");
                    }
                    break;
                case "logLevel":
                    if (tokens.length == 2) {
                        String level = setLevel(tokens[1]);
                        if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                            logger.error("No valid log level!");
                            printPossibleLogLevels();
                        } else {
                            System.out.println(PROMPT + "Log level changed to level " + level);
                            logger.info("Log level changed to level " + level);
                        }
                    } else {
                        logger.error("Invalid number of parameters!");
                    }
                    break;
                case "help":
                    printHelp();
                    logger.info("Printed out help.");
                    break;
                default:
                    logger.error("Unknown command.");
                    printHelp();
                    break;
            }
        } catch (Exception e) {
            logger.error("Unkown Error: " + e.getMessage());
        }
    }

    private String setLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT + "Possible log levels are:");
        System.out.println(PROMPT + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(PROMPT).append("KVCLIENT HELP:\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t Establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t Disconnects from the server \n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t Inserts a key-value pair into the storage server data structures \n");
        sb.append("\t\t\t\t Overwrites the value with the input if the server already contains the specified key \n");
        sb.append(PROMPT).append("put <key>");
        sb.append("\t Deletes the entry for the given key if <value> is empty \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the storage server \n");
        sb.append(PROMPT).append("recover <key>");
        sb.append("\t\t Attemps to recover the key in the storage server \n");
        sb.append(PROMPT).append("logLevel <level>");
        sb.append("\t Changes the logLevel to the specified level \n");
        sb.append(PROMPT).append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t Exits the program \n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append(":::::::::::::::::::::::::::::::: \n");
        System.out.println(sb.toString());
    }

    public void run() {
        while (!stop) {
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated.");
                logger.error("CLI does not respond - Application terminated.");
            }
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient kvClient = new KVClient();
            kvClient.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
