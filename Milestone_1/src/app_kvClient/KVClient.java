package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import shared.messages.JSONMessage;
import shared.messages.KVMessage;
import logger.LogSetup;
import java.net.UnknownHostException;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class KVClient implements IKVClient {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private KVCommInterface commInterfaceClient;
    private boolean stop = false;

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        // TODO Auto-generated method stub
        if (this.commInterfaceClient != null) {
            throw new IOException("Connection is already established");
        }
        try {
            this.commInterfaceClient = new KVStore(hostname, port);
            this.commInterfaceClient.connect();
        } catch (IOException e) {
            this.commInterfaceClient = null;
            throw e;
        }
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return this.commInterfaceClient;
    }

    private void disconnect() {
        if(this.commInterfaceClient != null) {
            this.commInterfaceClient.disconnect();
            this.commInterfaceClient = null;
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        try {
            switch(tokens[0]) {
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
                            System.out.println(PROMPT + "Connection established to " + serverHostName + ":" + tokens[2]);
                            logger.info("Connection established.");
                        } catch (NumberFormatException nfe) {
                            logger.error("No valid address. Port must be a number! ", nfe);
                        } catch (UnknownHostException e) {
                            logger.error("Unknown Host! ", e);
                        } catch (IOException e) {
                            logger.error(e);
                        }
                    } else {
                        // printError("Invalid number of parameters!");
                        logger.error("Invalid number of parameters!");
                    }
                    break;
                case "put":
                    if (this.commInterfaceClient != null) { // && this.commInterfaceClient.isRunning()) {
                        String key = tokens[1];
                        if (key.length() <= 20 && key.length() > 0) { // Exact size of key bytes idk
                            if (tokens.length >= 3) {
                                // CREATE or UPDATE key,value
                                JSONMessage json = new JSONMessage();
                                // load values into json format + get string
                                json.setMessage(tokens[0], tokens[1], tokens[2]);
                                String valStr = json.serialize();

                                if (valStr.length() <= 120000) {
                                    try {
                                        KVMessage msg = this.commInterfaceClient.put(tokens[1], valStr);
                                        // PLACE status message here
                                        logger.info(msg);
                                    } catch (Exception e) {
                                        logger.error(e);
                                    }
                                } else {
                                    logger.error("Value length must be max 120000."); // Exact size of val bytes idk
                                }
                            } else if (tokens.length == 2) {
                                // DELETE key,value pair
                                try {
                                    KVMessage msg = this.commInterfaceClient.put(tokens[1], "null");
                                    // PLACE status message here
                                    logger.info(msg);
                                } catch (Exception e) {
                                    logger.error(e);
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
                    if (this.commInterfaceClient != null) { // && this.commInterfaceClient.isRunning()) {
                        String key = tokens[1];
                        if (key.length() <= 20 && key.length() > 0) { // Exact size of key bytes idk
                            try {
                                KVMessage msg = this.commInterfaceClient.get(key);
                                // PLACE status message here
                                logger.info(msg);
                            } catch (Exception e) {
                                logger.error(e);
                            }
                        } else {
                            logger.error("Key length must be between 1 and 20.");
                        }
                    } else {
                        System.out.println("Not connected to any store.");
                        logger.warn("Not connected to any store.");
                    }
                    break;
                case "disconnect":
                    if (this.commInterfaceClient != null) {
                        disconnect();
                        System.out.println(PROMPT + "Disconnected.");
                        logger.info("Disconnected.");
                    } else {
                        System.out.println(PROMPT + "Nothing was connected.");
                        logger.warn("Nothing was connected.");
                    }
                    break;
                case "logLevel":
                    if(tokens.length == 2) {
                        String level = setLevel(tokens[1]);
                        if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
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
        } catch (Exception e){
            logger.error("Unkown Error: " + e.getMessage());
        }
    }

    private String setLevel(String levelString) {
        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " +  error);
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
        sb.append(PROMPT).append("put <key> null");
        sb.append("\t Deletes the entry for the given key if <value> equals null \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the storage server \n");
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
        while(!stop) {
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
            KVClient kv_client = new KVClient();
            kv_client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
