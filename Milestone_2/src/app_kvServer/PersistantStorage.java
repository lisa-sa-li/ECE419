package app_kvServer;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import org.apache.log4j.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.io.FileOutputStream;
import shared.messages.JSONMessage;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileNotFoundException;

import ecs.HashRing;

public class PersistantStorage implements IPersistantStorage {
    private static Logger logger = Logger.getRootLogger();
    private String fileName;
    private String pathToFile;
    private String dir = "./storage";
    private HashRing hashRing;

    public PersistantStorage(String name) {
        this.fileName = name.trim() + "_storage.txt";
        this.pathToFile = dir + "/" + this.fileName;
        try {
            initFile();
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void initFile() throws Exception {
        // Create directory if it does not exist
        File directory = new File(dir);
        try {
            if (directory.mkdirs()) {
                logger.info("Directory created: " + directory.getName());
            } else {
                logger.info("Directory already exists.");
            }
        } catch (Exception e) {
            logger.error("Error when creating directory " + directory.getName() + ": " + e);
        }
        // Create file if it does not exist
        File f = new File(this.pathToFile);
        try {
            if (f.createNewFile()) {
                logger.info("File created: " + f.getName());
            } else {
                logger.info("File already exists.");
            }
        } catch (Exception e) {
            logger.error("Error when creating file " + f.getName() + ": " + e);
        }

    }

    public BigInteger getHash(String value) {
        try {
            // get message bytes
            byte[] byteVal = value.getBytes("UTF-8");
            // create md5 instance
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            // convert value to md5 hash (returns bytes)
            byte[] mdDigest = md5.digest(byteVal);

            // convert to string
            StringBuilder stringHash = new StringBuilder();
            for (byte b : mdDigest) {
                // code below: modified code from
                // https://stackoverflow.com/questions/11380062/what-does-value-0xff-do-in-java
                stringHash.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
            }
            // return stringHash.toString();
            // return hex biginteger
            return new BigInteger(stringHash.toString(), 16);

        } catch (Exception e) {
            return new BigInteger("00000000000000000000000000000000");
        }
    }

    @Override
    public StatusType put(String key, String value) throws Exception {
        try {
            // Below is slightly modified logic from
            // https://stackoverflow.com/questions/20039980/java-replace-line-in-text-file
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            StringBuffer inputBuffer = new StringBuffer();
            String line;
            String keyFromFile;
            boolean foundKey = false;
            JSONMessage json;
            StatusType putStatus = StatusType.NO_STATUS;

            logger.debug("2key " + key);
            logger.debug("2value " + value);

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the key and value
                json = new JSONMessage();
                json.deserialize(line);
                keyFromFile = json.getKey();

                // The key exists in the file, update the old value with the new value
                if (keyFromFile.equals(key) && foundKey == false) {
                    foundKey = true;

                    // If value == "", that means to delete so we will skip appending the line
                    // Otherwise, update the value and append to file
                    if (value.isEmpty() || value == null) {
                        putStatus = StatusType.DELETE_SUCCESS;
                    } else {
                        json.setValue(value);
                        line = json.serialize(false);
                        logger.debug("LINE" + line);
                        inputBuffer.append(line);
                        inputBuffer.append('\n');
                        putStatus = StatusType.PUT_UPDATE;
                    }
                } else if (keyFromFile.equals(key) && foundKey == true) {
                    // This should never happen, but if there are more than 1 instances of a
                    // key in a file, remove the subsequent keys
                    continue;
                } else {
                    // If it's not the key-value we're looking for, copy the line over to the string
                    // buffer
                    inputBuffer.append(line);
                    inputBuffer.append('\n');
                }
            }

            // If key does not exist in the file
            // If delete: return DELETE_ERROR
            // If put: append to end of file and return PUT_SUCCESS
            if (foundKey == false) {
                if (value.isEmpty()) {
                    logger.info("Key does not exist and cannot 'delete'");
                    putStatus = StatusType.DELETE_ERROR;
                } else {
                    json = new JSONMessage();
                    json.setMessage("NO_STATUS", key, value); // We don't care about status here
                    line = json.serialize(false);
                    inputBuffer.append(line);
                    inputBuffer.append('\n');
                    putStatus = StatusType.PUT_SUCCESS;
                }
            }
            file.close();

            // Overwrite file with the string buffer data
            FileOutputStream fileOut = new FileOutputStream(this.pathToFile);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();

            logger.info("Completed 'put' operation into storage server");
            return putStatus;
        } catch (Exception e) {
            logger.error("Problem reading file to put.");
        }
        return StatusType.PUT_ERROR;
    }

    @Override
    public String get(String key) throws Exception {
        try {
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            JSONMessage json;
            String line;
            String keyFromFile;

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the key and value
                json = new JSONMessage();
                json.deserialize(line);
                keyFromFile = json.getKey();

                // The key exists in the file
                if (keyFromFile.equals(key)) {
                    file.close();
                    logger.info("Completed 'get' operation into storage server");
                    return json.getValue();
                }
            }
            file.close();
            logger.info("Completed 'get' operation into storage server");
        } catch (Exception e) {
            logger.error("Problem reading file to get.");
        }

        // The key does not exist in the file
        return null;
    }

    @Override
    public boolean inStorage(String key) throws Exception {
        String getValue = get(key);
        return getValue != null;
    }

    @Override
    public void deleteStorage() {
        File file = new File(this.pathToFile);

        if (file.delete()) {
            logger.info("File deleted successfully");
        } else {
            logger.info("Failed to delete the file");
        }
    }

    @Override
    public void clearStorage() {
        try {
            PrintWriter writer = new PrintWriter(this.pathToFile);
            writer.print("");
            writer.close();
        } catch (FileNotFoundException e) {
            logger.error("File not found. Cannot clear file.");
        } catch (Exception e) {
            logger.error("Error clearing storage.");
        }
    }

    private boolean isKeyInRange(BigInteger hash, BigInteger endHash, String key) {
        BigInteger keyHash = hashRing.getHash(key);
        int left = hash.compareTo(keyHash);
        int right = endHash.compareTo(keyHash);

        return (left >= 0 && right < 0);
    }

    @Override
    public String getDataInRange(BigInteger hash, BigInteger endHash) {
        try {
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            StringBuffer inputBuffer = new StringBuffer();
            StringBuffer outputBuffer = new StringBuffer();
            String line;
            String keyFromFile;
            boolean foundKey = false;
            JSONMessage json;
            StatusType putStatus = StatusType.NO_STATUS;

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the key and value
                json = new JSONMessage();
                json.deserialize(line);
                keyFromFile = json.getKey();

                if (isKeyInRange(hash, endHash, keyFromFile)) {
                    // We have to move this to a new server, write it to an output string buffer
                    outputBuffer.append(line);
                    outputBuffer.append('\n');
                } else {
                    // We keep this in the current server
                    inputBuffer.append(line);
                    inputBuffer.append('\n');
                }
            }

            file.close();

            // Overwrite file with the string buffer data
            FileOutputStream fileOut = new FileOutputStream(this.pathToFile);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();

            logger.info("Split server storage data");
            return outputBuffer.toString();
        } catch (Exception e) {
            logger.error("Problem reading file to put.");
        }
        return "";
    }

    @Override
    public StatusType appendToStorage(String keyValues) throws Exception {
        try {
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            StringBuffer inputBuffer = new StringBuffer();

            inputBuffer.append(keyValues);

            file.close();
            // Overwrite file with the string buffer data
            FileOutputStream fileOut = new FileOutputStream(this.pathToFile);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();

            logger.info("Completed 'put_many' operation into storage server");
            return StatusType.PUT_SUCCESS;
        } catch (Exception e) {
            logger.error("Problem reading file to put_many.");
        }
        return StatusType.PUT_ERROR;
    }

}