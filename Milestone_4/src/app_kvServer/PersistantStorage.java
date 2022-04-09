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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import shared.Utils;

public class PersistantStorage implements IPersistantStorage {
    private static Logger logger = Logger.getRootLogger();
    private String fileName;
    private String pathToFile;
    private String dir = "./storage";
    private String GLOBAL_STORAGE_PATH = dir + "/global_storage.txt";
    private String TRASH_PATH = dir + "/trash.txt";
    private Utils utils;

    public PersistantStorage(String name) {
        this.fileName = name.trim() + "_storage.txt";
        this.pathToFile = dir + "/" + this.fileName;
        this.utils = new Utils();

        try {
            initFile();
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public PersistantStorage(String name, String globalStorageFile) {
        // Use this for testing
        this.fileName = name.trim() + "_storage.txt";
        this.pathToFile = dir + "/" + this.fileName;
        this.utils = new Utils();

        GLOBAL_STORAGE_PATH = globalStorageFile;

        try {
            initFile();
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public PersistantStorage(boolean test) {
        // Use this for soft delete
        this.fileName = "trash.txt";
        this.pathToFile = dir + "/" + this.fileName;
        this.utils = new Utils();

        GLOBAL_STORAGE_PATH = dir + "/global_storage.txt";
    }

    private void initFile() throws Exception {
        // Create directory if it does not exist
        File directory = new File(dir);
        try {
            if (directory.mkdirs()) {
                logger.info("Directory created: " + directory.getName());
            } else {
                logger.info("Directory already exists: " + dir);
            }
        } catch (Exception e) {
            logger.error("Error when creating directory " + directory.getName() + ": " + e);
        }

        // Create storage file, global storage file, and trash file if it does not exist
        String[] filesToCreate = new String[] { this.pathToFile, GLOBAL_STORAGE_PATH, TRASH_PATH };
        File f;
        for (String filePath : filesToCreate) {
            f = new File(filePath);
            try {
                if (f.createNewFile()) {
                    logger.info("File created: " + f.getName());
                } else {
                    logger.info("File already exists: " + f.getName());
                }
            } catch (Exception e) {
                logger.error("Error when creating file " + f.getName() + ": " + e);
            }
        }
    }

    public BigInteger getHash(String value) {
        return utils.getHash(value);
    }

    public boolean clearTrash() {
        // get current time
        LocalDateTime currTime = LocalDateTime.now();

        // iterate through all keys in the file and remove expired ones <3
        try {
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            StringBuffer inputBuffer = new StringBuffer();
            String line;
            JSONMessage json;
            LocalDateTime ts;

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the timestamp
                json = new JSONMessage();
                json.deserialize(line);
                ts = json.getTimestamp();

                if (currTime.isAfter(ts)) {
                    // we keep
                    inputBuffer.append(line + '\n');
                }
            }
            file.close();

            // Overwrite file with the string buffer data
            FileOutputStream fileOut = new FileOutputStream(this.pathToFile);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();

            logger.info("Cleared trash.txt of expired kv-pairs");
            return true;

        } catch (Exception e) {
            logger.info("Error clearing trash.txt");
            return false;
        }
        return false;

        // turn string back into date

        // String str = "2016-03-04 11:30";
        // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd
        // HH:mm");
        // LocalDateTime dateTime = LocalDateTime.parse(str, formatter);

        // LocalDateTime now = LocalDateTime.now();
        // LocalDateTime pastDate = LocalDateTime.parse("2017-01-14T15:32:56.000");
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
                        // Add the file to trash, with an expiry of 1 minute
                        json.setTimestamp(LocalDateTime.now().plusMinutes(1));
                        this.appendToFile(json.serialize(false), TRASH_PATH);
                    } else {
                        json.setValue(value);
                        line = json.serialize(false);
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

            logger.info("Completed 'put' operation into storage server " + putStatus.name());
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

    public String recover(String key) throws Exception {
        try {
            BufferedReader file = new BufferedReader(new FileReader(TRASH_PATH));
            StringBuffer inputBuffer = new StringBuffer();
            String line;
            String keyFromFile;
            String recoveredValue = null;
            boolean foundKey = false;
            JSONMessage json;
            StatusType putStatus = StatusType.NO_STATUS;

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the key and value
                json = new JSONMessage();
                json.deserialize(line);
                keyFromFile = json.getKey();

                // The key exists in the file
                if (keyFromFile.equals(key) && foundKey == false) {
                    foundKey = true;
                    json.setTimestamp(null);
                    this.appendToStorage(json.serialize(false));
                    putStatus = StatusType.RECOVER_SUCCESS;
                    recoveredValue = json.getValue();
                } else if (keyFromFile.equals(key) && foundKey == true) {
                    // This should never happen, but if there are more than 1 instances of a
                    // key in a file, remove the subsequent keys
                    continue;
                } else {
                    // If it's not the key-value we're looking for, copy the line over to the buffer
                    inputBuffer.append(line + '\n');
                }
            }

            if (foundKey == false) {
                putStatus = StatusType.RECOVER_ERROR;
            }
            file.close();

            // Overwrite file with the string buffer data
            FileOutputStream fileOut = new FileOutputStream(TRASH_PATH);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();

            logger.info("Completed 'recover' operation into storage server " + putStatus.name());
            return recoveredValue;
        } catch (Exception e) {
            logger.error("Problem reading trash.txt to recover.");
        }
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

    public boolean isEmpty() {
        File file = new File(this.pathToFile);
        return file.length() == 0;
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

    @Override
    public String getDataInRange(BigInteger hash, BigInteger endHash, Boolean die) {
        // Removes kv-pairs from the storage where the key falls within hash:endHash
        // Returns those removed kv-pairs as a string
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

                if (die == true) {
                    // This indicates the server has been removed from the hashring
                    // We need to remove everything from its storage to send to another server
                    outputBuffer.append(line);
                    outputBuffer.append('\n');
                } else if (utils.isKeyInRange(hash, endHash, keyFromFile)) {
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

            return outputBuffer.toString();
        } catch (Exception e) {
            logger.error("Problem reading file to put.");
        }
        return "";
    }

    @Override
    public StatusType appendToStorage(String keyValues) {
        return appendToFile(keyValues, this.pathToFile);
    }

    public StatusType appendToFile(String keyValues, String filePath) {
        // Appends kv-pairs to the end of the storage file
        try {
            BufferedReader file = new BufferedReader(new FileReader(filePath));
            StringBuffer inputBuffer = new StringBuffer();
            String line;

            while ((line = file.readLine()) != null) {
                inputBuffer.append(line + '\n');
            }
            inputBuffer.append(keyValues);
            file.close();
            // Overwrite file with the string buffer data
            FileOutputStream fileOut = new FileOutputStream(filePath);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();

            if (filePath.equals(this.pathToFile)) {
                logger.info("Completed 'put_many' operation into storage server");
            } else {
                logger.info("Appended kv-pair(s) to file " + filePath);
            }

            return StatusType.PUT_SUCCESS;
        } catch (Exception e) {
            logger.error("Problem reading file to put_many.");
        }
        return StatusType.PUT_ERROR;
    }

    public String getAllFromStorage() {
        // Appends kv-pairs to the end of the storage file
        try {
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            StringBuffer buffer = new StringBuffer();
            String line;

            while ((line = file.readLine()) != null) {
                buffer.append(line);
                buffer.append('\n');
            }

            file.close();
            logger.info("Retrieved all key-value pairs from storage");
            return buffer.toString();
        } catch (Exception e) {
            logger.error("Problem retrieving all key-value pairs from storage.");
        }
        return "";
    }

    public void moveToGlobalStorage() {
        // Appends kv-pairs to global_storage.txt if it's being shut down and is the
        // last server
        try {
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            StringBuffer buffer = new StringBuffer();
            String line;

            while ((line = file.readLine()) != null) {
                buffer.append(line);
                buffer.append('\n');
            }
            file.close();
            this.clearStorage();
            // Write all its contents to a global storage
            FileOutputStream fileOut = new FileOutputStream(GLOBAL_STORAGE_PATH);
            fileOut.write(buffer.toString().getBytes());
            fileOut.close();
            logger.info("Successfully moved kv-pairs to " + GLOBAL_STORAGE_PATH);
        } catch (Exception e) {
            logger.error("Problem moving kv-pairs to " + GLOBAL_STORAGE_PATH);
        }
    }

    public void getFromGlobalStorage() {
        // Retrieve kv-pairs from global_storage.txt if it's the first server being
        // started up
        try {
            BufferedReader file = new BufferedReader(new FileReader(GLOBAL_STORAGE_PATH));
            StringBuffer buffer = new StringBuffer();
            String line;

            while ((line = file.readLine()) != null) {
                buffer.append(line);
                buffer.append('\n');
            }
            file.close();

            // Clear the file
            PrintWriter writer = new PrintWriter(GLOBAL_STORAGE_PATH);
            writer.print("");
            writer.close();

            // Append the kv-pairs to its own storage
            this.appendToStorage(buffer.toString());
            logger.info("Successfully retrieved kv-pairs from " + GLOBAL_STORAGE_PATH);
        } catch (Exception e) {
            logger.error("Problem retrieving kv-pairs from " + GLOBAL_STORAGE_PATH);
        }
    }

}