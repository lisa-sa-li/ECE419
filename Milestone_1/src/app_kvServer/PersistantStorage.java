package app_kvServer;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import org.apache.log4j.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.io.FileOutputStream;
import shared.messages.JSONMessage;
import java.io.File;

public class PersistantStorage implements IPersistantStorage {
    private static Logger logger = Logger.getRootLogger();
    private String fileName;
    private String pathToFile;
    private String dir = "./storage";

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

                // The key already exists in the file, update the old value with the new value
                if (keyFromFile.equals(key) && foundKey == false) {
                    foundKey = true;

                    // If value == "", that means to delete so we will skip appending the line
                    // Otherwise, update the value and append to file
                    if (value.trim().equals("") || value == null || value.trim().equals("null")) {
                        putStatus = StatusType.DELETE_SUCCESS;
                    } else {
                        json.setValue(value);
                        line = json.serialize();
                        inputBuffer.append(line);
                        inputBuffer.append('\n');
                        putStatus = StatusType.PUT_UPDATE;
                    }
                } else if (keyFromFile.equals(key) && foundKey == true) {
                    // This should never happen, but if there are more than 1 instances of a
                    // key in a file, remove the subsequent keys
                    continue;
                } else {
                    inputBuffer.append(line);
                    inputBuffer.append('\n');
                }
            }

            // If key does not exist in the file
            // If delete: return DELETE_ERROR
            // If put: append to end of file and return PUT_SUCCESS
            if (foundKey == false) {
                if (value.equals("")) {
                    logger.info("Key does not exist and cannot 'delete'");
                    putStatus = StatusType.DELETE_ERROR;
                } else {
                    json = new JSONMessage();
                    json.setMessage("NO_STATUS", key, value); // We don't care about status here
                    line = json.serialize();
                    inputBuffer.append(line);
                    inputBuffer.append('\n');
                    putStatus = StatusType.PUT_SUCCESS;
                }
            }
            file.close();

            // Overwrite file with the data
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
    public void clearStorage() {
        File file = new File(this.pathToFile);

        if (file.delete()) {
            logger.info("File deleted successfully");
        } else {
            logger.info("Failed to delete the file");
        }
    }
}