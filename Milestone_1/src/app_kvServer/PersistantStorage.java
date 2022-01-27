package app_kvServer;

import shared.messages.KVMessage;
import org.apache.log4j.*;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.BufferedReader;  
import java.io.FileReader;
import java.lang.StringBuffer;
import java.io.FileOutputStream;  
import shared.messages.JSONMessage;
import shared.messages.TextMessage;
import java.io.File;

public class PersistantStorage implements IPersistantStorage {
	private static Logger logger = Logger.getRootLogger();
	private String fileName;
    private String pathToFile;
    private String dir = "./storage";

	public PersistantStorage(String name) throws Exception {
        this.fileName = name.trim() + "_storage.txt";
        this.pathToFile = dir + "/" + this.fileName;

        initFile();
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
            System.err.println(e);
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
            System.err.println(e);
        }

    }

	@Override
	public boolean put(String key, String value) throws Exception {
        try {
            // Below is slightly modified logic from https://stackoverflow.com/questions/20039980/java-replace-line-in-text-file
            BufferedReader file = new BufferedReader(new FileReader(this.pathToFile));
            StringBuffer inputBuffer = new StringBuffer();
            String line;
            String keyFromFile;
            boolean foundKey = false;
            JSONMessage json;

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the key and value
                json = new JSONMessage();
                json.deserialize(line);
                keyFromFile = json.getKey();

                // The key already exists in the file, update the old value with the new value
                if (keyFromFile.equals(key)) {
                    foundKey = true;

                    // If value == null, that means to delete so we will skip appending the line
                    // Otherwise, update the value and append to file
                    if (value != null) {
                        json.setValue(value);
                        line = json.serialize();
                        inputBuffer.append(line);
                        inputBuffer.append('\n');
                    }
                    break;
                } 
            }

            // If key does not exist in the file, add it to the end of the file
            if (foundKey == false) {
                json = new JSONMessage();
                json.setMessage("", key, value); // We don't care about status here
                line = json.serialize();
                inputBuffer.append(line);
                inputBuffer.append('\n');
            }

            file.close();

            // Overwrite file with the data
            FileOutputStream fileOut = new FileOutputStream(this.pathToFile);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();
            
            logger.info("Completed 'put' operation into storage server");
            return true;
        } catch (Exception e) {
            System.out.println("Problem reading file.");
        }
        return false;
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
            System.out.println("Problem reading file.");
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
        }
        else {
            logger.info("Failed to delete the file");
        }
	}
}