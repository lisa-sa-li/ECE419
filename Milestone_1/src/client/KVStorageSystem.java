// package client;

// import shared.messages.KVMessage;
// import org.apache.log4j.*;
// import java.net.ServerSocket;
// import java.net.Socket;
// import java.io.InputStream;
// import java.io.OutputStream;
// import java.io.IOException;
// import java.io.RandomAccessFile;
// import java.io.BufferedReader;  
// import java.io.FileReader;
// import java.io.StringBuffer;
// import shared.messages.JSONMessage;

// public class KVStorageSystem implements KVCommInterface {
// 	private static Logger logger = Logger.getRootLogger();
//     String fileName = "storage.txt";

// 	public KVStorageSystem() {
//         // BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
//         // writer.write(str);
//         // writer.close();
//         this.fileName = fileName;
// 	}

//     @Override
// 	public boolean put(String key, String value) throws Exception {
//         try {
//             // Below is slightly modified logic from https://stackoverflow.com/questions/20039980/java-replace-line-in-text-file
//             BufferedReader file = new BufferedReader(new FileReader(this.fileName));
//             StringBuffer inputBuffer = new StringBuffer();
//             String line;
//             String keyFromFile;
//             boolean foundKey = false;
//             JSONMessage json;

//             while ((line = file.readLine()) != null) {
//                 // Covert each line to a JSON so we can read the key and value
//                 json = new JSONMessage();
//                 json.deserialize(line);
//                 keyFromFile = json.getKey();

//                 // The key already exists in the file, update the old value with the new value
//                 if (keyFromFile.equals(key)) {
//                     foundKey = true;

//                     // If value == null, that means to delete so we will skip appending the line
//                     // Otherwise, update the value and append to file
//                     if (value != null) {
//                         json.setValue(value);
//                         line = json.serialize();
//                         inputBuffer.append(line);
//                         inputBuffer.append('\n');
//                     }
//                     break;
//                 } 
//             }

//             // If key does not exist in the file, add it to the end of the file
//             if (foundKey == false) {
//                 json = new JSONMessage();
//                 json.setMessage("", key, value); // We don't care about status here
//                 line = json.serialize();
//                 inputBuffer.append(line);
//                 inputBuffer.append('\n');
//             }

//             file.close();

//             // Overwrite file with the data
//             FileOutputStream fileOut = new FileOutputStream(this.fileName);
//             fileOut.write(inputBuffer.toString().getBytes());
//             fileOut.close();
            
//             logger.info("Completed 'put' operation into storage server");
//             return true;
//         } catch (Exception e) {
//             System.out.println("Problem reading file.");
//         }
//         return false;
// 	}

//     @Override
// 	public String get(String key) throws Exception {
//         try {
//             BufferedReader file = new BufferedReader(new FileReader(this.fileName));
//             JSONMessage json;
//             String line;
//             String keyFromFile;
//             boolean foundKey = false;

//             while ((line = file.readLine()) != null) {
//                 // Covert each line to a JSON so we can read the key and value
//                 json = new JSONMessage();
//                 json.deserialize(line);
//                 key = json.getKey();

//                 // The key exists in the file
//                 if (keyFromFile.equals(key)) {
//                     foundKey = true;
//                     break;
//                 } 
//             }
//             file.close();
//             logger.info("Completed 'get' operation into storage server");

//             if (foundKey == true) {
//                 return json.getValue();
//             }
//         } catch (Exception e) {
//             System.out.println("Problem reading file.");
//         }

//         // The key does not exist in the file
// 		return null;
// 	}
// }


//             // RandomAccessFile storageFile = new RandomAccessFile(this.fileName, "r");
//             // String jsonStr;
//             // String line;
//             // String[] arrOfLine;
//             // JSONMessage json;

//             // while (storageFile.readLine() != null) {
//             //     line = storageFile.readLine();
//             //     json = new JSONMessage();
//             //     // jsonStr = json.byteToString
//             //     // json.setMessage("", key,)
//             //     // arrOfLine = line.split(":", 2);
//             //     if (arrOfLine[0].equals(key)) {
                    
                    
//                         // public void setMessage(String inStatus, String inKey, String inValue){
//     //     setStatus(inStatus);
//     //     setKey(inKey);
//     //     setValue(inValue);
//     // }

//                 //     return arrOfLine[1];
//                 //     // pointer = storageFile.getFilePointer();
//                 //     // break;
//                 // }
