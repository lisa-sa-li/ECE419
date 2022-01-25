package shared.messages;

public class Serialize implements Runnable {

    public void run() {
        // TODO insert run code?
    }

    private static StringBuilder messageToJSON(KVMessage message){
        // initialize string builder to create mutable string
        StringBuilder strMessage = new StringBuilder();

        // break down KV message into string
        // KV message = {method, key, value}
        // Beginning chars
        strMessage.append("{");
        String methodEntry = String.join("\"method\":", message.getStatus(), ",");
        strMessage.append(methodEntry);
        String keyEntry = String.join("\"key\":", message.getKey(), ",");
        strMessage.append(keyEntry);
        String valueEntry = String.join("\"method\":", message.getValue());
        strMessage.append(valueEntry);
        strMessage.append("}");

        return strMessage;

    }
    
}
