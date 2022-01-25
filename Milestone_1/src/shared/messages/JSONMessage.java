package shared.messages;

import java.util.StringTokenizer;

public class JSONMessage implements KVMessage{

    private StatusType status;
    private String key;
    private String value;

    public StatusType getStatus(){
        return status;
    }

    public void setStatus(String inStatus){
        // convert to enum
        StatusType enumStatus = StatusType.valueOf(inStatus);
        status = enumStatus;
    }

    public String getKey(){
        return key;
    }

    public void setKey(String inKey){
        key = inKey;
    }

    public String getValue(){
        return value;
    }

    public void setValue(String inValue){
        value = inValue;
    }

    public String serialize(){
        // initialize string builder to create mutable string
        StringBuilder strMessage = new StringBuilder();

        // Beginning chars
        strMessage.append("{");
        String statusEntry = String.join("\"status\":\"", status.name(), "\",");
        strMessage.append(statusEntry);
        String KVEntry = String.join("\"", key, "\":\"", value, "\",");
        strMessage.append(KVEntry);
        strMessage.append("}");

        return strMessage.toString();
    }

    public void deserialize(String json){
        StringTokenizer messageTokens = new StringTokenizer(json, "{}:,\"");

        while(messageTokens.hasMoreTokens()) {
            String k = messageTokens.nextToken();
            String status = messageTokens.nextToken();
            String key = messageTokens.nextToken(); 
            String value = messageTokens.nextToken();

            setStatus(status);
            setKey(key);
            setValue(value);
        }
    }

}
