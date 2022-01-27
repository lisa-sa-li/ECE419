package shared.messages;

import java.io.Serializable;
import java.util.StringTokenizer;

public class JSONMessage implements KVMessage, Serializable {

    private static final long serialVersionUID = 5549512212003782618L;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    private StatusType status;
    private String key;
    private String value;

    private byte[] byteJSON;
    private String json;

    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    private byte[] toByteArray(String s) {
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public String byteToString(byte[] jsonBytes) {
        // turn bytes to string
        byte[] tmp = addCtrChars(jsonBytes);
        String jsonStr = new String(tmp);

        return jsonStr;
    }

    public byte[] stringToByte(String json) {
        this.byteJSON = addCtrChars(toByteArray(json));
        return this.byteJSON;
    }

    public byte[] getJSONByte() {
        return this.byteJSON;
    }

    public void setMessage(String inStatus, String inKey, String inValue) {
        setStatus(inStatus);
        setKey(inKey);
        setValue(inValue);
    }

    public StatusType getStatus() {
        return this.status;
    }

    public void setStatus(String inStatus) {
        // convert to enum
        StatusType enumStatus = StatusType.valueOf(inStatus);
        this.status = enumStatus;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String inKey) {
        this.key = inKey;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String inValue) {
        this.value = inValue;
    }

    public String getJSON() {
        return this.json;
    }

    public String serialize() {
        // initialize string builder to create mutable string
        StringBuilder strMessage = new StringBuilder();

        // Beginning chars
        strMessage.append("{");
        String statusEntry = String.join("\"status\":\"", this.status.name(), "\",");
        strMessage.append(statusEntry);
        if (this.value != null && this.value != "null" && !this.value.trim().isEmpty()) {
            String KVEntry = String.join("\"", this.key, "\":\"", this.value, "\",");
            strMessage.append(KVEntry);
        } else {
            String KVEntry = String.join("\"", this.key, "\":\"\"");
            strMessage.append(KVEntry);
        }
        strMessage.append("}");

        this.json = strMessage.toString();

        return strMessage.toString();
    }

    public void deserialize(String json) {
        StringTokenizer messageTokens = new StringTokenizer(json, "{}:,\"");

        while (messageTokens.hasMoreTokens()) {
            String k = messageTokens.nextToken();
            String status = messageTokens.nextToken();
            String key = messageTokens.nextToken();
            String value = messageTokens.nextToken();

            setStatus(status);
            setKey(key);
            setValue(value);
            // System.out.print("HERE");
            // System.out.print(String.join(status, key, value));
        }
    }

}
