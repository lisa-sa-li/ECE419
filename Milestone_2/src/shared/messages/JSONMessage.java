package shared.messages;

import java.io.Serializable;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.*;
import com.google.gson.Gson;

import shared.messages.Metadata;

public class JSONMessage implements KVMessage, Serializable {

    private static Logger logger = Logger.getRootLogger();

    private static final long serialVersionUID = 5549512212003782618L;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    private StatusType status;
    private String key;
    private String value;
    // private String metadata;
    private String metadataStr

    private byte[] byteJSON;
    private String json;

    Gson gson = new Gson();

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
        if (this.byteJSON == null) {
            this.stringToByte(this.json);
        }
        return this.byteJSON;
    }

    public void setMessage(String inStatus, String inKey, String inValue, Metadata metadata) {
        // Metadata
        setStatus(inStatus);
        setKey(inKey);
        setValue(inValue);
        setMetadata(metadata);
        this.json = this.serialize();
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

    public void setMetadata(Metadata metadata) {
        if (metadata != null) {
            this.metadataStr = gson.toJson(metadata);
        }
    }

    public void setMetadataStr(String metadata) {
        this.metadataStr = metadata;
    }

    public Metadata getMetadata() {
        if (this.metadataStr == null) {
            return null;
        }
        return gson.fromJson(this.metadataStr, Metadata.class);
    }

    public String serialize() {
        // initialize string builder to create mutable string
        StringBuilder strMessage = new StringBuilder();

        // Beginning chars
        strMessage.append("{");
        String statusEntry = ("\"status\":\"" + this.status.name() + "\",");
        strMessage.append(statusEntry);
        if (this.value == null) {
            this.value = "";
        }
        if (!this.value.trim().isEmpty()) {
            String KVEntry = ("\"" + this.key + "\":\"" + this.value + "\",");
            strMessage.append(KVEntry);
        } else {
            String KVEntry = ("\"" + this.key + "\":\"\"");
            strMessage.append(KVEntry);
        }

        if (!this.metadataStr.trim().isEmpty()) {
            String KVEntry = ("\"metadata\":\"" + this.metadataStr + "\",");
            strMessage.append(KVEntry);
        } else {
            String KVEntry = ("\"metadata\":\"\"");
            strMessage.append(KVEntry);
        }
        strMessage.append("}");

        this.json = strMessage.toString();

        return strMessage.toString();
    }

    public void deserialize(String inJSON) {
        // trim newlines
        inJSON = inJSON.trim();
        this.json = inJSON;

        StringTokenizer messageTokens = new StringTokenizer(inJSON, "{}:,\"");

        // create Array object
        String[] tokens = null;
        tokens = new String[messageTokens.countTokens()];

        // iterate through StringTokenizer tokens
        int count = 0;
        while (messageTokens.hasMoreTokens()) {
            // add tokens to Array
            String nextTok = messageTokens.nextToken();
            tokens[count++] = nextTok;
        }

        String status = tokens[1];
        String key = tokens[2];
        String value;
        String metadataStr;

        if (tokens[3] != "metadata") {
            // This means token[3] is the value to "value"
            value = tokens[3];

            try {
                metadataStr = tokens[5];
            } catch (Exception iobe) {
                metadataStr = null;
            }
        } else {
            // This means there's no value for "value", so it's a DELETE or GET
            value = "";
            try {
                metadataStr = tokens[4];
            } catch (Exception iobe) {
                metadataStr = null;
            }
        }

        // // In a DELETE or GET, there is no tokens[3], so we set it to ""
        // try {
        // value = tokens[3];
        // } catch (Exception iobe) {
        // value = "";
        // }

        setStatus(status);
        setKey(key);
        setValue(value);
        setMetadataStr(metadataStr);
    }

}
