package app_kvClient;

import shared.messages.JSONMessage;
import java.io.IOException;

public interface IClientConnection {
    public JSONMessage receiveJSONMessage() throws IOException;

    public void sendJSONMessage(JSONMessage json) throws IOException;

    public void close() throws IOException;
}