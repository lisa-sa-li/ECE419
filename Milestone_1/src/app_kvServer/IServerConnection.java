package app_kvServer;

import shared.messages.JSONMessage;
import java.io.IOException;

public interface IServerConnection {
    public void connect() throws IOException;

	public JSONMessage receiveJSONMessage() throws IOException;
    
    public void sendJSONMessage(JSONMessage json) throws IOException;
}