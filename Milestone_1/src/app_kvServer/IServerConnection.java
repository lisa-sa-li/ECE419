package app_kvServer;

import shared.messages.JSONMessage;
import java.io.IOException;

public interface IServerConnection {
	// private JSONMessage receiveJSONMessage() throws IOException;
    public void sendJSONMessage(JSONMessage json) throws IOException;

}