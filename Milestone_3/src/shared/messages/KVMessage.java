package shared.messages;

public interface KVMessage {

	public enum StatusType {
		GET, /* Get - request */
		GET_ERROR, /* requested tuple (i.e. value) not found */
		GET_SUCCESS, /* requested tuple (i.e. value) found */
		PUT, /* Put - request */
		PUT_MANY, /*
					 * Put - request, given a bunch of keys at once, seperated by \n, used for ECS
					 * data transfers
					 */
		PUT_SUCCESS, /* Put - request successful, tuple inserted */
		PUT_UPDATE, /* Put - request successful, i.e. value updated */
		PUT_ERROR, /* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, /* Delete - request not successful */
		DISCONNECTED, /* Server or client is disconnected */
		NO_STATUS, /* Equivalent to null */

		SET_METADATA,
		MOVE_DATA,
		DONE,
		START,
		STOP,
		SERVER_STOPPED, // Server has not been started by the ECS, cannot process client commands
		SERVER_NOT_RESPONSIBLE, /* the server is not responsible for the key */
		SERVER_WRITE_LOCK,

		INIT_REPLICATE_DATA,
		UPDATE_REPLICATE_DATA,
		DELETE_REPLICATE_DATA,
	}

	/**
	 * @return the key that is associated with this message,
	 *         null if not key is associated.
	 */
	public String getKey();

	/**
	 * @return the value that is associated with this message,
	 *         null if not value is associated.
	 */
	public String getValue();

	/**
	 * @return a status string that is used to identify request types,
	 *         response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * @return a
	 */

}
