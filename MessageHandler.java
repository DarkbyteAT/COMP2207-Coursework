public interface MessageHandler {
	/**Runs before a packet is sent.*/
	void onSend(TCPClient connection, String message);
	/**Runs when a packet is received.*/
	void onReceive(TCPClient connection, String message);
}