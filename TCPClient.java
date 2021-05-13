import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

public class TCPClient extends Thread {
	
	/**Stores the socket.*/
	private volatile Socket socket;
	/**Stores the packet protocol.*/
	private volatile MessageHandler handler;
	
	/**Stores the InputStream for the socket.*/
	private volatile InputStream input;
	/**Stores the OutputStream for the socket.*/
	private volatile OutputStream output;
	/**Stores the PrintWriter for writing strings.*/
	private volatile PrintWriter writer;
	/**Stores the BufferedReader for reading strings.*/
	private volatile BufferedReader reader;
	
	/**Stores whether the connection is active or not.*/
	private volatile boolean active = true;
	
	/**Starts the TCP connection sockets.*/
	public TCPClient(final Socket socket, final MessageHandler handler) throws IOException {
		this.socket = socket;
		this.handler = handler;
	}
	
	public void send(String message) throws IOException {
		if(!socket.isClosed() && socket.isConnected()) {
			handler.onSend(this, message);
			writer.println(message);
		}
	}
	
	public String receive() throws IOException {
		return reader.readLine();
	}
	
	/**Starts the thread to handle the connection.*/
	@Override
	public void run() {
		try {
			output = socket.getOutputStream();
			input = socket.getInputStream();
			writer = new PrintWriter(output, true);
			reader = new BufferedReader(new InputStreamReader(input));
			String messageBuffer;
			
			/**Whilst the socket is open, passes all input to the protocol.*/
			while((messageBuffer = receive()) != null) {
				handler.onReceive(this, messageBuffer);
			}
			
			/**After the socket closes, deactivates the thread.*/
			close();
		} catch(SocketException e) {
			active = false;
		} catch (IOException e) {
			e.printStackTrace();
			active = false;
		}
	}
	
	public void close() throws IOException {
		input.close();
		output.close();
		reader.close();
		writer.close();
		socket.close();
		active = false;
		interrupt();
	}

	public Socket getSocket() {
		return socket;
	}

	public InputStream getInput() {
		return input;
	}

	public OutputStream getOutput() {
		return output;
	}

	public PrintWriter getWriter() {
		return writer;
	}

	public BufferedReader getReader() {
		return reader;
	}

	public boolean isActive() {
		return active;
	}
}