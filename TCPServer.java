import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class TCPServer extends Thread {

	/**Stores the port the server is running on.*/
	private volatile int port;
	/**Stores the timeout for the server.*/
	private volatile int timeout;
	/**Stores the server socket.*/
	private volatile ServerSocket serverSocket;
	/**Stores the packet handler.*/
	private volatile MessageHandler handler;
	
	/**Stores whether the server should run or not.*/
	private volatile boolean running;
	
	public TCPServer(int port, int timeout, MessageHandler handler) throws IOException {
		this.port = port;
		this.timeout = timeout;
		this.serverSocket = new ServerSocket(port);
		this.handler = handler;
	}
	
	@Override
	public void start() {
		running = true;
		super.start();
	}
	
	/**Accepts any incoming connections and creates a new TCPClient thread to handle incoming requests.*/
	@Override
	public void run() {
		while(running && !serverSocket.isClosed()) {
			try {
				Socket client = serverSocket.accept();
				TCPClient connection = new TCPClient(client, handler);
				connection.getSocket().setSoTimeout(timeout);
				connection.start();
			} catch(SocketException e) {
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	public int getPort() {
		return port;
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}
}