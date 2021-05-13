import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class Dstore {
	
	public static void main(String[] args) {
		try {
			var port = Integer.parseInt(args[0]);
			var cport = Integer.parseInt(args[1]);
			var timeout = Integer.parseInt(args[2]);
			var fileFolder = new File("." + File.separator + args[3] + File.separator);
			
			/**If the file_folder argument doesn't exist, creates it. If it does, deletes all the contained files.*/
			if(!fileFolder.exists()) {
				fileFolder.mkdirs();
			} else {
				Arrays.stream(fileFolder.listFiles()).forEach(File::delete);
			}
			
			DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
			new Dstore(port, cport, timeout, fileFolder);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private int timeout;
	private File fileFolder;
	private TCPServer clientServer;
	private TCPClient controllerConnection;
	
	/**Starts the Dstore server and controller connection.*/
	public Dstore(int port, int cport, int timeout, File fileFolder) throws IOException {
		this.timeout = timeout;
		this.fileFolder = fileFolder;
		this.clientServer = new TCPServer(port, timeout, clientHandler);
		this.controllerConnection = new TCPClient(new Socket("localhost", cport), controllerHandler);
		this.clientServer.start();
		this.controllerConnection.start();
		this.controllerConnection.send(Protocol.JOIN_TOKEN + " " + port);
	}
	
	/**Stores the handler for handling client messages.*/
	private MessageHandler clientHandler = new MessageHandler() {
		
		@Override
		public void onSend(TCPClient connection, String message) {
			DstoreLogger.getInstance().messageSent(connection.getSocket(), message);
		}
		
		@Override
		public void onReceive(TCPClient connection, String message) {
			DstoreLogger.getInstance().messageReceived(connection.getSocket(), message);
			var parts = message.trim().split(" ");
			
			try {
				switch(parts[0]) {
					case Protocol.STORE_TOKEN	  		-> store(connection, parts);
					case Protocol.LOAD_DATA_TOKEN 		-> loadData(connection, parts);
					case Protocol.REBALANCE_STORE_TOKEN -> rebalanceStore(connection, parts);
				}
			} catch(IOException e) {
				System.err.println("Error responding to client message \"" + message + "\"!");
			}
		}
		
		public void store(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 3) {
				var fileName = new File(fileFolder, parts[1]);
				var length = Integer.parseInt(parts[2]);
				/**Acknowledges file details received.*/
				connection.send(Protocol.ACK_TOKEN);
				
				/**Attempts to store the file within the timeout.*/
				try {
					var storedWithinTimeout = Utils.executeWithTimeout(timeout, () -> {
						try {
							Files.write(fileName.toPath(), connection.getSocket().getInputStream().readNBytes(length));
							controllerConnection.send(Protocol.STORE_ACK_TOKEN + " " + fileName.getName());
						} catch (IOException e) {
							e.printStackTrace();
						}
					});
					
					/**If not stored within the timeout successfully, logs the error.*/
					if(!storedWithinTimeout) {
						System.err.println("Could not store \"" + fileName + "\" within timeout!");
					}
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		public void loadData(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var file = new File(fileFolder, parts[1]);
				
				/**Checks if the file exists, closes the connection if not.*/
				if(file.exists()) {
					connection.getOutput().write(Files.readAllBytes(file.toPath()));
					connection.getSocket().shutdownOutput();
				} else {
					connection.close();
				}
			}
		}
		
		public void rebalanceStore(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 3) {
				var fileName = new File(fileFolder, parts[1]);
				var length = Integer.parseInt(parts[2]);
				/**Acknowledges file details received.*/
				connection.send(Protocol.ACK_TOKEN);
				
				/**Attempts to store the file within the timeout.*/
				try {
					var storedWithinTimeout = Utils.executeWithTimeout(timeout, () -> {
						try {
							Files.write(fileName.toPath(), connection.getSocket().getInputStream().readNBytes(length));
						} catch (IOException e) {
							e.printStackTrace();
						}
					});
					
					/**If not stored within the timeout successfully, logs the error.*/
					if(!storedWithinTimeout) {
						System.err.println("Could not store \"" + fileName + "\" within timeout!");
					}
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	};
	
	/**Stores the handler for receiving controller messages.*/
	private MessageHandler controllerHandler = new MessageHandler() {
		
		@Override
		public void onSend(TCPClient connection, String message) {
			DstoreLogger.getInstance().messageSent(connection.getSocket(), message);
		}

		@Override
		public void onReceive(TCPClient connection, String message) {
			DstoreLogger.getInstance().messageReceived(connection.getSocket(), message);
			var parts = message.trim().split(" ");
			
			try {
				switch(parts[0]) {
					case Protocol.REMOVE_TOKEN    -> remove(connection, parts);
					case Protocol.LIST_TOKEN      -> list(connection, parts);
					case Protocol.REBALANCE_TOKEN -> rebalance(connection, parts);
				}
			} catch(IOException e) {
				System.err.println("Error responding to client message \"" + message + "\"!");
			}
		}
		
		public void remove(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var file = new File(fileFolder, parts[1]);
				
				/**Checks if the file exists.*/
				if(file.exists()) {
					Files.delete(file.toPath());
					connection.send(Protocol.REMOVE_ACK_TOKEN);
				} else {
					controllerConnection.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + parts[1]);
				}
			}
		}
		
		public void list(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 1) {
				StringBuilder listBuilder = new StringBuilder(Protocol.LIST_TOKEN);
				Utils.getAllFilesInFolder(fileFolder).stream()
					.map(file -> fileFolder.toURI().relativize(file.toURI()).getPath())
					.forEach(file -> listBuilder.append(' ').append(file));
				connection.send(listBuilder.toString());
			}
		}
		
		public void rebalance(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the rebalance token contains anything to do.*/
			if(parts.length >= 3) {
				/**Stores the files to send to other Dstores mapped to the list of Dstore endpoint ports to send each file to.*/
				var filesToSend = new ConcurrentHashMap<String, List<Integer>>();
				/**Stores the files to remove.*/
				var filesToRemove = new Vector<String>();
				/**Stores the index iterated through of the rebalance string, starts after the REBALANCE token.*/
				var stringIndex = 1;
				/**Stores the extracted number of files to store.*/
				var sendCount = Integer.parseInt(parts[stringIndex]);
				
				/**Iterates over every file to store.*/
				for(int i = 0; i < sendCount; i++) {
					/**Extracts the file name from the next string in the message.*/
					stringIndex++;
					var fileName = parts[stringIndex];
					/**Extract the number of ports to send to from the next string in the message.*/
					stringIndex++;
					var portCount = Integer.parseInt(parts[stringIndex]);
					
					/**Initialises the map for the ports to store to for the file.*/
					filesToSend.put(fileName, new Vector<>());
					
					/**Iterates over the number of port counts and adds the ports to the files to store map.*/
					for(int j = 0; j < portCount; j++) {
						stringIndex++;
						filesToSend.get(fileName).add(Integer.parseInt(parts[stringIndex]));
					}
				}
				
				/**Extract the number of files to remove from the next string in the message.*/
				stringIndex++;
				var removeCount = Integer.parseInt(parts[stringIndex]);
				
				/**Iterates over every file to remove and adds it to the list.*/
				for(int i = 0; i < removeCount; i++) {
					stringIndex++;
					filesToRemove.add(parts[stringIndex]);
				}
				
				/**Attempts to send all of the send commands within the timeout.*/
				try {
					/**Stores the time before sending the files.*/
					var startTime = System.currentTimeMillis();
					
					var sentWithinTimeout = sendCount == 0
							|| Utils.executeWithTimeout(timeout, () -> {
						/**Iterates over each file to send, and all the ports to send each file to.*/
						filesToSend.forEach((fileName, ports) -> {
							/**Stores the path to the file.*/
							var filePath = new File(fileFolder, fileName).toPath();
							
							ports.forEach(port -> {
								/**Creates a client with a specific protocol to send the file data after receiving the ACK.*/
								try {
									TCPClient client = new TCPClient(new Socket("localhost", port), new MessageHandler() {

										@Override
										public void onSend(TCPClient connection, String message) {}

										@Override
										public void onReceive(TCPClient ackConnection, String ackMessage) {
											DstoreLogger.getInstance().messageReceived(ackConnection.getSocket(), ackMessage);
											
											/**Checks if the message is the expected ACK token.*/
											if(ackMessage.equals(Protocol.ACK_TOKEN)) {
												/**Sends the data for the file to the client.*/
												try {
													ackConnection.getOutput().write(Files.readAllBytes(filePath));
													ackConnection.getSocket().shutdownOutput();
												} catch (IOException e) {
													e.printStackTrace();
												}
											}
										}
									});
									
									/**Starts the client and sends the rebalance store message.*/
									client.start();
									client.send(Protocol.REBALANCE_STORE_TOKEN + " " + fileName + " " + Files.size(filePath));
								} catch (IOException e) {
									e.printStackTrace();
								}
							});
						});
					});
					
					/**Checks if the files were all sent within the timeout.*/
					if(sentWithinTimeout) {
						/**Iterates over every file to delete and attempts to deletes them all within the remaining timeout.*/
						var removedWithinTimeout = removeCount == 0 ||
								Utils.executeWithTimeout(timeout - (System.currentTimeMillis() - startTime), () -> {
							filesToRemove.forEach(fileName -> {
								try {
									Files.delete(new File(fileFolder, fileName).toPath());
								} catch (IOException e) {
									e.printStackTrace();
								}
							});
						});
						
						/**If all files deleted within timeout, sends a rebalance completion message.*/
						if(removedWithinTimeout) {
							connection.send(Protocol.REBALANCE_COMPLETE_TOKEN);
						} else {
							System.err.println("Error removing files within timeout during rebalance!");
						}
					} else {
						System.err.println("Error sending files within timeout during rebalance!");
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				connection.send(Protocol.REBALANCE_COMPLETE_TOKEN);
			}
		}
	};
}