import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Controller {

	public static void main(String[] args) {
		try {
			var cport = Integer.parseInt(args[0]);
			var R = Integer.parseInt(args[1]);
			var timeout = Integer.parseInt(args[2]);
			var rebalancePeriod = Integer.parseInt(args[3]);
			
			ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
			new Controller(cport, R, timeout, rebalancePeriod);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**Stores the replication factor of the files.*/
	private int R;
	/**Stores the server to accept clients and Dstores.*/
	private TCPServer server;
	/**Stores the file index to handle stored files across Dstores.*/
	private FileIndex fileIndex;
	/**Maps the socket addresses to the server ports for Dstores.*/
	private ConcurrentHashMap<TCPClient, Integer> ports = new ConcurrentHashMap<>();
	
	public Controller(int cport, int R, int timeout, int rebalancePeriod) throws IOException {
		this.R = R;
		this.server = new TCPServer(cport, timeout, controllerHandler);
		this.fileIndex = new FileIndex(this, timeout, rebalancePeriod);
		this.server.start();
		
		/**Starts the update thread.*/
		new Thread(() -> {
			while(!Thread.interrupted()) {
				fileIndex.update();
			}
		}).start();
	}
	
	/**Main handler, passes messages onto the correct handler.*/
	private MessageHandler controllerHandler = new MessageHandler() {
		
		@Override
		public void onSend(TCPClient connection, String message) {
			ControllerLogger.getInstance().messageSent(connection.getSocket(), message);
		}
		
		@Override
		public void onReceive(TCPClient connection, String message) {
			ControllerLogger.getInstance().messageReceived(connection.getSocket(), message);
			
			/**Locks the update read lock, preventing the file index from updating.*/
			fileIndex.getUpdateLock().readLock().lock();
			
			try {
				/**Passes the message to a separate handler depending on the sender.*/
				if(ports.containsKey(connection) || message.startsWith(Protocol.JOIN_TOKEN))
					dstoreHandler.onReceive(connection, message);
				else
					clientHandler.onReceive(connection, message);
			} finally {
				/**Unlocks the update read lock, allowing the file index to potentially begin updating.*/
				fileIndex.getUpdateLock().readLock().unlock();
			}
		}
	};
	
	/**Handles incoming messages from Dstores.*/
	private MessageHandler dstoreHandler = new MessageHandler() {
		
		@Override
		public void onSend(TCPClient connection, String message) {}
		
		@Override
		public void onReceive(TCPClient connection, String message) {
			var parts = message.split(" ");
			var shouldLock = RebalancePhase.shouldLock(parts[0], fileIndex.getRebalancePhase());
			
			/**Locks the rebalance read lock if the message is not a rebalance completion message.*/
			if(shouldLock) fileIndex.getRebalanceLock().readLock().lock();
			
			try {
				try {
					switch(parts[0]) {
						case Protocol.JOIN_TOKEN	   		   -> join(connection, parts);
						case Protocol.STORE_ACK_TOKEN  		   -> storeAck(connection, parts);
						case Protocol.REMOVE_ACK_TOKEN 		   -> removeAck(connection, parts);
						case Protocol.LIST_TOKEN	   		   -> list(connection, parts);
						case Protocol.REBALANCE_COMPLETE_TOKEN -> rebalanceComplete(connection, parts);
					}
				} catch(IOException e) {
					e.printStackTrace();
				}
			} finally {
				/**Unlocks the rebalance read lock if the message is not a rebalance completion message.*/
				if(shouldLock) fileIndex.getRebalanceLock().readLock().unlock();
			}
		}
		
		public void join(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var port = Integer.parseInt(parts[1]);
				ControllerLogger.getInstance().dstoreJoined(connection.getSocket(), port);
				fileIndex.join(connection);
				ports.put(connection, port);
			}
		}
		
		public void storeAck(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var fileName = parts[1];
				
				/**Checks if the file is being stored.*/
				if(fileIndex.exists(fileName) && fileIndex.getState().get(fileName) == FileState.STORE) {
					/**Updates the index and decrements the required acknowledgements.*/
					var requiredAcks = fileIndex.getRequiredAcks().get(fileName).decrementAndGet();
					fileIndex.getFileDstores().get(fileName).add(connection);
					fileIndex.getDstoreFiles().get(connection).add(fileName);
					
					/**Checks if an appropriate number of STORE_ACKs have been processed.
					 * If so, updates the state of the file to the default state in the file index.*/
					if(requiredAcks == 0) {
						fileIndex.getState().put(fileName, FileState.AVAILABLE);
						fileIndex.getStateTimers().remove(fileName);
						fileIndex.getRequiredAcks().remove(fileName);
						fileIndex.getStateClients().get(fileName).get(0).send(Protocol.STORE_COMPLETE_TOKEN);
					}
				} else {
					connection.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
				}
			}
		}
		
		public void removeAck(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var fileName = parts[1];
				
				/**Checks if the file is being removed.*/
				if(fileIndex.exists(fileName) && fileIndex.getState().get(fileName) == FileState.REMOVE) {
					/**Decrements the required acknowledgements.*/
					var requiredAcks = fileIndex.getRequiredAcks().get(fileName).decrementAndGet();
					
					/**Checks if an appropriate number of REMOVE_ACKs have been processed.
					 * If so, removes the file from the file index.*/
					if(requiredAcks == 0) {
						fileIndex.getState().remove(fileName);
						fileIndex.getStateTimers().remove(fileName);
						fileIndex.getRequiredAcks().remove(fileName);
						fileIndex.getStateClients().get(fileName).get(0).send(Protocol.REMOVE_COMPLETE_TOKEN);
					}
				} else {
					connection.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
				}
			}
		}
		
		public void list(TCPClient connection, String[] parts) {
			/**Checks if the phase is correct.*/
			if(fileIndex.getRebalancePhase() == RebalancePhase.ACQUIRING_LIST) {
				/**Collects the files present in the message.*/
				var files = Arrays.stream(parts).skip(1).collect(Collectors.toList());
				fileIndex.rebalanceListReceive(connection, files);
			}
		}
		
		public void rebalanceComplete(TCPClient connection, String[] parts) {
			/**Checks if the message is formatted correctly and the phase is correct.*/
			if(parts.length == 1 && fileIndex.getRebalancePhase() == RebalancePhase.WAITING_ON_ACKS) {
				fileIndex.rebalanceWaitReceive(connection);
			}
		}
	};
	
	/**Handles incoming messages from clients.*/
	private MessageHandler clientHandler = new MessageHandler() {
		
		@Override
		public void onSend(TCPClient connection, String message) {}
		
		@Override
		public void onReceive(TCPClient connection, String message) {
			var parts = message.split(" ");
			
			/**Locks the rebalance read lock, preventing the file index from rebalancing.*/
			fileIndex.getRebalanceLock().readLock().lock();
			
			try {
				/**Checks if there are enough Dstores to handle requests.*/
				if(ports.size() >= R) {
					switch(parts[0]) {
						case Protocol.STORE_TOKEN  -> store(connection, parts);
						case Protocol.LOAD_TOKEN   -> load(connection, parts);
						case Protocol.RELOAD_TOKEN -> reload(connection, parts);
						case Protocol.REMOVE_TOKEN -> remove(connection, parts);
						case Protocol.LIST_TOKEN   -> list(connection, parts);
					}
				} else {
					connection.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
				}
			} catch(IOException e) {
				System.err.println("Error responding to client message " + parts[0] + "!");
				e.printStackTrace();
			}
			
			/**Unlocks the rebalance read lock, allowing the file index to potentially begin rebalancing.*/
			fileIndex.getRebalanceLock().readLock().unlock();
		}
		
		public void store(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 3) {
				var fileName = parts[1];
				var fileSize = Integer.parseInt(parts[2]);
				
				/**Checks if the file can be stored according to the file index.*/
				if(!fileIndex.exists(fileName)) {
					var messageBuilder = new StringBuilder(Protocol.STORE_TO_TOKEN);
					var locations = new Vector<TCPClient>();
					
					/**Collects upto R Dstores with the smallest number of files, and sends their ports to the client.*/
					fileIndex.getDstoreFiles().entrySet().stream()
							.sorted(Comparator.comparingInt(entry -> entry.getValue().size()))
							.limit(R)
							.forEach(entry -> {
								locations.add(entry.getKey());
								messageBuilder.append(' ').append(ports.get(entry.getKey()));
							});
					
					fileIndex.store(fileName, fileSize, connection, locations);
					connection.send(messageBuilder.toString());
				} else {
					connection.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
				}
			}
		}
		
		public void load(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var fileName = parts[1];
				
				/**Checks if the file can be loaded according to the file index.*/
				if(fileIndex.exists(fileName) && fileIndex.getState().get(fileName) == FileState.AVAILABLE) {
					var location = fileIndex.getFileDstores().get(fileName).get(0);
					fileIndex.load(fileName, connection, location);
					connection.send(Protocol.LOAD_FROM_TOKEN + " " + ports.get(location));
				} else {
					connection.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
				}
			}
		}
		
		public void reload(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var fileName = parts[1];
				
				/**Checks if the file can be reloaded according to the file index.*/
				if(fileIndex.exists(fileName)) {
					/**Gets the Dstores the client has already tried to load from.*/
					var loadAttempts = fileIndex.getLoadAttempts().get(connection).get(fileName);
					/**Gets one of the Dstores the client hasn't loaded from that contains the file.*/
					var nextDstore = fileIndex.getFileDstores().get(fileName).stream()
							.filter(Predicate.not(loadAttempts::contains))
							.findFirst()
							.orElse(null);
					
					/**Checks if there is a remaining Dstore, if so, sends its port and adds the Dstore to the consumed list.
					 * If not, removes the load attempts for the file from the map for the client.*/
					if(nextDstore != null) {
						loadAttempts.add(nextDstore);
						connection.send(Protocol.LOAD_FROM_TOKEN + " " + ports.get(nextDstore));
					} else {
						fileIndex.getLoadAttempts().get(connection).remove(fileName);
						connection.send(Protocol.ERROR_LOAD_TOKEN);
					}
				} else if(!fileIndex.exists(fileName)) {
					connection.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
				}
			}
		}
		
		public void remove(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 2) {
				var fileName = parts[1];
				
				/**Checks if the file can be removed according to the file index.*/
				if(fileIndex.exists(fileName) && fileIndex.getState().get(fileName) == FileState.AVAILABLE) {
					var locations = fileIndex.getFileDstores().get(fileName);
					fileIndex.remove(fileName, connection, locations);
					for(TCPClient location : locations) location.send(Protocol.REMOVE_TOKEN + " " + fileName);
				} else {
					connection.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
				}
			}
		}
		
		public void list(TCPClient connection, String[] parts) throws IOException {
			/**Checks if the message is formatted correctly.*/
			if(parts.length == 1) {
				StringBuilder messageBuilder = new StringBuilder(Protocol.LIST_TOKEN);
				fileIndex.list().stream()
						.filter(fileName -> fileIndex.getState().get(fileName) == FileState.AVAILABLE)
						.forEach(fileName -> messageBuilder.append(' ').append(fileName));
				connection.send(messageBuilder.toString());
			}
		}
	};
	
	public int getR() {
		return R;
	}
	
	public TCPServer getServer() {
		return server;
	}
	
	public ConcurrentHashMap<TCPClient, Integer> getPorts() {
		return ports;
	}
}