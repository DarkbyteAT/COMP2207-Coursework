import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FileIndex {
	
	/**Stores an instance of the Controller class for use during the rebalance operation.*/
	private volatile Controller controller;
	/**Stores the timeout length for the index states.*/
	private volatile long timeout;
	/**Stores the period between rebalances.*/
	private volatile long rebalancePeriod;
	/**Stores the time of the last rebalance operation's conclusion.*/
	private volatile long lastRebalance;
	/**Stores the start time of the current rebalance.*/
	private volatile long rebalanceTimer;
	/**Stores how far along in the rebalance the current rebalance operation is.
	 * <ol start>
	 * <li>Available</li>
	 * <li>Collecting File Lists</li>
	 * <li>Computing Allocations</li>
	 * <li>Awaiting Responses</li>
	 * */
	private volatile RebalancePhase rebalancePhase = RebalancePhase.AVAILABLE;
	
	/**Stores whether a Dstore has joined or not.*/
	private AtomicBoolean dstoreJoinedFlag = new AtomicBoolean(false);
	/**Stores the rebalancing flag. Indicates that a rebalance is occurring.*/
	private AtomicBoolean rebalancing = new AtomicBoolean(false);
	/**Stores the ReadWriteLock for handling locking and queueing client operations during rebalances.*/
	private volatile ReentrantReadWriteLock rebalanceLock = new ReentrantReadWriteLock(true);
	/**Stores the ReadWriteLock for handling locking during index updates.*/
	private volatile ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock(true);
	
	/**Maps files to their current index state.*/
	private ConcurrentHashMap<String, FileState> state = new ConcurrentHashMap<>();
	/**Maps files to the timers associated with their states.*/
	private ConcurrentHashMap<String, Long> stateTimers = new ConcurrentHashMap<>();
	/**Maps files to their sizes.*/
	private ConcurrentHashMap<String, Integer> size = new ConcurrentHashMap<>();
	/**Maps Dstores to the list of files they store.*/
	private ConcurrentHashMap<TCPClient, List<String>> dstoreFiles = new ConcurrentHashMap<>();
	/**Maps files to the list of Dstores storing them.*/
	private ConcurrentHashMap<String, List<TCPClient>> fileDstores = new ConcurrentHashMap<>();
	
	/**Maps files to the list of clients resulting in their states.*/
	private ConcurrentHashMap<String, List<TCPClient>> stateClients = new ConcurrentHashMap<>();
	/**Stores the Dstores each client has tried to load each loaded file from.*/
	private ConcurrentHashMap<TCPClient, ConcurrentHashMap<String, List<TCPClient>>> loadAttempts = new ConcurrentHashMap<>();
	/**Stores the number of required acknowledgements for the completion of a modification operation on a file.*/
	private ConcurrentHashMap<String, AtomicInteger> requiredAcks = new ConcurrentHashMap<>();
	
	/**Stores the list of files received from each Dstore during a rebalance.*/
	private ConcurrentHashMap<TCPClient, List<String>> rebalanceDstoreFiles = new ConcurrentHashMap<>();
	/**Stores the list of files to send for each Dstore during a rebalance.*/
	private ConcurrentHashMap<TCPClient, ConcurrentHashMap<String, List<TCPClient>>> rebalanceStores = new ConcurrentHashMap<>();
	/**Stores the list of files to remove for each Dstore during a rebalance.*/
	private ConcurrentHashMap<TCPClient, List<String>> rebalanceRemovals = new ConcurrentHashMap<>();
	/**Stores the list of Dstores remaining that haven't sent REBALANCE_COMPLETE tokens.*/
	private Vector<TCPClient> rebalanceAcksRequired = new Vector<>();
	
	public FileIndex(Controller controller, long timeout, long rebalancePeriod) {
		this.controller = controller;
		this.timeout = timeout;
		this.rebalancePeriod = rebalancePeriod;
		this.lastRebalance = System.currentTimeMillis();
	}
	
	/**Updates the index when a store is in progress.*/
	public void store(String fileName, int size, TCPClient client, List<TCPClient> locations) {
		this.state.put(fileName, FileState.STORE);
		this.stateTimers.put(fileName, System.currentTimeMillis());
		this.size.put(fileName, size);
		this.fileDstores.put(fileName, new Vector<>());
		this.stateClients.put(fileName, new Vector<>(Arrays.asList(client)));
		this.requiredAcks.put(fileName, new AtomicInteger(locations.size()));
	}
	
	/**Updates the index when a load is requested.*/
	public void load(String fileName, TCPClient client, TCPClient location) {
		/**Updates the load attempts for the file.*/
		var clientLoadMap = (this.loadAttempts.containsKey(client) 
				? this.loadAttempts.get(client) 
				: this.loadAttempts.put(client, new ConcurrentHashMap<>()));
		clientLoadMap.put(fileName, new Vector<>(Arrays.asList(location)));
	}
	
	/**Updates the index when a remove is in progress.*/
	public void remove(String fileName, TCPClient client, List<TCPClient> locations) {
		this.state.put(fileName, FileState.REMOVE);
		this.stateTimers.put(fileName, System.currentTimeMillis());
		this.size.remove(fileName);
		this.dstoreFiles.values().forEach(fileList -> fileList.remove(fileName));
		this.stateClients.put(fileName, new Vector<>(Arrays.asList(client)));
		this.fileDstores.remove(fileName);
		this.requiredAcks.put(fileName, new AtomicInteger(locations.size()));
	}
	
	/**Updates the index when a join is in progress and forces a rebalance whenever next possible.*/
	public void join(TCPClient dstore) {
		this.dstoreJoinedFlag.set(true);
		this.dstoreFiles.put(dstore, new Vector<>());
	}
	
	/**Updates the index state timer and handles any expired timers.*/
	public void update() {
		/**Doesn't allow for regular updates during a rebalance.
		 * Passes to the rebalance update method if rebalancing.*/
		if(!isRebalancing()) {
			updateLock.writeLock().lock();
			
			/**Collects any files with expired states.*/
			var currentTime = System.currentTimeMillis();
			var expired = stateTimers.entrySet().stream()
					.filter(entry -> currentTime - entry.getValue() >= timeout)
					.collect(Collectors.mapping(Entry::getKey, Collectors.toList()));
			
			/**Sets expired states back to the default state.*/
			expired.forEach(file -> expireState(file, state.get(file)));
			
			/**Starts a rebalance if every state is not in a modifying state and a rebalance is due.*/
			if((currentTime - lastRebalance >= rebalancePeriod
					&& state.values().stream().allMatch(Predicate.isEqual(FileState.AVAILABLE)))
					|| dstoreJoinedFlag.get()) {
				if(dstoreJoinedFlag.get()) dstoreJoinedFlag.set(false);
				rebalanceStart();
			}
			
			updateLock.writeLock().unlock();
		} else rebalanceUpdate();
	}
	
	/**Handles the expiration of a given file in a given state.*/
	public void expireState(String fileName, FileState expiredState) {
		switch(expiredState) {
			case STORE -> {
				state.remove(fileName);
				size.remove(fileName);
				fileDstores.remove(fileName);
				dstoreFiles.values().forEach(fileList -> fileList.remove(fileName));
				requiredAcks.remove(fileName);
			}
			
			case REMOVE -> {
				state.remove(fileName);
				requiredAcks.remove(fileName);
			}
			
			default -> throw new IllegalArgumentException("Cannot expire a file in the AVAILABLE state!");
		}
		
		stateClients.remove(fileName);
		stateTimers.remove(fileName);
	}
	
	/**Updates the index to begin a rebalance.*/
	public void rebalanceStart() {
		rebalanceLock.writeLock().lock();
		
		/**Checks if there are enough Dstores to begin a rebalance.*/
		if(controller.getPorts().size() >= controller.getR()) {
			rebalancing.set(true);
			rebalanceTimer = System.currentTimeMillis();
			rebalanceListStart();
		} else {
			rebalanceLock.writeLock().unlock();
			rebalancing.set(false);
		}
	}
	
	/**Updates the index whilst rebalancing and handles the rebalance timeout.*/
	public void rebalanceUpdate() {
		/**Checks if the rebalance operation hasn't timed out.
		 * Otherwise restarts the rebalance operation.*/
		if(System.currentTimeMillis() - rebalanceTimer < timeout) {
			switch(rebalancePhase) {
				case ACQUIRING_LIST -> rebalanceListUpdate();
				case WAITING_ON_ACKS -> rebalanceWaitUpdate();
				default -> throw new IllegalStateException("Illegal state " + rebalancePhase + " reached in rebalanceUpdate()!");
			}
		} else {
			rebalanceRestart();
		}
	}
	
	/**Restarts the rebalance if a timeout happened.*/
	public void rebalanceRestart() {
		/**If failed whilst acquiring lists, closes connections with all Dstores that failed to return a list in time.*/
		if(rebalancePhase == RebalancePhase.ACQUIRING_LIST) {
			dstoreFiles.entrySet().stream()
					.filter(entry -> !rebalanceDstoreFiles.containsKey(entry.getKey()))
					.forEach(entry -> {
						try {
							entry.getKey().close();
						} catch (IOException e) {
							System.err.println("Error closing port after failing to receive list!");
							e.printStackTrace();
						}
					});
		}
		
		/**Logs the phase in which the rebalance failed.*/
		System.err.println("Rebalance failed in " + rebalancePhase + " phase, retrying...");
		
		/**Checks if there are enough Dstores to begin a rebalance.*/
		if(controller.getPorts().size() >= controller.getR()) {
			rebalanceTimer = System.currentTimeMillis();
			rebalanceListStart();
		} else {
			rebalanceLock.writeLock().unlock();
			rebalancing.set(false);
		}
	}
	
	/**Begins the rebalance "Collecting File Lists" stage.*/
	public void rebalanceListStart() {
		rebalancePhase = RebalancePhase.ACQUIRING_LIST;
		rebalanceDstoreFiles = new ConcurrentHashMap<>();
		
		/**Sends a LIST request to every Dstore.*/
		controller.getPorts().keySet().forEach(dstore -> {
			try {
				dstore.send(Protocol.LIST_TOKEN);
			} catch (IOException e) {
				System.err.println("Error sending rebalance LIST to Dstore!");
				e.printStackTrace();
			}
		});
	}
	
	/**Handles a file list retrieval from a Dstore.*/
	public void rebalanceListReceive(TCPClient dstore, List<String> files) {
		rebalanceDstoreFiles.put(dstore, files);
	}
	
	/**Handles updating during the "Collecting File Lists" stage.*/
	public void rebalanceListUpdate() {
		/**Checks if the Dstores have all sent updated file lists, if so, begins allocation of files to Dstores.*/
		if(rebalanceDstoreFiles.size() == controller.getPorts().size()) {
			rebalanceAllocationStart();
		}
	}
	
	/**Begins the rebalance "Computing Allocations" stage.*/
	public void rebalanceAllocationStart() {
		rebalancePhase = RebalancePhase.FILE_ALLOCATION;
		var maxFiles = (int) Math.ceil(((float) controller.getR() * (float) size()) / (float) controller.getPorts().size());
		var dstoreOperations = new HashMap<TCPClient, Integer>();
		rebalanceStores = new ConcurrentHashMap<>();
		rebalanceRemovals = new ConcurrentHashMap<>();
		
		/**Iterates over each Dstore and removes every file that doesn't exist in the index.*/
		rebalanceDstoreFiles.forEach((dstore, files) -> {
			var toRemove = files.stream().filter(Predicate.not(this::exists)).collect(Collectors.toList());
			rebalanceRemovals.put(dstore, toRemove);
			/**Increments the number of operations done by the Dstore by the number of files to remove.*/
			dstoreOperations.put(dstore, dstoreOperations.containsKey(dstore) 
					? dstoreOperations.get(dstore) + toRemove.size() : toRemove.size());
		});
		
		/**Calculates the number of files in each Dstore taking into account the removed files.*/
		var dstoreFileCount = new HashMap<TCPClient, Integer>();
		rebalanceDstoreFiles.entrySet().stream().forEach(entry -> dstoreFileCount.put(entry.getKey(),
						entry.getValue().size() - rebalanceRemovals.get(entry.getKey()).size()));
		
		/**Calculates the required change in the number of Dstores storing each file.
		 * Positive implies it needs to be added to Dstores, negative implies it needs to be removed from Dstores.*/
		var fileLocationCountDelta = new HashMap<String, Integer>();
		list().stream().forEach(file -> fileLocationCountDelta.put(file, (int) (controller.getR()
						- rebalanceDstoreFiles.values().stream()
								.filter(files -> files.contains(file))
								.count())));
		
		/**Corrects the Dstore count delta for each file by adding and removing the file from Dstores until the delta reaches 0.
		 * When adding and removing files, Dstores with many files should remove files and Dstores.
		 * Conversely, Dstores with few files should have files added to them.
		 * Sorts the delta entries in ascending order to handle removals first.*/
		fileLocationCountDelta.entrySet().stream()
				.sorted(Entry.comparingByValue())
				.forEach(deltaEntry -> {
					var fileName = deltaEntry.getKey();
					var delta = deltaEntry.getValue();
					var absDelta = Math.abs(delta);
					
					if(delta > 0) {
						/**Iterates over the smallest delta Dstores that don't contain the file.*/
						dstoreFileCount.entrySet().stream()
								.filter(entry -> entry.getValue() < maxFiles && !rebalanceDstoreFiles.get(entry.getKey()).contains(fileName))
								.sorted(Entry.comparingByValue())
								.limit(absDelta)
								.forEach(endpointEntry -> {
									/**Iterates over every Dstore and picks the Dstore with the lowest number of operations to send from.*/
									rebalanceDstoreFiles.entrySet().stream()
											.filter(dstoreEntry -> dstoreEntry.getValue().contains(fileName))
											.min(Comparator.comparingInt(dstoreEntry -> dstoreOperations.get(dstoreEntry.getKey())))
											.ifPresent(startpointEntry -> {
												/**Assigns the low-operation Dstore to send the file to the low-file Dstore.*/
												if(rebalanceStores.containsKey(startpointEntry.getKey())) {
													if(rebalanceStores.get(startpointEntry.getKey()).containsKey(fileName)) {
														rebalanceStores.get(startpointEntry.getKey()).get(fileName).add(endpointEntry.getKey());
													} else {
														rebalanceStores.get(startpointEntry.getKey()).put(fileName,
																new Vector<>(Arrays.asList(endpointEntry.getKey())));
													}
												} else {
													rebalanceStores.put(startpointEntry.getKey(), new ConcurrentHashMap<>());
													rebalanceStores.get(startpointEntry.getKey())
															.put(fileName, new Vector<>(Arrays.asList(endpointEntry.getKey())));
												}
												
												/**Adds the file to the low-file Dstore in the Dstore files map.*/
												rebalanceDstoreFiles.get(endpointEntry.getKey()).add(fileName);
												/**Increments the number of files stored by the low-file Dstore.*/
												endpointEntry.setValue(endpointEntry.getValue() + 1);
												/**Increments the number of operations done by the Dstore sending the file.*/
												dstoreOperations.put(startpointEntry.getKey(),
														dstoreOperations.containsKey(startpointEntry.getKey()) 
																? dstoreOperations.get(startpointEntry.getKey()) + 1 : 1);
											});
								});
					} else if(delta < 0) {
						/**Iterates over the largest delta Dstores that do contain the file.*/
						for(int i = 0; i < absDelta; i++) {
							dstoreFileCount.entrySet().stream()
									.filter(entry -> rebalanceDstoreFiles.get(entry.getKey()).contains(fileName))
									.max(Comparator.comparingInt(entry -> entry.getValue()))
									.ifPresent(entry -> {
										/**Assigns the file to be removed from the Dstore.*/
										if(rebalanceRemovals.containsKey(entry.getKey())) {
											rebalanceRemovals.get(entry.getKey()).add(fileName);
										} else {
											rebalanceRemovals.put(entry.getKey(), new Vector<>(Arrays.asList(fileName)));
										}
										
										/**Removes the file from the Dstore in the Dstore files map.*/
										rebalanceDstoreFiles.get(entry.getKey()).remove(fileName);
										/**Decrements the number of files stored by the Dstore.*/
										entry.setValue(entry.getValue() - 1);
										/**Increments the number of operations done by the Dstore.*/
										dstoreOperations.put(entry.getKey(), dstoreOperations.containsKey(entry.getKey()) 
												? dstoreOperations.get(entry.getKey()) + 1 : 1);
									});
						}
						
					} else {
						Predicate<Entry<TCPClient, Integer>> overloadedContainer = (entry) -> entry.getValue() > maxFiles 
								&& rebalanceDstoreFiles.get(entry.getKey()).contains(fileName);
						
						/**If delta is equal to 0, moves the file to an underloaded Dstore without the file, and removes the file.
						 * This operation is repeated for any overloaded Dstores that contain the file.*/
						while(dstoreFileCount.entrySet().stream().anyMatch(overloadedContainer)) {
							dstoreFileCount.entrySet().stream()
									.filter(overloadedContainer)
									.max(Entry.comparingByValue())
									.ifPresent(startpointEntry -> {
										/**Finds the smallest Dstore that doesn't contain the file.*/
										dstoreFileCount.entrySet().stream()
												.filter(endpointEntry -> endpointEntry.getValue() < maxFiles
														&& !rebalanceDstoreFiles.get(endpointEntry.getKey()).contains(fileName))
												.min(Entry.comparingByValue())
												.ifPresent(endpointEntry -> {
													/**Assigns the overloaded Dstore to send the file to the low-file Dstore.*/
													if(rebalanceStores.containsKey(startpointEntry.getKey())) {
														if(rebalanceStores.get(startpointEntry.getKey()).containsKey(fileName)) {
															rebalanceStores.get(startpointEntry.getKey()).get(fileName).add(endpointEntry.getKey());
														} else {
															rebalanceStores.get(startpointEntry.getKey()).put(fileName,
																	new Vector<>(Arrays.asList(endpointEntry.getKey())));
														}
													} else {
														rebalanceStores.put(startpointEntry.getKey(), new ConcurrentHashMap<>());
														rebalanceStores.get(startpointEntry.getKey()).put(fileName, new Vector<>(Arrays.asList(endpointEntry.getKey())));
													}
													
													/**Assigns the file to be removed from the Dstore.*/
													if(rebalanceRemovals.containsKey(startpointEntry.getKey())) {
														rebalanceRemovals.get(startpointEntry.getKey()).add(fileName);
													} else {
														rebalanceRemovals.put(startpointEntry.getKey(), new Vector<>(Arrays.asList(fileName)));
													}
													
													/**Adds the file to the low-file Dstore in the Dstore files map.*/
													rebalanceDstoreFiles.get(endpointEntry.getKey()).add(fileName);
													/**Removes the file from the overloaded Dstore in the Dstore files map.*/
													rebalanceDstoreFiles.get(startpointEntry.getKey()).remove(fileName);
													/**Increments the number of files in the low-file Dstore.*/
													endpointEntry.setValue(endpointEntry.getValue() + 1);
													/**Decrements the number of files in the overloaded Dstore.*/
													startpointEntry.setValue(startpointEntry.getValue() - 1);
													/**Increments the number of operations done by the Dstores.*/
													dstoreOperations.put(endpointEntry.getKey(), 
															dstoreOperations.containsKey(endpointEntry.getKey()) 
																	? dstoreOperations.get(endpointEntry.getKey()) + 1 : 1);
													dstoreOperations.put(startpointEntry.getKey(), 
															dstoreOperations.containsKey(startpointEntry.getKey()) 
																	? dstoreOperations.get(startpointEntry.getKey()) + 1 : 1);
												});
									});
						}
					}
				});
		
		/**Sends the rebalance requests to the Dstores and begins waiting for REBALANCE_COMPLETE acknowledgements.*/
		rebalanceWaitStart();
	}
	
	/**Begins the "Awaiting Responses" stage.*/
	public void rebalanceWaitStart() {
		rebalancePhase = RebalancePhase.WAITING_ON_ACKS;
		rebalanceAcksRequired = new Vector<>();
		
		/**Iterates over every connected Dstore and constructs the rebalance token for the Dstore.*/
		controller.getPorts().entrySet().forEach(entry -> {
			/**Stores the StringBuilder to build the rebalance message string.*/
			var rebalanceMessageBuilder = new StringBuilder("REBALANCE");
			
			/**Builds the sending portion of the rebalance string.*/
			if(rebalanceStores.containsKey(entry.getKey())) {
				/**Appends the number of files to send.*/
				rebalanceMessageBuilder.append(' ').append(rebalanceStores.get(entry.getKey()).size());
				
				/**Iterates over every file to send to other Dstores.*/
				rebalanceStores.get(entry.getKey()).forEach((fileName, dstores) -> {
					/**Appends the file name, the number of ports to send to, and the ports of the Dstores to send to.*/
					rebalanceMessageBuilder.append(' ').append(fileName).append(' ').append(dstores.size());
					dstores.forEach(dstore -> rebalanceMessageBuilder.append(' ').append(controller.getPorts().get(dstore)));
				});
			} else {
				rebalanceMessageBuilder.append(" 0");
			}
			
			/**Builds the removing portion of the rebalance string.*/
			if(rebalanceRemovals.containsKey(entry.getKey())) {
				/**Appends the number of files to remove.*/
				rebalanceMessageBuilder.append(' ').append(rebalanceRemovals.get(entry.getKey()).size());
				/**Appends the names of the files to remove.*/
				rebalanceRemovals.get(entry.getKey()).forEach(removeFileName -> rebalanceMessageBuilder.append(' ').append(removeFileName));
			} else {
				rebalanceMessageBuilder.append(" 0");
			}
			
			/**Adds the Dstore to the list of Dstores to await rebalance completion acknowledgements from.*/
			rebalanceAcksRequired.add(entry.getKey());
			
			/**Sends the rebalance request to the Dstore.*/
			try {
				entry.getKey().send(rebalanceMessageBuilder.toString());
			} catch (IOException e) {
				System.err.println("Error sending rebalance request!");
				e.printStackTrace();
			}
		});
	}
	
	/**Handles receiving messages during the "Awaiting Responses" stage.*/
	public void rebalanceWaitReceive(TCPClient dstore) {
		/**Removes the Dstore from the list of Dstores awaiting rebalance complete acknowledgements from.*/
		rebalanceAcksRequired.remove(dstore);
	}
	
	/**Handles updating during the "Awaiting Responses" stage.*/
	public void rebalanceWaitUpdate() {
		/**Checks if the rebalance acknowledgements have all been removed.*/
		if(rebalanceAcksRequired.isEmpty()) {
			/**Updates the Dstore files map with the updated Dstore files map.*/
			dstoreFiles = new ConcurrentHashMap<>();
			rebalanceDstoreFiles.forEach((address, files) -> dstoreFiles.put(address, files.stream().collect(Collectors.toList())));
			/**Updates the file Dstores map with this information.*/
			fileDstores = new ConcurrentHashMap<>();
			dstoreFiles.values().stream()
					.flatMap(List::stream)
					.distinct()
					.forEach(fileName -> {
						fileDstores.put(fileName, dstoreFiles.entrySet().stream()
								.filter(entry -> entry.getValue().contains(fileName))
								.map(Entry::getKey)
								.collect(Collectors.toList()));
					});
			
			/**Sets the last rebalance time to now.*/
			lastRebalance = System.currentTimeMillis();
			/**Resets the rebalance phase to being available for a rebalance.*/
			rebalancePhase = RebalancePhase.AVAILABLE;
			/**Sets the rebalancing flag to false.*/
			rebalancing.set(false);
			/**Unlocks the rebalance write lock.*/
			rebalanceLock.writeLock().unlock();
		}
	}
	
	public boolean exists(String fileName) {
		return state.containsKey(fileName);
	}
	
	public Set<String> list() {
		return state.keySet();
	}
	
	public int size() {
		return state.size();
	}
	
	public long getLastRebalance() {
		return lastRebalance;
	}
	
	public RebalancePhase getRebalancePhase() {
		return rebalancePhase;
	}
	
	public boolean isRebalancing() {
		return rebalancing.get();
	}
	
	public AtomicBoolean getDstoreJoinedFlag() {
		return dstoreJoinedFlag;
	}
	
	public ReentrantReadWriteLock getUpdateLock() {
		return updateLock;
	}
	
	public ReentrantReadWriteLock getRebalanceLock() {
		return rebalanceLock;
	}

	public ConcurrentHashMap<String, FileState> getState() {
		return state;
	}
	
	public ConcurrentHashMap<String, Long> getStateTimers() {
		return stateTimers;
	}

	public ConcurrentHashMap<String, Integer> getSize() {
		return size;
	}

	public ConcurrentHashMap<TCPClient, List<String>> getDstoreFiles() {
		return dstoreFiles;
	}
	
	public ConcurrentHashMap<String, List<TCPClient>> getFileDstores() {
		return fileDstores;
	}
	
	public ConcurrentHashMap<String, List<TCPClient>> getStateClients() {
		return stateClients;
	}
	
	public ConcurrentHashMap<TCPClient, ConcurrentHashMap<String, List<TCPClient>>> getLoadAttempts() {
		return loadAttempts;
	}
	
	public ConcurrentHashMap<String, AtomicInteger> getRequiredAcks() {
		return requiredAcks;
	}
	
	public Vector<TCPClient> getRebalanceAcksRequired() {
		return rebalanceAcksRequired;
	}
}