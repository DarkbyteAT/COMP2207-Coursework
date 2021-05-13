public enum RebalancePhase {
	AVAILABLE, ACQUIRING_LIST, FILE_ALLOCATION, WAITING_ON_ACKS;
	
	/**Returns if a message from a Dstore should be handled during a given rebalance phase.*/
	public static boolean shouldLock(String token, RebalancePhase phase) {
		return switch(phase) {
			case ACQUIRING_LIST  -> !token.equals(Protocol.LIST_TOKEN);
			case WAITING_ON_ACKS -> !token.equals(Protocol.REBALANCE_COMPLETE_TOKEN);
			default				 -> true;
		};
	}
}