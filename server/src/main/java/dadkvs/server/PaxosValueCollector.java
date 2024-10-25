package dadkvs.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PaxosValueCollector  {
	private final KeyValueStore keyValueStore;

	private final Map<Integer, PaxosValue> collectedPaxosValues = new ConcurrentHashMap<>();
	private final Map<Integer, Boolean> wasPaxosSuccessful = new ConcurrentHashMap<>();

	private final AtomicInteger next_paxos_index = new AtomicInteger(0);

	private final Map<Integer, Object> locks = new ConcurrentHashMap<>();

	public PaxosValueCollector(DadkvsServerState serverState) {
		this.keyValueStore = serverState.store;

		Thread thread = new Thread(() -> handleTransactions());
		thread.start();
	}

	synchronized public void addPaxosValue(PaxosValue new_paxos_value) {
		if (new_paxos_value == null) {
			System.err.println("[addPaxosValue]: Expected not null paxos value");
			return;
		}
		final int paxos_index = new_paxos_value.proposal_vector.getPaxosIndex(); 
		if (collectedPaxosValues.putIfAbsent(paxos_index, new_paxos_value) != null) {
			System.err.println(
					"[addPaxosValue]: Expected no paxos value entry with key " + paxos_index);
		}
		if (locks.putIfAbsent(paxos_index, new Object()) != null) {
			System.err.println("[addPaxosValue]: Expected no lock entry with key " + paxos_index);
		}
		System.out.println(paxosValueCollectorToString());
		notify();
	}

	private String paxosValueCollectorToString() {
		if (collectedPaxosValues == null || collectedPaxosValues.isEmpty()) {
			return "CollectedPaxosValues{}";
		}
		// Find the maximum key in the map to determine the range to iterate
		final int maxKey = collectedPaxosValues
				.keySet().stream().max(Integer::compareTo).orElse(0);

		final StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append("CollectedPaxosValues{\n");
		for (int i = 0; i <= maxKey; i++) {
			stringBuilder.append('\t');
			if (collectedPaxosValues.containsKey(i)) {
				stringBuilder.append(collectedPaxosValues.get(i));
			} else {
				stringBuilder.append("<empty>");
			}
			if (i < maxKey) {  // Add a separator between values
				stringBuilder.append(',');
			}
			stringBuilder.append('\n');
		}
		stringBuilder.append('}');

		return stringBuilder.toString();
	}

	synchronized public void handleTransactions() {
		while (true) {

			int paxos_index = next_paxos_index.get();
			while (collectedPaxosValues.containsKey(paxos_index)) {

				final boolean was_transaction_successful =
						keyValueStore.commit(collectedPaxosValues.get(paxos_index).tr);

				wasPaxosSuccessful.put(paxos_index, was_transaction_successful);

				synchronized (locks.get(paxos_index)) {
					locks.get(paxos_index).notify();
				}

				paxos_index = next_paxos_index.incrementAndGet();
			}

			try {
				wait();
			} catch (InterruptedException e) {
				System.err.println("[handleTransactions]: Something went wrong");
			}
		}
	}

	public boolean waitForCommit(int paxos_index) {
		if (wasPaxosSuccessful.containsKey(paxos_index)) {
			return wasPaxosSuccessful.get(paxos_index);
		}
		try {
			synchronized (locks.get(paxos_index)) {
				locks.get(paxos_index).wait();
			}
			if (wasPaxosSuccessful.containsKey(paxos_index)) {
				return wasPaxosSuccessful.get(paxos_index);
			}
			System.err.println("[waitForCommit]: Expected to have result boolean after wait");
			return false;
		} catch (InterruptedException e) {
			System.err.println("[waitForCommit]: InterruptedException not expected");
			return false;
		}
	} 
}
