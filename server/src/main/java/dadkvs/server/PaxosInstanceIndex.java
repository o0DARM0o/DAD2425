package dadkvs.server;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PaxosInstanceIndex {
	final AtomicInteger paxosInstanceIndex;
	private final Map<Integer, ReplicaPaxosManager> replicaPaxosManagers;

	PaxosInstanceIndex(AtomicInteger paxosInstanceIndex,
			Map<Integer, ReplicaPaxosManager> replicaPaxosManagers) {

		this.paxosInstanceIndex = paxosInstanceIndex;
		this.replicaPaxosManagers = replicaPaxosManagers;
	}

	int get() {
		return paxosInstanceIndex.get();
	}

	synchronized void setIfHigherPaxosInstanceIndex(int new_paxos_instance_index) {
		int current_paxos_instance_index = get();
		if (current_paxos_instance_index < 0) {
			current_paxos_instance_index = 0;
		}
		if (current_paxos_instance_index < new_paxos_instance_index) {
			for (int i = current_paxos_instance_index; i <= new_paxos_instance_index; i++) {
				replicaPaxosManagers.putIfAbsent(i, new ReplicaPaxosManager());
			}
		}
	}
	

	synchronized int incrementAndGet() {
		final int new_paxos_instance_index = paxosInstanceIndex.incrementAndGet();
		if (new_paxos_instance_index >= 0) {
			replicaPaxosManagers.putIfAbsent(new_paxos_instance_index, new ReplicaPaxosManager());
		}
		return new_paxos_instance_index;
	}
}
