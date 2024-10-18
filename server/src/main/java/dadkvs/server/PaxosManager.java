package dadkvs.server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;


 public class PaxosManager {

    private Map<Integer, PaxosInstance> instances;
    private int commitIndex;  // Tracks the next instance that can be committed
    private final Object commitLock = new Object();  // Lock to ensure ordered commits
    private DadkvsMainServiceImpl service_impl;
    private DadkvsServerState server_state;

	public DadkvsServerState getServer_state() {
		return server_state;
	}

	public void setServer_state(DadkvsServerState server_state) {
		this.server_state = server_state;
	}

	public void setService_impl(DadkvsMainServiceImpl service_impl) {
        this.service_impl = service_impl;
    }

    public PaxosManager() {
        this.instances = new ConcurrentHashMap<>();
        this.commitIndex = 0;
    }
 
    public void startPaxosInstance(int index, int key, int proposalTimestamp, int proposalValue) {
        // Create a new Paxos instance or retrieve existing one
        PaxosInstance instance = instances.getOrDefault(index, new PaxosInstance(
				key, -1, -1, proposalTimestamp, proposalValue));

        instances.put(index, instance);
        
    }

    public void incrementCommitedIndex () {
        this.commitIndex++;
    }

    public PaxosInstance getPaxosInstance(int index) {
        return instances.get(index);
    }

    // Mark an instance as "ready to commit"
    public synchronized void markAsReadyToCommit(int instanceId) {
        PaxosInstance instance = instances.get(instanceId);
        if (instance != null) {
            instance.setReadyToCommit(true);  // Mark the instance as ready
        }
    }

    // Check if the instance can commit (i.e., it's the next one in order)
    public boolean canCommit(int instanceId) {
        return instanceId == commitIndex+1;
    }

    public void removeInstance(int index) {
        instances.remove(index);
    }

    public int instancesSize() {
        return instances.size();
    }

     // Method to commit the instance and update commitIndex
     public synchronized void commitInstance(int instanceId) {
        if (canCommit(instanceId)) {
            PaxosInstance instance = instances.get(instanceId);
            if (instance != null && instance.isReadyToCommit()) {
                // Commit logic
                System.out.println("Committing instance: " + instanceId);

                this.service_impl.doCommit(
						instanceId,
						instance.getRequest(),
						instance.getObserver(),
						(service_impl.server_state.n_servers / 2) + 1,
						instance.getAcceptedTimestamp()
				);

                // Update commit index
                instance.setCommited(true);
                commitIndex++;
            }
        }
    }

    // Commit thread that constantly checks for ready-to-commit instances
    public void startCommitThread() {
        new Thread(() -> {
            System.out.println("STARTED COMMIT THREAD!!!");
            while (this.server_state.i_am_leader.get()) {
                synchronized (commitLock) {
                    for (Iterator<Map.Entry<Integer, PaxosInstance>> it = 
							instances.entrySet().iterator(); it.hasNext();) {

                        Map.Entry<Integer, PaxosInstance> entry = it.next();
                        PaxosInstance instance = entry.getValue();
                        if(!instance.isCommited()) {

                            this.service_impl.handleLeaderRole(
									instance.getIndex(),
									instance.getRequest(),
									instance.getObserver()
							);

                            commitInstance(instance.getIndex());
                            it.remove();
                        }
                    }
                }
                try {
                    Thread.sleep(100);  // Sleep to avoid busy waiting
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();
    }
    
}

