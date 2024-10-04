package dadkvs.util;

import java.util.HashMap;
import java.util.Map;


 public class PaxosManager {
    private Map<Integer, PaxosInstance> instances; // key: index, value: PaxosInstance

    public PaxosManager() {
        this.instances = new HashMap<>();
    }

    public void startPaxosInstance(int index, int key, int proposalTimestamp, int proposalValue) {
        // Create a new Paxos instance or retrieve existing one
        PaxosInstance instance = instances.getOrDefault(index, new PaxosInstance(key, -1, -1, proposalTimestamp, proposalValue));
        instances.put(index, instance);
        
        // Logic to handle proposing values, accepting, etc.
    }

    public PaxosInstance getPaxosInstance(int index) {
        return instances.get(index);
    }

    // Additional methods for managing Paxos logic
    // ...
}

