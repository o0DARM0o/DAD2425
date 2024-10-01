package dadkvs.util;

import java.util.HashMap;
import java.util.Map;

public class PaxosInstance {
    private int proposalNumber;
    private int acceptedValue;
    private int acceptedTimestamp;

    public PaxosInstance(int proposalNumber, int acceptedValue, int acceptedTimestamp) {
        this.proposalNumber = proposalNumber;
        this.acceptedValue = acceptedValue;
        this.acceptedTimestamp = acceptedTimestamp;
    }

    public int getProposalNumber() {
        return proposalNumber;
    }

    public void setProposalNumber(int proposalNumber) {
        this.proposalNumber = proposalNumber;
    }

    public int getAcceptedValue() {
        return acceptedValue;
    }

    public void setAcceptedValue(int acceptedValue) {
        this.acceptedValue = acceptedValue;
    }

    public int getAcceptedTimestamp() {
        return acceptedTimestamp;
    }

    public void setAcceptedTimestamp(int acceptedTimestamp) {
        this.acceptedTimestamp = acceptedTimestamp;
    }

}

class PaxosManager {
    private Map<Integer, PaxosInstance> instances; // key: index, value: PaxosInstance

    public PaxosManager() {
        this.instances = new HashMap<>();
    }

    public void startPaxosInstance(int index, int proposalNumber) {
        // Create a new Paxos instance or retrieve existing one
        PaxosInstance instance = instances.getOrDefault(index, new PaxosInstance(proposalNumber, -1, -1));
        instances.put(index, instance);
        
        // Logic to handle proposing values, accepting, etc.
    }

    public PaxosInstance getPaxosInstance(int index) {
        return instances.get(index);
    }

    // Additional methods for managing Paxos logic
    // ...
}
