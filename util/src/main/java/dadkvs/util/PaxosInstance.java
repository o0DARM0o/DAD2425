package dadkvs.util;

public class PaxosInstance {
    private int key;
    private int acceptedValue;
    private int acceptedTimestamp;
    private int proposalTimestamp;
    private int proposalValue;

    public int getProposalValue() {
        return proposalValue;
    }

    public void setProposalValue(int proposalValue) {
        this.proposalValue = proposalValue;
    }

    public PaxosInstance(int key, int acceptedValue, int acceptedTimestamp, int proposalTimestamp, int proposalValue) {
        this.key = key;
        this.acceptedValue = acceptedValue;
        this.acceptedTimestamp = acceptedTimestamp;
        this.proposalTimestamp = proposalTimestamp;
        this.proposalValue = proposalValue;

    }

    public int getProposalTimestamp() {
        return proposalTimestamp;
    }

    public void setProposalTimestamp(int proposalTimestamp) {
        this.proposalTimestamp = proposalTimestamp;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int proposalNumber) {
        this.key = proposalNumber;
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