package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class PaxosInstance {
    private int key;
    private int acceptedValue;
    private int acceptedTimestamp;
    private int proposalTimestamp;
    private int proposalValue;
    private boolean isCommited = false;
    private boolean isReadyToCommit = false;
    private DadkvsMain.CommitRequest request;
    private StreamObserver<DadkvsMain.CommitReply> observer;
    private int index;



    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public StreamObserver<DadkvsMain.CommitReply> getObserver() {
        return observer;
    }

    public void setObserver(StreamObserver<DadkvsMain.CommitReply> observer) {
        this.observer = observer;
    }

    public DadkvsMain.CommitRequest getRequest() {
        return request;
    }

    public void setRequest(DadkvsMain.CommitRequest request) {
        this.request = request;
    }

    public boolean isReadyToCommit() {
        return isReadyToCommit;
    }

    public void setReadyToCommit(boolean isReadyToCommit) {
        this.isReadyToCommit = isReadyToCommit;
    }

    public boolean isCommited() {
        return isCommited;
    }

    public void setCommited(boolean isCommited) {
        this.isCommited = isCommited;
    }

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