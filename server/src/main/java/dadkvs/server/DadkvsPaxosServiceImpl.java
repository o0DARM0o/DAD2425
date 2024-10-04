
package dadkvs.server;


import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.DebugMode;
import dadkvs.util.PaxosInstance;
import dadkvs.util.PaxosManager;
import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;
    PaxosManager paxosManager;
    
	private final Object freezeLock = new Object(); // Lock object for freeze/unfreeze mechanism
    
    public DadkvsPaxosServiceImpl(DadkvsServerState state, PaxosManager paxosManager) {
        this.server_state = state;
		this.server_state.paxosServiceImpl = this;
        this.paxosManager = paxosManager;
	
    }
    
	private void checkFreeze() {
		synchronized (freezeLock) {
			while (server_state.new_debug_mode == DebugMode.FREEZE) {
				try {
					System.out.println("Server is in FREEZE mode. Pausing request handling...");
					freezeLock.wait(); // Wait until UN_FREEZE is called
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt(); // Reset thread interrupt status
				}
			}
		}
    }

	private void unfreeze() {
		synchronized (freezeLock) {
			System.out.println("Server is in UN_FREEZE mode. Resuming request handling...");
			freezeLock.notifyAll(); // Notify all waiting threads
		}
	}

    @Override
public void phaseone(DadkvsPaxos.PhaseOneRequest request, StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
	checkFreeze(); // Check if server is frozen before processing the request
	System.out.println("Receive phase1 request: " + request);

    int proposedIndex = request.getPhase1Index();  // Proposal number n
    int promisedIndex = server_state.promisedIndex;  // Highest promised proposal number
    int acceptedProposalNumber = server_state.acceptedProposalNumber; // Highest accepted proposal number
    int acceptedValue = server_state.acceptedValue; // Value of highest accepted proposal

    DadkvsPaxos.PhaseOneReply.Builder response = DadkvsPaxos.PhaseOneReply.newBuilder()
        .setPhase1Config(request.getPhase1Config())
        .setPhase1Index(proposedIndex);

    if (proposedIndex > promisedIndex) {
        // Update promisedIndex
        server_state.promisedIndex = proposedIndex;
        response.setPhase1Accepted(true);

        if (acceptedProposalNumber != -1) {
            // Include highest accepted proposal number and value
            response.setPhase1Timestamp(acceptedProposalNumber)
                    .setPhase1Value(acceptedValue);
        }
    } else {
        response.setPhase1Accepted(false);
    }

    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
}


@Override
public void phasetwo(DadkvsPaxos.PhaseTwoRequest request, StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
	checkFreeze(); // Check if server is frozen before processing the request
	// for debug purposes
    System.out.println("Receive phase two request: " + request);
    int proposedIndex = request.getPhase2Index(); // Proposal number n
    int value = request.getPhase2Value();
    int promisedIndex = server_state.promisedIndex;

    DadkvsPaxos.PhaseTwoReply.Builder response = DadkvsPaxos.PhaseTwoReply.newBuilder()
        .setPhase2Config(request.getPhase2Config())
        .setPhase2Index(proposedIndex);

    if (proposedIndex == promisedIndex) {
        // Accept the proposal
        server_state.acceptedProposalNumber = proposedIndex;
        server_state.acceptedValue = value;

        response.setPhase2Accepted(true);
    } else {
        response.setPhase2Accepted(false);
    }

    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
}


@Override
public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
	checkFreeze(); // Check if server is frozen before processing the request
	System.out.println("Receive learn request: " + request);
    int learnedIndex = request.getLearnindex();
    int value = request.getLearnvalue();
    int timestamp = request.getLearntimestamp();

    // Update the accepted value
    server_state.acceptedProposalNumber = learnedIndex;
    server_state.acceptedValue = value;

    DadkvsPaxos.LearnReply response = DadkvsPaxos.LearnReply.newBuilder()
        .setLearnconfig(request.getLearnconfig())
        .setLearnindex(learnedIndex)
        .setLearnaccepted(true)
        .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
}


    // Handle heartbeat request from leader
    @Override
    public void heartbeat(DadkvsPaxos.HeartbeatRequest request, StreamObserver<DadkvsPaxos.HeartbeatReply> responseObserver) {
        checkFreeze(); // Check if server is frozen before processing the request

		int leaderId = request.getLeaderId();
        System.out.println("Received heartbeat from leader: " + leaderId);
        this.server_state.handleHeartbeat(leaderId);  // Update the server state with the received heartbeat

        DadkvsPaxos.HeartbeatReply reply = DadkvsPaxos.HeartbeatReply.newBuilder().setAck(true).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

	public void executeDebugMode(DebugMode debugMode) {
		if (debugMode == DebugMode.UN_FREEZE) {
			unfreeze(); // Call unfreeze when the mode is set to UN_FREEZE
		}
	}
}
