
package dadkvs.server;


import java.util.Map.Entry;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.LearnReply;
import dadkvs.DadkvsPaxos.PhaseOneReply;
import dadkvs.DadkvsPaxos.PhaseTwoReply;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.DebugMode;
import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

	DadkvsServerState server_state;
	final PaxosManager paxosManager;
	
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
	public void phaseone(DadkvsPaxos.PhaseOneRequest request,
			StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {

		checkFreeze(); // Check if server is frozen before processing the request

		final PhaseOneReply response = paxosManager.getPhaseOneReply(request);
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}


	@Override
	public void phasetwo(DadkvsPaxos.PhaseTwoRequest request,
			StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {

		checkFreeze(); // Check if server is frozen before processing the request

		final Entry<PhaseTwoReply, PaxosValue> responseTuple =
				paxosManager.getPhaseTwoReply(request);

		if (responseTuple.getKey().getPhase2Accepted()) {
			paxosManager.sendLearnRequests(responseTuple.getValue());
		}
		responseObserver.onNext(responseTuple.getKey());
		responseObserver.onCompleted();
	}


@Override
public void learn(DadkvsPaxos.LearnRequest request,
		StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {

	checkFreeze(); // Check if server is frozen before processing the request
	
	final LearnReply response = paxosManager.getLearnReply(request);
	responseObserver.onNext(response);
	responseObserver.onCompleted();
}


	// Handle heartbeat request from leader
	@Override
	public void heartbeat(DadkvsPaxos.HeartbeatRequest request,
			StreamObserver<DadkvsPaxos.HeartbeatReply> responseObserver) {

		checkFreeze(); // Check if server is frozen before processing the request

		int leaderId = request.getLeaderId();
		// Update the server state with the received heartbeat
		this.server_state.handleHeartbeat(leaderId);

		DadkvsPaxos.HeartbeatReply reply = 
				DadkvsPaxos.HeartbeatReply.newBuilder().setAck(true).build();

		responseObserver.onNext(reply);
		responseObserver.onCompleted();
	}

	public void executeDebugMode(DebugMode debugMode) {
		if (debugMode == DebugMode.UN_FREEZE) {
			unfreeze(); // Call unfreeze when the mode is set to UN_FREEZE
		}
	}
}
