package dadkvs.server;

import java.util.List;
import java.util.Map.Entry;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.PhaseOneReply;
import dadkvs.DadkvsPaxos.PhaseOneRequest;
import dadkvs.DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class PrePreparePaxosInstance extends PaxosInstance {
	final TimeOutManager timeOutManager;

	private static final GenericPaxosFunctions
			<DadkvsPaxos.PhaseOneRequest, DadkvsPaxos.PhaseOneReply> paxosFunctions =
			new GenericPaxosFunctions<>() {

		@Override
		public void doPaxosPhase(PhaseOneRequest request,
				StreamObserver<PhaseOneReply> responseObserver, DadkvsPaxosServiceStub stub) {

			stub.phaseone(request, responseObserver);
		}

		@Override
		public boolean getResponseBoolean(PhaseOneReply response) {
			return response.getPhase1Accepted();
		}
	}; 

	private final GenericPaxosMessagesHandler
			<DadkvsPaxos.PhaseOneRequest, DadkvsPaxos.PhaseOneReply> paxosMessagesHandler;


	PrePreparePaxosInstance(PaxosValue paxosValue) {
		super(paxosValue);
		timeOutManager = new TimeOutManager();

		paxosMessagesHandler = new GenericPaxosMessagesHandler<>(
				paxosFunctions,
				timeOutManager.getTimeoutMilis(),
				false
		);
	}

	

	private PrePreparePaxosInstance(PrePreparePaxosInstance timedOutPrePreparePaxosInstance) {

		super(timedOutPrePreparePaxosInstance.paxosValue.incrementProposalNumber());
		this.timeOutManager = timedOutPrePreparePaxosInstance.timeOutManager;

		final boolean reject_timeout = !timeOutManager.increaseTimeOut();

		paxosMessagesHandler = new GenericPaxosMessagesHandler<>(
				paxosFunctions,
				timeOutManager.getTimeoutMilis(),
				reject_timeout
		);
	}

	PaxosInstance sendPrepareRequests(ManagedChannel[] managedChannels) {
		final DadkvsPaxos.PhaseOneRequest phaseOneRequest = createPhaseOneRequest();

		final Entry<MessageResultEnum, List<PhaseOneReply>> resultTuple =
				paxosMessagesHandler.sendRequestsToServers(managedChannels, phaseOneRequest);

		switch (resultTuple.getKey()) {
			case MessageResultEnum.ACCEPTED:
				return new PromisedPaxosInstance(this, resultTuple.getValue());
			case MessageResultEnum.REJECTED:
				return new RejectedPaxosInstance(this);
			case MessageResultEnum.TIMED_OUT:
				return new PrePreparePaxosInstance(this);
			default:
				System.out.println("[sendPrepareRequests]: Something went wrong");
				return new RejectedPaxosInstance(this);
		}
	}

	private DadkvsPaxos.PhaseOneRequest createPhaseOneRequest() {
		return DadkvsPaxos.PhaseOneRequest.newBuilder()
			.setPhase1Config(-1) // Not Implemented
			.setProposalVector(paxosValue.proposal_vector)
			.build();
	}
}
