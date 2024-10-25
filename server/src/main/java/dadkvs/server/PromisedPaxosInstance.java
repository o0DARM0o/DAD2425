package dadkvs.server;

import java.util.List;
import java.util.Map.Entry;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.PhaseOneReply;
import dadkvs.DadkvsPaxos.PhaseTwoReply;
import dadkvs.DadkvsPaxos.PhaseTwoRequest;
import dadkvs.DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class PromisedPaxosInstance extends PaxosInstance {

	private final TimeOutManager timeOutManager;

	private static final GenericPaxosFunctions
			<DadkvsPaxos.PhaseTwoRequest, DadkvsPaxos.PhaseTwoReply> paxosFunctions =
			new GenericPaxosFunctions<>() {

		@Override
		public void doPaxosPhase(PhaseTwoRequest request,
				StreamObserver<PhaseTwoReply> responseObserver, DadkvsPaxosServiceStub stub) {

			stub.phasetwo(request, responseObserver);
		}

		@Override
		public boolean getResponseBoolean(PhaseTwoReply response) {
			return response.getPhase2Accepted();
		}
	};

	private final GenericPaxosMessagesHandler
			<DadkvsPaxos.PhaseTwoRequest, DadkvsPaxos.PhaseTwoReply> paxosMessagesHandler;


	PromisedPaxosInstance(PrePreparePaxosInstance successfulPrePreparePaxosInstance,
			List<PhaseOneReply> phaseOneReplies) {

		super(decideValue(successfulPrePreparePaxosInstance.paxosValue, phaseOneReplies));
		this.timeOutManager = new TimeOutManager();

		paxosMessagesHandler = new GenericPaxosMessagesHandler<>(
				paxosFunctions,
				timeOutManager.getTimeoutMilis(),
				false
		);
	}

	private PromisedPaxosInstance(PromisedPaxosInstance timedOutPromisedPaxosInstance) {

		super(timedOutPromisedPaxosInstance.paxosValue.incrementProposalNumber());
		this.timeOutManager = timedOutPromisedPaxosInstance.timeOutManager;

		final boolean reject_timeout = timeOutManager.increaseTimeOut();

		paxosMessagesHandler = new GenericPaxosMessagesHandler<>(
				paxosFunctions,
				timeOutManager.getTimeoutMilis(),
				reject_timeout
		);
	}

	private static PaxosValue decideValue(PaxosValue myPaxosValue,
			List<PhaseOneReply> phaseOneReplies) {
		
		final PaxosValue potentialProposedValue =
				getPhaseOneRepliesProposedValue(phaseOneReplies);

		final boolean otherValuesWhereProposed = potentialProposedValue != null;

		if (otherValuesWhereProposed) {
			return potentialProposedValue;
		}
		return myPaxosValue;
	}
	
	private static PaxosValue getPhaseOneRepliesProposedValue(
			List<PhaseOneReply> phaseOneReplies) {

		PaxosValue mostRecentProposedPaxosValue = null;
		for (PhaseOneReply phaseOneReply : phaseOneReplies) {
			final PaxosValue potentialProposedValue = new PaxosValue(
				phaseOneReply.getPhase1Value(),
				phaseOneReply.getHighestProposalVector()
			);

			final boolean otherValueProposed = potentialProposedValue.tr != null;
			if (otherValueProposed) {
				mostRecentProposedPaxosValue = PaxosValue.getMoreRecentValue(
						mostRecentProposedPaxosValue,
						potentialProposedValue
				);
			}
		}
		return mostRecentProposedPaxosValue;
	}

	PaxosInstance sendAcceptRequests(ManagedChannel[] managedChannels) {
		final DadkvsPaxos.PhaseTwoRequest phaseTwoRequest = createPhaseTwoRequest();

		final Entry<MessageResultEnum, List<PhaseTwoReply>> resultTuple =
				paxosMessagesHandler.sendRequestsToServers(managedChannels, phaseTwoRequest);

		switch (resultTuple.getKey()) {
			case MessageResultEnum.ACCEPTED:
				return new AcceptedPaxosInstance(this, resultTuple.getValue());
			case MessageResultEnum.REJECTED:
				return new RejectedPaxosInstance(this);
			case MessageResultEnum.TIMED_OUT:
				return new PromisedPaxosInstance(this);
			default:
				System.out.println("[sendPrepareRequests]: Something went wrong");
				return new RejectedPaxosInstance(this);
		}
	}

	private PhaseTwoRequest createPhaseTwoRequest() {
		return DadkvsPaxos.PhaseTwoRequest.newBuilder()
			.setPhase2Config(-1) // Not Implemented
			.setProposalVector(paxosValue.proposal_vector)
			.setPhase2Value(super.paxosValue.tr)
			.build();
	}
}
