package dadkvs.server;

import java.util.ArrayList;
import java.util.List;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.LearnRequest;
import dadkvs.DadkvsPaxos.TransactionRecord;
import dadkvs.DadkvsPaxos.LearnReply;
import dadkvs.DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class LearnPaxosManager {
	private final int majority;

	private final PaxosValueCollector paxosValueCollector;

	private final PaxosValue myPaxosValue;

	private final TimeOutManager timeOutManager = new TimeOutManager();

	private final List<PaxosValue> learnRequests = new ArrayList<>();

	private final Object getAcceptRequestReplyLock = new Object();

	private static final GenericPaxosFunctions
			<DadkvsPaxos.LearnRequest, DadkvsPaxos.LearnReply> paxosFunctions =
			new GenericPaxosFunctions<>() {

		@Override
		public void doPaxosPhase(LearnRequest request,
				StreamObserver<LearnReply> responseObserver, DadkvsPaxosServiceStub stub) {

			stub.learn(request, responseObserver);
		}

		@Override
		public boolean getResponseBoolean(LearnReply response) {
			return response.getLearnaccepted();
		}
	};

	private GenericPaxosMessagesHandler
			<DadkvsPaxos.LearnRequest, DadkvsPaxos.LearnReply> paxosMessagesHandler;

	LearnPaxosManager(PaxosValueCollector paxosValueCollector, PaxosValue paxosValue) {
		this.majority = getMajority();
		this.paxosValueCollector = paxosValueCollector;
		this.myPaxosValue = paxosValue;

		this.paxosMessagesHandler = new GenericPaxosMessagesHandler<>(
				paxosFunctions,
				timeOutManager.getTimeoutMilis(),
				false
		);
	}

	private static int getMajority() {
		return DadkvsServer.TOTAL_SERVERS / 2 + 1;
	}

	synchronized boolean sendLearnRequests(ManagedChannel[] managedChannels) {
		final DadkvsPaxos.LearnRequest learnRequest = createLearnRequest();
		MessageResultEnum result = MessageResultEnum.TIMED_OUT;
		while (true) {
			result = paxosMessagesHandler
					.sendRequestsToServers(managedChannels, learnRequest).getKey();

			if (result != MessageResultEnum.TIMED_OUT) {
				break;
			} else {
				updatePaxosMessagesHandler();
			}
		}
		return result == MessageResultEnum.ACCEPTED; 
	}

	private void updatePaxosMessagesHandler() {
		final boolean reject_timeout = timeOutManager.increaseTimeOut();
		this.paxosMessagesHandler = new GenericPaxosMessagesHandler<>(
				paxosFunctions,
				timeOutManager.getTimeoutMilis(),
				reject_timeout
		);
	}

	private LearnRequest createLearnRequest() {
		return DadkvsPaxos.LearnRequest.newBuilder()
			.setLearnconfig(-1) // Not Implemented
			.setProposalVector(myPaxosValue.proposal_vector)
			.setLearnvalue(myPaxosValue.tr)
			.build();
	}

	LearnReply getLearnRequestReply(LearnRequest learnRequest) {
		synchronized (getAcceptRequestReplyLock) {
			final PaxosValue requestPaxosValue = new PaxosValue(learnRequest.getLearnvalue(),
					learnRequest.getProposalVector());

			updateLearnRequests(requestPaxosValue);
			return createLearnRequestReply(myPaxosValue);
		}
	}

	private void updateLearnRequests(PaxosValue requestPaxosValue) {
		learnRequests.add(requestPaxosValue);
		if (learnRequests.size() == majority) {
			final TransactionRecord myTransactionRecord = myPaxosValue.tr;
			boolean are_equal = true;
			for (PaxosValue otherPaxosValue : learnRequests) {
				are_equal = TransactionRecordUtils.areTransactionsEqual(
						myTransactionRecord, 
						otherPaxosValue.tr
				);
			}
			if (!are_equal) {
				System.err.println("[updateLearnRequests]: Expected The same transaction");
			}
			paxosValueCollector.addPaxosValue(learnRequests.get(0));
		}
	}

	private static LearnReply createLearnRequestReply(PaxosValue paxosValue) {
		return DadkvsPaxos.LearnReply.newBuilder()
			.setLearnconfig(-1) // Not Implemented
			.setLearnedProposalVector(paxosValue.proposal_vector)
			.setLearnaccepted(true)
			.build();
	}
}
