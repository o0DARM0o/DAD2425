package dadkvs.server;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsPaxos.LearnReply;
import dadkvs.DadkvsPaxos.LearnRequest;
import dadkvs.DadkvsPaxos.PhaseOneReply;
import dadkvs.DadkvsPaxos.PhaseOneRequest;
import dadkvs.DadkvsPaxos.PhaseTwoReply;
import dadkvs.DadkvsPaxos.PhaseTwoRequest;
import dadkvs.DadkvsPaxos.ProposalVector;
import dadkvs.DadkvsPaxos.TransactionRecord;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class PaxosManager {
	final private ManagedChannel[] channels;

	final private PaxosInstanceIndex paxosInstanceIndex;
	final private AtomicInteger my_current_id;

	final private PaxosValueCollector paxosValueCollector;

	final private Object startPaxosInstanceLock = new Object();
	final private Object waitForCommitLock = new Object();
	final private Object updateLearnRequestsLock = new Object();

	final private Map<Integer, ReplicaPaxosManager> replicaPaxosManagers =
		new ConcurrentHashMap<>();

	final private Map<Integer, LearnPaxosManager> learnPaxosManagers =
		new ConcurrentHashMap<>();

	public PaxosManager(DadkvsServerState serverState) {
		this.channels = initializeChannels(serverState.base_port);

		this.paxosInstanceIndex = new PaxosInstanceIndex(new AtomicInteger(-1),
				replicaPaxosManagers);

		this.my_current_id = serverState.my_current_id;
		this.paxosValueCollector = new PaxosValueCollector(serverState);
	}

	private static ManagedChannel[] initializeChannels(int base_port) {
		final ManagedChannel[] managedChannels = new ManagedChannel[DadkvsServer.TOTAL_SERVERS];
		for (int i = 0; i < DadkvsServer.TOTAL_SERVERS; i++) {
			try {
				managedChannels[i] = ManagedChannelBuilder
						.forAddress("localhost", base_port + i).usePlaintext().build(); 

			} catch (Exception e) {
				System.err.println(
						"[PaxosManager]: Error initializing channel " + i + ": " + e.getMessage());
			}
		}
		return managedChannels;
	}

	public int startPaxosInstance(TransactionRecord tr) {
		synchronized (startPaxosInstanceLock) {
			final int newPaxosInstanceIndex = paxosInstanceIndex.incrementAndGet();
			final ProposalVector proposal_vector = ProposalVectorUtils.createProposalVector(
					my_current_id.get(),
					newPaxosInstanceIndex,
					0
			);

			final PaxosInstance final_paxos_instance = runPaxosStateMachine(tr, proposal_vector);

			if (final_paxos_instance instanceof RejectedPaxosInstance) {
				paxosValueCollector.addPaxosValue(final_paxos_instance.paxosValue);
			}
			return newPaxosInstanceIndex;
		}
	}

	private PaxosInstance runPaxosStateMachine(TransactionRecord tr,
			ProposalVector proposal_vector) {

		PaxosInstance paxosInstance = new PrePreparePaxosInstance(
				new PaxosValue(tr, proposal_vector));

		while (!(paxosInstance instanceof AcceptedPaxosInstance ||
				paxosInstance instanceof RejectedPaxosInstance)) {

			if (paxosInstance instanceof PrePreparePaxosInstance prePreparePaxosInstance) {
				paxosInstance = prePreparePaxosInstance.sendPrepareRequests(channels);
			} else if (paxosInstance instanceof PromisedPaxosInstance promisedPaxosInstance) {
				paxosInstance = promisedPaxosInstance.sendAcceptRequests(channels);
			} else {
				System.err.println("[startPaxosInstance]: something went wrong");
				paxosInstance = new RejectedPaxosInstance(paxosInstance);
			}
		}
		return paxosInstance;
	}

	public boolean waitForCommit(int paxos_instance_index) {
		synchronized (waitForCommitLock) {
			return paxosValueCollector.waitForCommit(paxos_instance_index);
		}
	}

	public int getPaxosInstanceIndex() {
		return paxosInstanceIndex.get();
	}

	public PhaseOneReply getPhaseOneReply(PhaseOneRequest prepareRequest) {
		final int new_instance_index = prepareRequest.getProposalVector().getPaxosIndex();
		paxosInstanceIndex.setIfHigherPaxosInstanceIndex(new_instance_index);
		return replicaPaxosManagers.get(new_instance_index).getPrepareRequestReply(prepareRequest);
	}

	public Entry<PhaseTwoReply, PaxosValue> getPhaseTwoReply(PhaseTwoRequest acceptRequest) {
		final int new_instance_index = acceptRequest.getProposalVector().getPaxosIndex();
		paxosInstanceIndex.setIfHigherPaxosInstanceIndex(new_instance_index);
		return replicaPaxosManagers.get(new_instance_index).getAcceptRequestReply(acceptRequest);
	}

	public LearnReply getLearnReply(LearnRequest learnRequest) {
		final PaxosValue new_paxos_value = new PaxosValue(
				learnRequest.getLearnvalue(), learnRequest.getProposalVector());

		final int new_paxos_instance_index = new_paxos_value.proposal_vector.getPaxosIndex();
		paxosInstanceIndex.setIfHigherPaxosInstanceIndex(new_paxos_instance_index);
		updateLearnRequests(new_paxos_value);
		return learnPaxosManagers.get(new_paxos_instance_index).getLearnRequestReply(learnRequest);
	}

	private void updateLearnRequests(PaxosValue new_paxos_value) {
		synchronized (updateLearnRequestsLock) {
			final int new_paxos_instance_index = new_paxos_value.proposal_vector.getPaxosIndex();
			int current_paxos_instance_index = paxosInstanceIndex.get();
			if (current_paxos_instance_index < 0) {
				current_paxos_instance_index = 0;
			}
			if (current_paxos_instance_index < new_paxos_instance_index) {
				for (int i = current_paxos_instance_index; i <= new_paxos_instance_index; i++) {
					learnPaxosManagers.putIfAbsent(i, new LearnPaxosManager(
							paxosValueCollector, new_paxos_value));
				}
			}
		}
	}

	public void sendLearnRequests(PaxosValue new_paxos_value) {
		final int new_paxos_instance_index = new_paxos_value.proposal_vector.getPaxosIndex();
		paxosInstanceIndex.setIfHigherPaxosInstanceIndex(new_paxos_instance_index);
		updateLearnRequests(new_paxos_value);

		Thread thread = new Thread(() ->
				learnPaxosManagers.get(new_paxos_instance_index).sendLearnRequests(channels));
		thread.start();
	}
}
