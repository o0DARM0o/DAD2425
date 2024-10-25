package dadkvs.server;

import java.util.AbstractMap;
import java.util.Map.Entry;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.PhaseOneReply;
import dadkvs.DadkvsPaxos.PhaseOneRequest;
import dadkvs.DadkvsPaxos.PhaseTwoReply;
import dadkvs.DadkvsPaxos.PhaseTwoRequest;
import dadkvs.DadkvsPaxos.ProposalVector;

public class ReplicaPaxosManager {
	private final PaxosValueCollector paxosValueCollector;

	private ProposalVector prepareHighestProposalVector = null;
	private ProposalVector promisedHighestProposalVector = null;
	private PaxosValue mostRecentPaxosValue = null;

	ReplicaPaxosManager(PaxosValueCollector paxosValueCollector) {
		this.paxosValueCollector = paxosValueCollector;
	}

	synchronized PhaseOneReply getPrepareRequestReply(PhaseOneRequest prepareRequest) {
		final ProposalVector new_proposal_vector = prepareRequest.getProposalVector();
		if (alreadyLearned(new_proposal_vector)) {
			createPrepareRequestReply(false);
		}

		if (ProposalVectorUtils.areProposalsEqual(prepareHighestProposalVector, new_proposal_vector)) {
			System.err.println("[getPrepareRequestReply]: Expected different proposal vectors");
			return createPrepareRequestReply(true);
		}
		if (ProposalVectorUtils.isProposalHigherThan(prepareHighestProposalVector, new_proposal_vector)) {
			return createPrepareRequestReply(false);
		}
		prepareHighestProposalVector = new_proposal_vector;
		return createPrepareRequestReply(true);
	}

	private PhaseOneReply createPrepareRequestReply(boolean was_promised) {
		if (mostRecentPaxosValue == null) {
	
			return DadkvsPaxos.PhaseOneReply.newBuilder()
					.setPhase1Config(-1) // Not Implemented
					.setHighestProposalVector(ProposalVectorUtils.getOrPlaceholder(null))
					.setPhase1Accepted(was_promised)
					.setPhase1Value(TransactionRecordUtils.getOrPlaceholder(null))
					.build();
		}
		return DadkvsPaxos.PhaseOneReply.newBuilder()
				.setPhase1Config(-1) // Not Implemented
				.setHighestProposalVector(ProposalVectorUtils.getOrPlaceholder(
						mostRecentPaxosValue.proposal_vector))
				.setPhase1Accepted(was_promised)
				.setPhase1Value(TransactionRecordUtils.getOrPlaceholder(mostRecentPaxosValue.tr))
				.build();
	}

	synchronized Entry<PhaseTwoReply, PaxosValue> getAcceptRequestReply(PhaseTwoRequest acceptRequest) {
		final ProposalVector new_proposal_vector = acceptRequest.getProposalVector();
		if (alreadyLearned(new_proposal_vector)) {
			return createAcceptReplyTuple(false);
		}

		if (ProposalVectorUtils.areProposalsEqual(promisedHighestProposalVector, new_proposal_vector)) {
			System.err.println("[getAcceptRequestReply]: Expected different proposal vectors");
			return createAcceptReplyTuple(true);
		}
		if (ProposalVectorUtils.isProposalHigherThan(prepareHighestProposalVector, new_proposal_vector)) {
			return createAcceptReplyTuple(false);
		}

		promisedHighestProposalVector = new_proposal_vector;
		mostRecentPaxosValue = new PaxosValue(acceptRequest.getPhase2Value(), new_proposal_vector);
		return createAcceptReplyTuple(true);
	}

	private Entry<PhaseTwoReply, PaxosValue> createAcceptReplyTuple(boolean was_accepted) {

		final PaxosValue currentMostRecentPaxosValue = mostRecentPaxosValue;
		return new AbstractMap.SimpleEntry<>(
				createAcceptRequestReply(was_accepted, currentMostRecentPaxosValue),
				currentMostRecentPaxosValue
		);
	}

	private static PhaseTwoReply createAcceptRequestReply(boolean was_accepted,
			PaxosValue paxosValue) {

		if (paxosValue == null) {
			return DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(-1) // Not Implemented
					.setAcceptedProposalVector(ProposalVectorUtils.getOrPlaceholder(null))
					.setPhase2Accepted(was_accepted)
					.build();
		}
		return DadkvsPaxos.PhaseTwoReply.newBuilder()
				.setPhase2Config(-1) // Not Implemented
				.setAcceptedProposalVector(ProposalVectorUtils.getOrPlaceholder(
						paxosValue.proposal_vector))
				.setPhase2Accepted(was_accepted)
				.build();
	}

	private boolean alreadyLearned(ProposalVector proposalVector) {
		return paxosValueCollector.contains(proposalVector.getPaxosIndex());
	}
}
