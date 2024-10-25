package dadkvs.server;

import java.util.AbstractMap;
import java.util.Map.Entry;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.PhaseOneReply;
import dadkvs.DadkvsPaxos.PhaseOneRequest;
import dadkvs.DadkvsPaxos.PhaseTwoReply;
import dadkvs.DadkvsPaxos.PhaseTwoRequest;
import dadkvs.DadkvsPaxos.ProposalVector;
import dadkvs.DadkvsPaxos.TransactionRecord;

public class ReplicaPaxosManager {

	private ProposalVector highestProposalVector = null;
	private PaxosValue mostRecentPaxosValue = null;

	ReplicaPaxosManager() {

	}

	synchronized PhaseOneReply getPrepareRequestReply(PhaseOneRequest prepareRequest) {
		final ProposalVector new_proposal_vector = prepareRequest.getProposalVector();
		if (ProposalVectorUtils.areProposalsEqual(highestProposalVector, new_proposal_vector)) {
			System.err.println("[getPrepareRequestReply]: Expected different proposal vectors");
			return createPrepareRequestReply(true);
		}
		if (ProposalVectorUtils.isProposalHigherThan(highestProposalVector, new_proposal_vector)) {
			return createPrepareRequestReply(false);
		}
		highestProposalVector = new_proposal_vector;
		return createPrepareRequestReply(true);
	}

	private PhaseOneReply createPrepareRequestReply(boolean was_promised) {
		if (mostRecentPaxosValue == null) {
			return DadkvsPaxos.PhaseOneReply.newBuilder()
					.setPhase1Config(-1) // Not Implemented
					.setHighestProposalVector((ProposalVector)null)
					.setPhase1Accepted(was_promised)
					.setPhase1Value((TransactionRecord)null)
					.build();
		}
		return DadkvsPaxos.PhaseOneReply.newBuilder()
				.setPhase1Config(-1) // Not Implemented
				.setHighestProposalVector(mostRecentPaxosValue.proposal_vector)
				.setPhase1Accepted(was_promised)
				.setPhase1Value(mostRecentPaxosValue.tr)
				.build();
	}

	synchronized Entry<PhaseTwoReply, PaxosValue> getAcceptRequestReply(PhaseTwoRequest acceptRequest) {
		final ProposalVector new_proposal_vector = acceptRequest.getProposalVector();
		if (ProposalVectorUtils.areProposalsEqual(highestProposalVector, new_proposal_vector)) {
			System.err.println("[getAcceptRequestReply]: Expected different proposal vectors");
			return createAcceptReplyTuple(true);
		}
		if (ProposalVectorUtils.isProposalHigherThan(highestProposalVector, new_proposal_vector)) {
			return createAcceptReplyTuple(false);
		}
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
					.setAcceptedProposalVector((ProposalVector)null)
					.setPhase2Accepted(was_accepted)
					.build();
		}
		return DadkvsPaxos.PhaseTwoReply.newBuilder()
				.setPhase2Config(-1) // Not Implemented
				.setAcceptedProposalVector(paxosValue.proposal_vector)
				.setPhase2Accepted(was_accepted)
				.build();
	}
}
