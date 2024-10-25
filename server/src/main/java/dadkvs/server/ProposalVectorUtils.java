package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.ProposalVector;

public class ProposalVectorUtils {

	private ProposalVectorUtils() {
		// Can't create instances
	}

	static ProposalVector createProposalVector(int proposer_id, int paxos_index,
			int proposal_number) {

		return DadkvsPaxos.ProposalVector.newBuilder()
				.setProposerId(proposer_id)
				.setPaxosIndex(paxos_index)
				.setProposalNumber(proposal_number)
				.build();
	}

	static ProposalVector updateProposalNumber(ProposalVector proposalVector,
			int new_proposal_number) {

		return createProposalVector(
				proposalVector.getProposerId(),
				proposalVector.getPaxosIndex(),
				new_proposal_number
		);
	}

	static ProposalVector incrementProposalNumber(ProposalVector proposalVector) {

		return createProposalVector(
				proposalVector.getProposerId(),
				proposalVector.getPaxosIndex(),
				proposalVector.getProposalNumber() + 1
		);
	}

	static boolean areProposalsEqual(ProposalVector proposal1, ProposalVector proposal2) {
		// Check trivial cases
		if (proposal1 == null && proposal2 == null) {
			return true;
		}
		if (proposal1 == null || proposal2 == null) {
			return false;
		}
	
		// Compare proposerId, paxosIndex, and proposalNumber
		return proposal1.getProposerId() == proposal2.getProposerId() &&
			   proposal1.getPaxosIndex() == proposal2.getPaxosIndex() &&
			   proposal1.getProposalNumber() == proposal2.getProposalNumber();
	}

	static boolean isProposalHigherThan(ProposalVector proposal1,
			ProposalVector proposal2) {

		// Check trivial cases
		if (proposal1 == null) {
			return false;
		}
		if (proposal2 == null) {
			return true;
		}

		// Compare based on proposerId, paxosIndex, and proposalNumber in that order
		if (proposal1.getProposerId() > proposal2.getProposerId()) {
			return true;
		} else if (proposal1.getProposerId() < proposal2.getProposerId()) {
			return false;
		}
	
		if (proposal1.getPaxosIndex() > proposal2.getPaxosIndex()) {
			return true;
		} else if (proposal1.getPaxosIndex() < proposal2.getPaxosIndex()) {
			return false;
		}
	
		if (proposal1.getProposalNumber() > proposal2.getProposalNumber()) {
			return true;
		}
	
		return false;
	}

	static ProposalVector getHighestProposal(ProposalVector proposal1, ProposalVector proposal2) {
		// Check trivial cases
		if (proposal1 == null) {
			return proposal2;
		}
		if (proposal2 == null) {
			return proposal1;
		}
	
		// Compare based on proposerId, paxosIndex, and proposalNumber in that order
		if (proposal1.getProposerId() > proposal2.getProposerId()) {
			return proposal1;
		} else if (proposal1.getProposerId() < proposal2.getProposerId()) {
			return proposal2;
		}
	
		if (proposal1.getPaxosIndex() > proposal2.getPaxosIndex()) {
			return proposal1;
		} else if (proposal1.getPaxosIndex() < proposal2.getPaxosIndex()) {
			return proposal2;
		}
	
		if (proposal1.getProposalNumber() > proposal2.getProposalNumber()) {
			return proposal1;
		}
	
		return proposal2;
	}
}
