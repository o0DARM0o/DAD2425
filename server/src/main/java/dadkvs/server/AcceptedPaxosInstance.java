package dadkvs.server;

import java.util.List;

import dadkvs.DadkvsPaxos.PhaseTwoReply;


public class AcceptedPaxosInstance extends PaxosInstance {

	AcceptedPaxosInstance(PromisedPaxosInstance successfulPromisedPaxosInstance,
			List<PhaseTwoReply> phaseTwoReplies) {

		super(successfulPromisedPaxosInstance.paxosValue);

		areValidProposalVectors(successfulPromisedPaxosInstance.paxosValue, phaseTwoReplies);
	}

	private static boolean areValidProposalVectors(PaxosValue paxosValue,
			List<PhaseTwoReply> phaseTwoReplies) {

		for (PhaseTwoReply phaseTwoReply : phaseTwoReplies) {
			if (!ProposalVectorUtils.areProposalsEqual(
					paxosValue.proposal_vector,
					phaseTwoReply.getAcceptedProposalVector()
			)) {
				System.err.println("[areValidProposedVectors]: expected same ProposalVectors");
				return false;
			}
		}
		return true;
	}
}
