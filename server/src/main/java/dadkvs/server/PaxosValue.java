package dadkvs.server;

import java.util.Objects;

import dadkvs.DadkvsPaxos.ProposalVector;
import dadkvs.DadkvsPaxos.TransactionRecord;

public class PaxosValue {
	final TransactionRecord tr;
	final ProposalVector proposal_vector;

	public PaxosValue(TransactionRecord tr, ProposalVector proposal_vector) {
		this.tr = tr;
		this.proposal_vector = proposal_vector;
	}

	public PaxosValue incrementProposalNumber() {

		return new PaxosValue(
				this.tr,
				ProposalVectorUtils.incrementProposalNumber(proposal_vector)
		);
	}

	public PaxosValue rejectPaxosValue() {
		return new PaxosValue(null, this.proposal_vector);
	}

	public static PaxosValue getMoreRecentValue(PaxosValue value1, PaxosValue value2) {
		// Check trivial cases
		if (value1 == null) {
			return value2;
		}
		if (value2 == null) {
			return value1;
		}
		if (ProposalVectorUtils
				.areProposalsEqual(value1.proposal_vector, value2.proposal_vector)) {

			if (!value1.equals(value2)) {
				System.err.println("[getMoreRecentValue]: PaxosValues expected to be equal");
				return null;
			}
			return value1;
		}
		// Check when proposal number are different
		if (ProposalVectorUtils
				.isProposalHigherThan(value1.proposal_vector, value2.proposal_vector)) {

			return value1;
		}
		return value2;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		PaxosValue other = (PaxosValue) obj;

		return ProposalVectorUtils.areProposalsEqual(proposal_vector, other.proposal_vector) &&
				TransactionRecordUtils.areTransactionsEqual(tr, other.tr);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tr, proposal_vector);
	}
}
