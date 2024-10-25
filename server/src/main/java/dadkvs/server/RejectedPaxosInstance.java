package dadkvs.server;

public class RejectedPaxosInstance extends PaxosInstance {

	RejectedPaxosInstance(PaxosInstance rejectedPaxosInstance) {
		super(rejectedPaxosInstance.paxosValue.rejectPaxosValue());
	}
}
