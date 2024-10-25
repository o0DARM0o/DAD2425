package dadkvs.server;

abstract class PaxosInstance {
	final PaxosValue paxosValue;

	PaxosInstance(PaxosValue paxosValue) {
		this.paxosValue = paxosValue;
	}
}
