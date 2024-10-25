package dadkvs.server;

public class TimeOutManager {
	private static final int MAX_TIME_OUT_TRIES = 4;
	static final int DEFAULT_TIMEOUT_MILIS = 1000;
	private static final int MAX_TIMEOUT_MILIS = DEFAULT_TIMEOUT_MILIS << (MAX_TIME_OUT_TRIES - 1);
	private int timeout_milis;

	TimeOutManager() {
		resetTimeOut();
	}

	void resetTimeOut() {
		timeout_milis = DEFAULT_TIMEOUT_MILIS;
	}

	boolean increaseTimeOut() {
		if (timeout_milis >= MAX_TIMEOUT_MILIS) {
			return false;
		}
		timeout_milis <<= 1;
		return true;
	}

	int getTimeoutMilis() {
		return timeout_milis;
	}
}
