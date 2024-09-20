package dadkvs.util;

public enum DebugMode {
	CRASH,
	FREEZE,
	UN_FREEZE,
	SLOW_MODE_ON,
	SLOW_MODE_OFF;

	public static DebugMode getDebugMode(String debugModeString) {
		DebugMode debugMode = null;
		String CuratedDebugModeString = debugModeString.replace("-", "_");
		try {
			debugMode = DebugMode.valueOf(CuratedDebugModeString.toUpperCase());
		} catch (IllegalArgumentException | NullPointerException e) {
			System.out.println("[class DebugMode]: Unknown debugMode: " + debugModeString);
		}
		return debugMode;
	}
}
