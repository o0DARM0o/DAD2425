package dadkvs.util;

/**
 * Enum representing various debug modes for the application.
 */
public enum DebugMode {
	CRASH {
		@Override
        public  void executeDebugMode() {
			System.out.println("Server crashed!");
            // The rest is made in DadkvsConsoleServiceImpl.java
        }
	},
	FREEZE {
		@Override
        public  void executeDebugMode() {
			System.out.println("Server froze!");
            // The rest is made in DadkvsMainServiceImpl.java and DadkvsServerState.java
        }
	},
	UN_FREEZE,
	SLOW_MODE_ON,
	SLOW_MODE_OFF;

	public void executeDebugMode() {
		System.out.println("executeMode not implemented for " + "\"" + this + "\"");
	}

	/**
	 * Converts a string representation of a debug mode to the corresponding {@link DebugMode} enum.
	 * The method replaces hyphens ("-") with underscores ("_") and converts the string to uppercase
	 * to match the naming convention of the enum constants.
	 * <p>
	 * For example, the input "crash" or "CRASH" will return {@link DebugMode#CRASH}, 
	 * and "slow-mode-on" will return {@link DebugMode#SLOW_MODE_ON}.
	 * </p>
	 * 
	 * @param debugModeString The string representation of the debug mode.
	 *                        May contain hyphens ("-").
	 * @return The corresponding {@link DebugMode} enum value if the input string matches a constant; 
	 *         otherwise, returns {@code null}.
	 */
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
