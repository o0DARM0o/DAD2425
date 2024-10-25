package dadkvs.server;
public class MainLoop implements Runnable  {
	DadkvsServerState server_state;

	private boolean has_work;
	
	public MainLoop(DadkvsServerState state) {
		this.server_state = state;
		this.has_work = false;
	}

	public void run() {
		while (true)
			this.doWork();
	}
	
	
	synchronized public void doWork() {
		System.out.println("Main loop do work start");
		if (server_state.new_debug_mode != server_state.old_debug_mode) {
			server_state.new_debug_mode.executeDebugMode();
			server_state.executeDebugMode(server_state.new_debug_mode);
			if (server_state.mainServiceImpl != null) {
				server_state.mainServiceImpl.executeDebugMode(server_state.new_debug_mode);
			}
			if (server_state.paxosServiceImpl != null) {
				server_state.paxosServiceImpl.executeDebugMode(server_state.new_debug_mode);
			} 
			server_state.old_debug_mode = server_state.new_debug_mode;
		}
		this.has_work = false;
		while (this.has_work == false) {
			System.out.println("Main loop do work: waiting");
			try {
				wait();
			}
			catch (InterruptedException e) {
				System.out.println("[doWork]: InterruptedException not expected");
			}
		}
		System.out.println("Main loop do work finish");
	}
	
	synchronized public void wakeup() {
		this.has_work = true;
		notify();    
	}
}
