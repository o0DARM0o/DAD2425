package dadkvs.server;

import dadkvs.util.DebugMode;
import io.grpc.Server;

public class DadkvsServerState {
	Server		   server;
    boolean        i_am_leader;
	DebugMode      old_debug_mode;
    DebugMode      new_debug_mode;
    int            base_port;
    int            my_id;
    int            store_size;
    KeyValueStore  store;
    MainLoop       main_loop;
    Thread         main_loop_worker;

    
    public DadkvsServerState(int kv_size, int port, int myself) {
	server = null;
	base_port = port;
	my_id = myself;
	i_am_leader = false;
	old_debug_mode = null;
	new_debug_mode = null;
	store_size = kv_size;
	store = new KeyValueStore(kv_size);
	main_loop = new MainLoop(this);
	main_loop_worker = new Thread (main_loop);
	main_loop_worker.start();
    }

	public void setServer(Server server) {
		this.server = server;
	}
}
