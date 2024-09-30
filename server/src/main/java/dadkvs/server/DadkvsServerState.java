package dadkvs.server;

import java.util.List;

import dadkvs.util.DebugMode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
    List<ManagedChannel> followerChannels;  // Channels to communicate with follower servers

    
    public DadkvsServerState(int kv_size, int port, int myself) {
	server = null;
    public DadkvsServerState(int kv_size, int port, int myself, boolean leader) {
	base_port = port;
	my_id = myself;
	i_am_leader = leader;
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
    

     // Initialize the gRPC channels for the followers if this server is the leader
     if (i_am_leader) {
        // Initialize the channels for communicating with followers (other servers)
        initializeFollowerChannels();
    }
    }

     /**
     * Initializes the communication channels to the follower servers.
     * 
     * This is required only if the current server is the leader.
     */
    private void initializeFollowerChannels() {
        followerChannels = List.of(
                ManagedChannelBuilder.forAddress("localhost", base_port + 1).usePlaintext().build(),
                ManagedChannelBuilder.forAddress("localhost", base_port + 2).usePlaintext().build(),
                ManagedChannelBuilder.forAddress("localhost", base_port + 3).usePlaintext().build(),
                ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build()
                
        );

        
    }

    /**
     * Cleanup function to properly shut down gRPC channels when the server is shutting down.
     */
    public void cleanup() {
        if (followerChannels != null) {
            for (ManagedChannel channel : followerChannels) {
                channel.shutdown();  // Shutdown each channel properly
            }
        }
    }

    
}
