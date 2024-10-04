package dadkvs.server;

import java.util.ArrayList;
import java.util.List;

import com.google.api.SystemParameter;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.HeartbeatReply;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.DebugMode;
import dadkvs.util.GenericResponseCollector;
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
    int promisedIndex;
    int config;
    int current_leader_id;
    boolean leader_alive;  // Track whether the leader is alive
    long last_heartbeat;  // Track the last heartbeat from the leader
    private final Object electionLock = new Object(); 
    boolean isInElection = false;

	DadkvsMainServiceImpl mainServiceImpl = null;
	DadkvsPaxosServiceImpl paxosServiceImpl = null;
	private final Object freezeLock = new Object(); // Lock object for freeze/unfreeze mechanism

    int n_servers;

    public int acceptedProposalNumber; // Highest accepted proposal number
    public int acceptedValue = -1; 
    
    /**
     * Constructor for the server state. Initializes key parameters, store,
     * channels, and starts the main loop.
     * 
     * @param kv_size Size of the key-value store
     * @param port    Base port for communication
     * @param myself  Server's ID
     * @param leader  Indicates whether this server starts as the leader
     */
    public DadkvsServerState(int kv_size, int port, int myself, boolean leader) {
	server = null;
	base_port = port;
	my_id = myself;
	i_am_leader = leader;
    current_leader_id = (leader ? myself : -1);
    leader_alive = true;
    last_heartbeat = System.currentTimeMillis();
	old_debug_mode = null;
	new_debug_mode = null;
	store_size = kv_size;
	store = new KeyValueStore(kv_size);
	main_loop = new MainLoop(this);
	main_loop_worker = new Thread (main_loop);
	main_loop_worker.start();
    promisedIndex = -1;
    config = 0;

    n_servers = 5;

	// Initialize the gRPC channels for the followers if this server is the leader
	if (i_am_leader) {
	// Initialize the channels for communicating with followers (other servers)
		initializeFollowerChannels();
		try {
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			System.out.println("[new DadkvsServerState]: something went wrong");
		}
        startHeartbeat();
	} else {
        monitorLeader();
    }
	}

	private void checkFreeze() {
		synchronized (freezeLock) {
			while (new_debug_mode == DebugMode.FREEZE) {
				try {
					System.out.println("Server_State is in FREEZE mode. Pausing heartbeat...");
					freezeLock.wait(); // Wait until UN_FREEZE is called
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt(); // Reset thread interrupt status
				}
			}
		}
    }

	private void unfreeze() {
		synchronized (freezeLock) {
			System.out.println("Server State is in UN_FREEZE mode. Resuming heartbeat...");
			freezeLock.notifyAll(); // Notify all waiting threads
		}
	}



	public void setServer(Server server) {
		this.server = server;
	}

     /**
     * Initializes the communication channels to the follower servers.
     * 
     * This is required only if the current server is the leader.
     */
    private void initializeFollowerChannels() {
        followerChannels = new ArrayList<>(); // Create a mutable list
        
    try {
        if (this.my_id == 0) {
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 1).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 2).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 3).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
        } else if (this.my_id == 1) {
            //followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 1).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 2).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 3).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port).usePlaintext().build());
        } else if (this.my_id == 2) {
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 3).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 1).usePlaintext().build());


        } else if (this.my_id == 3) {
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 2).usePlaintext().build());            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 1).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port).usePlaintext().build());


            n_servers = 0;
        } else if (this.my_id == 4) {
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 3).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 2).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 1).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port).usePlaintext().build());




        }
        n_servers = followerChannels.size() + 1;
        if (this.my_id == 4){
            n_servers = 1;
        }
    } catch (Exception e) {
        System.err.println("Error initializing follower channels: " + e.getMessage());
    }
}


     /**
     * Starts sending heartbeat messages periodically to all followers. Only the
     * leader sends heartbeats.
     */
     private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (i_am_leader) {
                try {
					Thread.sleep(2000);  // Send heartbeat every 2 seconds
					checkFreeze();
                    sendHeartbeatToFollowers();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        heartbeatThread.start();
    }

	// Send heartbeat to all followers asynchronously using the new response collector
    private void sendHeartbeatToFollowers() {
        ArrayList<HeartbeatReply> responses = new ArrayList<>();
        GenericResponseCollector<HeartbeatReply> collector = new GenericResponseCollector<>(responses, followerChannels.size());
        // Send heartbeat requests to all follower servers
        for (ManagedChannel channel : followerChannels) {
            try {
                // Create an asynchronous stub
                DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub asyncStub = DadkvsPaxosServiceGrpc.newStub(channel);
                // Create a heartbeat request
                DadkvsPaxos.HeartbeatRequest request = DadkvsPaxos.HeartbeatRequest.newBuilder().setLeaderId(my_id).build();
                // Create a StreamObserver to collect the response
                CollectorStreamObserver<HeartbeatReply> streamObserver = new CollectorStreamObserver<>(collector);
                // Make the asynchronous gRPC call
                asyncStub.heartbeat(request, streamObserver);
            } catch (Exception e) {
                System.err.println("Failed to send heartbeat: " + e.getMessage());
            }
        }
		// Wait for all responses or a target number of responses
		collector.waitForTarget(followerChannels.size() / 2);
    }
      
     /**
     * Handles a received heartbeat from the leader and resets the heartbeat timer.
     * 
     * @param leaderId The ID of the leader sending the heartbeat
     */
    public void handleHeartbeat(int leaderId) {
        synchronized (electionLock) {
            System.out.println("HEARTBEAT: " + leaderId + " SERVER: " + current_leader_id);
            if (current_leader_id < leaderId) {
                System.out.println("New leader detected: " + leaderId);
                current_leader_id = leaderId;
                this.i_am_leader = false;
                monitorLeader();
            } else if(my_id == leaderId && !this.i_am_leader) {
                System.out.println("I'm the new leader");
                this.i_am_leader = true;
                current_leader_id = my_id;
                reinitializeFollowerChannels();  // Reinitialize channels for the new leader
                startHeartbeat();  // Start sending heartbeats as the new leader
            } else if(leaderId > current_leader_id && this.i_am_leader) {
                current_leader_id = leaderId;
                this.i_am_leader = false;
            }
            last_heartbeat = System.currentTimeMillis();
            leader_alive = true;
        }
    }

    /**
     * Starts monitoring the current leader by checking for heartbeat timeouts.
     * If no heartbeat is received within 5 seconds, an election is triggered.
     */
    public void monitorLeader() {

            new Thread(() -> {
                while (!i_am_leader) {
                    long now = System.currentTimeMillis();
                    if (now - last_heartbeat > 5000) {
                        System.out.println("Leader timeout detected, starting re-election.");
                        leader_alive = false;
                        last_heartbeat = System.currentTimeMillis();
                        startLeaderElection();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
            }
            }).start();
    }
       
    /**
     * Starts a leader election process if no leader heartbeat is received within
     * the expected timeout.
     */
    private void startLeaderElection() {

        if(isInElection) {
            return;
        }
        
        isInElection = true;
    
        int current_leader = (current_leader_id + 1) % 5; 
        
        if (current_leader == my_id) {
            System.out.println("I'm the new leader");
            i_am_leader = true;
            current_leader_id = current_leader;
            reinitializeFollowerChannels();  // Reinitialize channels for the new leader
            startHeartbeat();  // Start sending heartbeats as the new leader
        } else {
			current_leader_id = (current_leader_id + 1) % 5;
            i_am_leader = false;
        }
    
        isInElection = false; 
    }
    
    /**
     * Reinitializes the communication channels with follower servers.
     * This is typically done after an election when a new leader is chosen.
     */
    public void reinitializeFollowerChannels() {
        cleanup();
        followerChannels = new ArrayList<>();
        initializeFollowerChannels();
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

	public void executeDebugMode(DebugMode debugMode) {
		if (debugMode == DebugMode.UN_FREEZE) {
			unfreeze(); // Call unfreeze when the mode is set to UN_FREEZE
		}
	}
}
