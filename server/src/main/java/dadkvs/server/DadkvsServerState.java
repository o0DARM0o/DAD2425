package dadkvs.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import java.util.Iterator;

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
    private final Object electionLock = new Object(); // Bloqueio para sincronização da eleição
    boolean isInElection = false;
	DadkvsMainServiceImpl mainServiceImpl = null;
	DadkvsPaxosServiceImpl paxosServiceImpl = null;
	private final Object freezeLock = new Object(); // Lock object for freeze/unfreeze mechanism
	
	private int i = 0;

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


	// Initialize the gRPC channels for the followers if this server is the leader
	if (i_am_leader) {
	// Initialize the channels for communicating with followers (other servers)
		initializeFollowerChannels();
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

            //followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 1).usePlaintext().build());
        } else if (this.my_id == 2) {
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 3).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
            // followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 1).usePlaintext().build());
            // followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 2).usePlaintext().build());
        } else if (this.my_id == 3) {
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port + 4).usePlaintext().build());
            // followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 1).usePlaintext().build());
            // followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 2).usePlaintext().build());
            // followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 3).usePlaintext().build());
        } else if (this.my_id == 4) {
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 1).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 2).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 3).usePlaintext().build());
            followerChannels.add(ManagedChannelBuilder.forAddress("localhost", base_port - 4).usePlaintext().build());
        }
    } catch (Exception e) {
        System.err.println("Error initializing follower channels: " + e.getMessage());
        // Handle the exception, e.g., set followerChannels to an empty list
       // followerChannels = new ArrayList<>(); // Set to an empty list or handle as needed
    }
}


     // Start sending heartbeat messages periodically
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
    
    // Method to check if a channel is active
    private boolean isChannelActive(ManagedChannel channel) {
        try {
            // Lightweight operation if applicable
            DadkvsPaxosServiceGrpc.DadkvsPaxosServiceBlockingStub stub = DadkvsPaxosServiceGrpc.newBlockingStub(channel);
            DadkvsPaxos.HeartbeatRequest request = DadkvsPaxos.HeartbeatRequest.newBuilder().setLeaderId(my_id).build();
            stub.heartbeat(request); // This could just be a ping call or similar
            return true; // If no exception is thrown, the channel is active
        } catch (Exception e) {
            System.err.println("Channel check failed: " + e.getMessage());
            return false; // Channel is not active
        }
    }

    // Check if the server is active
private boolean isServerActive(int followerId) {
    // Logic to check if the server is up, e.g., by attempting a lightweight RPC call
    int checkPort = base_port + followerId;
    try {
        // You can attempt a lightweight connection check
        ManagedChannel checkChannel = ManagedChannelBuilder.forAddress("localhost", checkPort)
                .usePlaintext()
                .build();
        
        // Create a stub and perform a no-op request
        DadkvsPaxosServiceGrpc.DadkvsPaxosServiceBlockingStub stub = DadkvsPaxosServiceGrpc.newBlockingStub(checkChannel);
        // Use a lightweight request like a ping, if applicable
        DadkvsPaxos.HeartbeatRequest request = DadkvsPaxos.HeartbeatRequest.newBuilder().setLeaderId(my_id).build();
        stub.heartbeat(request); // This could just be a ping call or similar

        // If no exception occurs, the server is active
        return true;
    } catch (Exception e) {
        System.err.println("Server " + followerId + " is not active: " + e.getMessage());
        return false; // Server is not active
    }
}
    
    // Reinitialize a follower channel
    private void reinitializeFollowerChannel(ManagedChannel channel) {
        int followerId = followerChannels.indexOf(channel) + 1; // Get the ID from the index
        int newPort = base_port + followerId; // Calculate the new port based on your logic
    
        // Check if the server is active before creating a new channel
        if (isServerActive(followerId)) {
            // Shutdown the old channel if it's being reinitialized
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Channel did not terminate in the specified time.");
                }
            } catch (InterruptedException e) {
                System.err.println("Interrupted while shutting down channel: " + e.getMessage());
                Thread.currentThread().interrupt(); // Restore interrupted status
            }
    
            // Create a new channel for the follower
            ManagedChannel newChannel = ManagedChannelBuilder.forAddress("localhost", newPort)
                    .usePlaintext()
                    .build();
    
            // Replace the old channel with the new channel
            int index = followerChannels.indexOf(channel);
            if (index != -1) {
                followerChannels.set(index, newChannel);
            }
        } else {
            System.err.println("Server " + followerId + " is not active, skipping channel reinitialization.");
            followerChannels.remove(channel); // Remove the channel if the corresponding server is inactive
        }
    }
    
    // Follower method to handle heartbeat from leader
    public void handleHeartbeat(int leaderId) {
        synchronized (electionLock) {
            System.out.println("HEARTBEAT: " + leaderId + " SERVER: " + current_leader_id);
            if (current_leader_id != leaderId) {
                System.out.println("New leader detected: " + leaderId);
                current_leader_id = leaderId;
                monitorLeader();
            } else if(my_id == leaderId && !this.i_am_leader) {
                System.out.println("I'm the new leader");
                this.i_am_leader = true;
                reinitializeFollowerChannels();  // Reinitialize channels for the new leader
                startHeartbeat();  // Start sending heartbeats as the new leader
            }
            last_heartbeat = System.currentTimeMillis();
            leader_alive = true;
        }
    }

    // Monitor leader's heartbeat and trigger election if no heartbeat is received
    public void monitorLeader() {

            new Thread(() -> {
                while (!i_am_leader) {
                    System.out.println("AQUII 1111");
                    long now = System.currentTimeMillis();
                    if (now - last_heartbeat > 5000) {
                        System.out.println("Leader timeout detected, starting re-election.");
                        leader_alive = false;
                        last_heartbeat = System.currentTimeMillis();
                        System.out.println("AQUI 22222");
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
    // Placeholder method for starting leader election
    private void startLeaderElection() {

        if(isInElection) {
            return;
        }
        
        isInElection = true; // Marque que estamos em uma eleição
    
        // Lógica para eleger um novo líder
        int current_leader = (current_leader_id + 1) % 5; 
        //System.out.println("New leader elected: " + current_leader);
        
        // Se este servidor é agora o líder
        if (current_leader == my_id) {
            System.out.println("I'm the new leader");
            i_am_leader = true;
            reinitializeFollowerChannels();  // Reinitialize channels for the new leader
            startHeartbeat();  // Start sending heartbeats as the new leader
        } else {
            i_am_leader = false; // Este servidor não é mais o líder
        }
    
        // Notifica todos os servidores que a eleição terminou
        isInElection = false; // Libere a flag após a eleição
       /*  try {
            notifyAll(); // Notifica todos os threads que estavam esperando
        } catch (IllegalMonitorStateException e ){
            System.out.println("Can't access Thread: " + e.getMessage());

        }*/
    }
    
    /**
     * Reinitialize follower channels when this server becomes the leader.
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
