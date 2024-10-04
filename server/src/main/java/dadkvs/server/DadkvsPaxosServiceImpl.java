
package dadkvs.server;


import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.DebugMode;
import dadkvs.util.PaxosInstance;
import dadkvs.util.PaxosManager;
import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;
    PaxosManager paxosManager;
    
	private final Object freezeLock = new Object(); // Lock object for freeze/unfreeze mechanism
    
    public DadkvsPaxosServiceImpl(DadkvsServerState state, PaxosManager paxosManager) {
        this.server_state = state;
		this.server_state.paxosServiceImpl = this;
        this.paxosManager = paxosManager;
	
    }
    
	private void checkFreeze() {
		synchronized (freezeLock) {
			while (server_state.new_debug_mode == DebugMode.FREEZE) {
				try {
					System.out.println("Server is in FREEZE mode. Pausing request handling...");
					freezeLock.wait(); // Wait until UN_FREEZE is called
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt(); // Reset thread interrupt status
				}
			}
		}
    }

	private void unfreeze() {
		synchronized (freezeLock) {
			System.out.println("Server is in UN_FREEZE mode. Resuming request handling...");
			freezeLock.notifyAll(); // Notify all waiting threads
		}
	}

    @Override
    public void phaseone(DadkvsPaxos.PhaseOneRequest request, StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
		checkFreeze(); // Check if server is frozen before processing the request
		// for debug purposes
        System.out.println("Receive phase1 request: " + request);

        int proposedIndex = request.getPhase1Index();  // Índice da proposta recebida
        int proposedTimestamp = request.getPhase1Timestamp();  // Timestamp da proposta
        int promisedIndex = server_state.promisedIndex;  // Último índice prometido
        int currentTimestamp = -1;

        // Verifica o timestamp da proposta anterior aceita (se existir)
        PaxosInstance instance = paxosManager.getPaxosInstance(proposedIndex - 1);
        if (instance != null) {
            currentTimestamp = instance.getAcceptedTimestamp();
        }

        DadkvsPaxos.PhaseOneReply.Builder response = DadkvsPaxos.PhaseOneReply.newBuilder()
            .setPhase1Config(request.getPhase1Config())
            .setPhase1Index(proposedIndex)
            .setPhase1Accepted(false);  // Inicialmente, a proposta não é aceita

        // Verifica se a proposta pode ser aceita
        if (proposedIndex > promisedIndex) {
            if (proposedTimestamp >= currentTimestamp) {
                server_state.promisedIndex = proposedIndex;
                System.out.println("PHASE 1 PROMISED INDEX: " + server_state.promisedIndex);

                // Aceita a proposta e envia o valor e timestamp aceitos
                if (instance != null) {
                    response.setPhase1Accepted(true)
                            .setPhase1Value(instance.getProposalValue())  // Valor da última proposta
                            .setPhase1Timestamp(proposedTimestamp);  // Timestamp associado
                } else {
                    // Caso não haja proposta anterior
                    response.setPhase1Accepted(true)
                            .setPhase1Timestamp(proposedTimestamp);  // Usar timestamp da proposta atual
                }
            }
        }

        // Envia a resposta ao líder
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();

    }

    @Override
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request, StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
        checkFreeze(); // Check if server is frozen before processing the request
		// for debug purposes
        System.out.println ("Receive phase two request: " + request);
        int proposedIndex = request.getPhase2Index();   // Número da proposta
        int value = request.getPhase2Value();           // Valor proposto
        int timestamp = request.getPhase2Timestamp();   // Timestamp do valor proposto
        int promisedIndex = server_state.promisedIndex; // Último índice prometido

        System.out.println("PHASE 2 PROMISED INDEX: " + server_state.promisedIndex);
        
        PaxosInstance currentInstance = paxosManager.getPaxosInstance(proposedIndex);

        PaxosInstance previousInstance = paxosManager.getPaxosInstance(proposedIndex-1);
        int currentAcceptedTimestamp = previousInstance != null ? previousInstance.getAcceptedTimestamp() : -1;

        DadkvsPaxos.PhaseTwoReply.Builder response = DadkvsPaxos.PhaseTwoReply.newBuilder()
            .setPhase2Config(request.getPhase2Config())
            .setPhase2Index(proposedIndex)
            .setPhase2Accepted(false);  // Por padrão, a resposta é de não aceitação

        // Verifica se o índice proposto é maior ou igual ao índice prometido
        if (proposedIndex >= promisedIndex) {
            // Verifica se o timestamp é mais recente que o último aceito
            if (timestamp >= currentAcceptedTimestamp) {
                // Aceita a proposta e atualiza o valor e timestamp
                currentInstance.setAcceptedValue(value);
                currentInstance.setAcceptedTimestamp(timestamp);

                // Marca como aceita
                response.setPhase2Accepted(true);
            }
        }

        // Envia a resposta ao líder
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();

    }

    @Override
    public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
		checkFreeze(); // Check if server is frozen before processing the request
		// for debug purposes
	    System.out.println("Receive learn request: " + request);
        int learnedIndex = request.getLearnindex();    // Índice do valor aprendido
        int value = request.getLearnvalue();           // Valor aprendido
        int timestamp = request.getLearntimestamp();   // Timestamp associado
        
        PaxosInstance currentInstance = paxosManager.getPaxosInstance(learnedIndex);

        PaxosInstance previousInstance = paxosManager.getPaxosInstance(learnedIndex-1);
        int previousAcceptedTimestamp = previousInstance != null ? previousInstance.getAcceptedTimestamp() : -1;

        // Apenas aprende o valor se o timestamp for mais recente ou igual
        if (timestamp >= previousAcceptedTimestamp) {
            currentInstance.setAcceptedValue(value);
            currentInstance.setAcceptedTimestamp(timestamp);
        }
    
        DadkvsPaxos.LearnReply response = DadkvsPaxos.LearnReply.newBuilder()
            .setLearnconfig(request.getLearnconfig())
            .setLearnindex(learnedIndex)
            .setLearnaccepted(true)  // Assume que o aprendizado foi bem-sucedido
            .build();
    
        // Envia a resposta
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    // Handle heartbeat request from leader
    @Override
    public void heartbeat(DadkvsPaxos.HeartbeatRequest request, StreamObserver<DadkvsPaxos.HeartbeatReply> responseObserver) {
        checkFreeze(); // Check if server is frozen before processing the request

		int leaderId = request.getLeaderId();
        System.out.println("Received heartbeat from leader: " + leaderId);
        this.server_state.handleHeartbeat(leaderId);  // Update the server state with the received heartbeat

        DadkvsPaxos.HeartbeatReply reply = DadkvsPaxos.HeartbeatReply.newBuilder().setAck(true).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

	public void executeDebugMode(DebugMode debugMode) {
		if (debugMode == DebugMode.UN_FREEZE) {
			unfreeze(); // Call unfreeze when the mode is set to UN_FREEZE
		}
	}
}
