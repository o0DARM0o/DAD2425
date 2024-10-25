package dadkvs.server;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.stub.StreamObserver;

public interface GenericPaxosFunctions<T, R> {
	void doPaxosPhase(T request, StreamObserver<R> responseObserver,
			DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub stub);

	boolean getResponseBoolean(R response);
}
