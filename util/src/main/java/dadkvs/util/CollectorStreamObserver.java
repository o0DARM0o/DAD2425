package dadkvs.util;

import io.grpc.stub.StreamObserver;

public class CollectorStreamObserver<T> implements StreamObserver<T> {

    dadkvs.util.GenericResponseCollector collector;
    boolean done;

    public CollectorStreamObserver (GenericResponseCollector c) {
        collector = c;
	done = false;
    }

    @Override
    public void onNext(T value) {
        // Handle the received response of type T
	if (done == false) {
	    collector.addResponse(value);
	    done = true;
	}
    }

    @Override
    public void onError(Throwable t) {
        // Handle error
		if (done == false) {
			collector.addNoResponse();
			done = true;
		}
    }

    @Override
    public void onCompleted() {
        // Handle stream completion
	if (done == false) {
	    collector.addNoResponse();
	    done = true;
	}
    }
}
