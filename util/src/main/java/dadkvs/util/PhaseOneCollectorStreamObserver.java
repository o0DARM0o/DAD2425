package dadkvs.util;

import dadkvs.DadkvsPaxos;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;

public class PhaseOneCollectorStreamObserver implements StreamObserver<DadkvsPaxos.PhaseOneReply> {
    private int majority;
    private int acceptCount = 1;
    private int rejectCount = 0;
    private dadkvs.util.GenericResponseCollector collector;
    private boolean done;

    public PhaseOneCollectorStreamObserver(int majority, GenericResponseCollector c ) {
        this.majority = majority;
        this.collector = c;
        this.done = false;
    }

    @Override
    public void onNext(DadkvsPaxos.PhaseOneReply response) {

            if (!done) {
                collector.addResponse(response);
                
                if (response.getPhase1Accepted()) {
                    //System.out.println("Follower accepted Phase 1 for index: " + response.getPhase1Index());
                    acceptCount++;
                } else {
                    rejectCount++;
                }

                // If we have reached the majority, mark as done
                if (acceptCount >= majority) {
                    done = true;
                }
            }
        
    }

    @Override
    public void onError(Throwable t) {
        System.err.println("Error during Phase 1 request: " + t.getMessage());
        if (!done) {
            collector.addNoResponse();
            done = true;
        }
       
    }

    @Override
    public void onCompleted() {
        System.out.println("Stream completed");
        if (!done) {
            collector.addNoResponse();
            done = true;
        }

    }

    public int getAcceptCount () {
        return this.acceptCount;
    }
}
