package dadkvs.server;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GenericPaxosMessagesHandler<T, R> {
	private final int majority;

	private final AtomicInteger acceptationResponseCount = new AtomicInteger(0);
	private final AtomicInteger rejectionResponseCount = new AtomicInteger(0);

	private final List<R> acceptedResponsesValues =
			Collections.synchronizedList(new ArrayList<>());

	private final AtomicBoolean[] has_channel_responded;

	private final Object lock = new Object();

	private final GenericPaxosFunctions<T, R> paxosFunctions;
	private final int timeout_milis;
	private final boolean reject_timeout;

	public GenericPaxosMessagesHandler(GenericPaxosFunctions<T, R> paxosFunctions,
			int timeout_milis, boolean reject_timeout) {

		this.majority = getMajority();
		this.has_channel_responded = getHasChannelResponded(DadkvsServer.TOTAL_SERVERS);
		this.paxosFunctions = paxosFunctions;
		this.timeout_milis = timeout_milis;
		this.reject_timeout = reject_timeout;
	}

	private static int getMajority() {
		return DadkvsServer.TOTAL_SERVERS / 2 + 1;
	}

	private static AtomicBoolean[] getHasChannelResponded(int totalReplicas) {
		final AtomicBoolean[] new_has_channel_responded = new AtomicBoolean[totalReplicas];
		for (int i = 0; i < new_has_channel_responded.length; i++) {
			new_has_channel_responded[i] = new AtomicBoolean(false);
		}
		return new_has_channel_responded;
	}

	public  Map.Entry<MessageResultEnum, List<R>> createResultTuple(MessageResultEnum resultEnum) {
        return new AbstractMap.SimpleEntry<>(resultEnum, this.acceptedResponsesValues);
    }


	public Map.Entry<MessageResultEnum, List<R>> sendRequestsToServers(
			ManagedChannel[] serverChannels, T request) throws IllegalArgumentException {

		if (serverChannels.length != DadkvsServer.TOTAL_SERVERS) {
			throw new IllegalArgumentException(
					"Expected " +
					DadkvsServer.TOTAL_SERVERS +
					" server channels, but got " +
					serverChannels.length
			);
		}
		// Iterate over each server and send the request
		for (int i = 0; i < DadkvsServer.TOTAL_SERVERS; i++) {
			final ManagedChannel replicaChannel = serverChannels[i];
			final AtomicBoolean has_this_channel_responded = has_channel_responded[i];
			// Create a generic StreamObserver to handle the responses
			final StreamObserver<R> responseObserver =
					initiateStreamObserver(has_this_channel_responded);

			final DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub stub =
                    DadkvsPaxosServiceGrpc.newStub(replicaChannel);

            paxosFunctions.doPaxosPhase(request, responseObserver, stub);
		}

		try {
			final MessageResultEnum resultEnum = waitForMajorityOrTimeout();
			return createResultTuple(resultEnum);
		} catch (InterruptedException e) {
			System.err.println("[sendRequestsToReplicas]: Unexpected InterruptedException");
			return createResultTuple(MessageResultEnum.REJECTED);
		}
	}

	private StreamObserver<R> initiateStreamObserver(AtomicBoolean has_this_channel_responded) {

		return new StreamObserver<R>() {
			@Override
			public void onNext(R response) {
				if (!has_this_channel_responded.get()) {
					// Mark the channel as responded
					has_this_channel_responded.set(true);
					// Extract boolean value from the response
					final boolean responseBool = paxosFunctions.getResponseBoolean(response);
					// Update counters based on the response
					if (responseBool) {
						acceptationResponseCount.incrementAndGet();
						acceptedResponsesValues.add(response);
					} else {
						rejectionResponseCount.incrementAndGet();
					}
					// Check for majority
					checkMajority();
				}
			}

			@Override
			public void onError(Throwable t) {
				System.err.println("[initiateStreamObserver]: Error from some replica channel: " +
						t.getMessage());
			}

			@Override
			public void onCompleted() {
				// Do nothing
			}
		};
	}

	private void checkMajority() {
		if (hasMajority()) {
			lock.notify();
		}
	}

	private boolean hasMajority() {
		return hasAcceptanceMajority() || hasRejectionMajority();
	}

	private boolean hasAcceptanceMajority() {
		return acceptationResponseCount.get() >= majority;
	}

	private boolean hasRejectionMajority() {
		return rejectionResponseCount.get() >= majority;
	}

	private MessageResultEnum waitForMajorityOrTimeout()
			throws InterruptedException {

		synchronized (lock) {
			final long timeout =
					System.currentTimeMillis() + TimeUnit.MILLISECONDS.toMillis(timeout_milis);

			while (!hasMajority() && System.currentTimeMillis() < timeout) {
				final long remainingTime = timeout - System.currentTimeMillis();
				if (remainingTime > 0) {
					lock.wait(remainingTime); // Wait for majority or timeout
				}
			}
			
			if (hasAcceptanceMajority()) {
				return MessageResultEnum.ACCEPTED;
			} else if (hasRejectionMajority()) {
				return MessageResultEnum.TIMED_OUT;
			} else if (reject_timeout) {
				return MessageResultEnum.REJECTED;
			} else {
				return MessageResultEnum.TIMED_OUT;
			}
		}
	}
}
