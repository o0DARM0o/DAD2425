package dadkvs.util;

import java.util.List;

public class GenericResponseCollector<T>  {
    List<T> collectedResponses;
    int   received;
    int   pending;

    public GenericResponseCollector(List<T> responses, int maxresponses) {
        collectedResponses = responses;
	received = 0;
	pending = maxresponses;
    }

    synchronized public void addResponse(T resp) {
        collectedResponses.add(resp);
	received++;
	pending--;
	notifyAll();
    }

    synchronized public void addNoResponse() {
	pending--;
	notifyAll();
    }
 
    synchronized public void waitForTarget(int target) {
        while ((pending > 0) && (received < target)) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
    }
}
