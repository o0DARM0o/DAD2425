package dadkvs.server;

import java.util.AbstractMap;
import java.util.Map.Entry;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxos.TransactionRecord;

public class TransactionRecordUtils {

	private TransactionRecordUtils() {
		// Can't create instances
	}

	static Entry<Integer, Integer> createIntegerTuple(int key, int value) {
		return new AbstractMap.SimpleEntry<>(key, value);
	}

	static TransactionRecord createTransactionRecord(DadkvsMain.CommitRequest request) {
			
			return DadkvsPaxos.TransactionRecord.newBuilder()
				.setRead1Key(request.getKey1())
				.setRead1Version(request.getVersion1())
				.setRead2Key(request.getKey2())
				.setRead2Version(request.getVersion2())
				.setWriteKey(request.getWritekey())
				.setWriteValue(request.getWriteval())
				.setReqId(request.getReqid())
				.build();

	}

	static boolean areTransactionsEqual(TransactionRecord tr1, TransactionRecord tr2) {
		if (tr1 == tr2) {
			return true;
		}
		if (tr1 == null || tr2 == null) {
			return false;
		}
		return tr1.getRead1Key() == tr2.getRead1Key() &&
				tr1.getRead1Version() == tr2.getRead1Version() &&
				tr1.getRead2Key() == tr2.getRead2Key() &&
				tr1.getRead2Version() == tr2.getRead2Version() &&
				tr1.getWriteKey() == tr2.getWriteKey() &&
				tr1.getWriteValue() == tr2.getWriteValue() &&
				tr1.getReqId() == tr2.getReqId();
	}
}
